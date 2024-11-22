import requests
import random
import ndjson
import json
from datetime import datetime, timezone
import os
import time
import gzip
import sys
import logging
import concurrent.futures
import urllib3
import argparse
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from functools import lru_cache 
import uuid

# configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
format_output = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
stdout_handler.setFormatter(format_output)
logger.addHandler(stdout_handler)

# not required, but helpful to distinguish your custom schema from ECS or OTel semantics
CUSTOM_FIELDS_NAMESPACE = "com.example"
# in ES, you can segregate services into namespaces
DATASTREAM_NAMESPACE = 'default'

# the number of records to batch at a time
BATCH_SIZE = 2000
# the number of simultaneous threads pushing batches
THREADS = 2
# timeout waiting for ES to respond
TIMEOUT_S = 5
# typically we gzip bulks
ENABLE_GZIP = True
# target bitrate to ES cluster in megabits/second (throttle to size ingest rate to cluster size)
TARGET_MBPS = 1
# report status every 1 s
REPORT_S = 1
# retry 429s 3 times, exponentially starting with 10ms
RETRIES = 3
RETRY_BACKOFF_S = 10 / 1000

TARGET_BITRATE = TARGET_MBPS * 1024 * 1024

USE_SERVICE_NAME_FOR_INDEX = False

# some sample data
services = ['frontend', 'processor']
messages = ['SyntaxError: invalid syntax', 'IndentationError: unexpected indent', "TypeError: 'list' object cannot be interpreted as an integer"]
log_levels = ['DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL']
versions = ['1.0.0', '2.0.0']
filenames = ['app.java', 'processor.java']
functions = ['foo', 'bar']
threads = ['main', 'background']
environments = ['test', 'prod']
error_types = ['java.lang.NullPointerException']
error_messages = ['The argument cannot be null']

# see https://www.elastic.co/guide/en/ecs-logging/overview/current/intro.html
def make_log_record():
    record = {}

    ## ---- mandatory
    # ideally, like "2015-01-01T12:10:30.000Z"
    record['@timestamp'] = datetime.now(tz=timezone.utc).isoformat()
    # value can be whatever you'd like
    record['log.level'] = random.choice(log_levels)
    # the log message
    record['message'] = random.choice(messages)
    # the name of the service emitting the log
    record['service.name'] = random.choice(services)

    ## ---- ideally present if message represents exception/errors:
    if record['log.level'] == 'WARN' or record['log.level'] == 'ERROR' or record['log.level'] == 'FATAL':
        record['error.type'] = random.choice(error_types)
        record['error.message'] = random.choice(error_messages)
        if record['log.level'] == 'ERROR' or record['log.level'] == 'FATAL':
            record['error.stack_trace'] = 'Exception in thread "main" java.lang.NullPointerException\n\tat org.example.App.methodName(App.java:42)'

    # ---- optional context
    record['service.version'] = random.choice(versions)
    record['service.environment'] = random.choice(environments)
    record['log.origin.file.name'] = random.choice(filenames)
    record['log.origin.file.line'] = random.randrange(0, 1000)
    record['log.origin.function'] = random.choice(functions)
    record['process.thread.name'] = random.choice(threads)

    # ---- custom fields
    record[f'{CUSTOM_FIELDS_NAMESPACE}.foo'] = 'bar'
    record[f'{CUSTOM_FIELDS_NAMESPACE}.bar'] = random.random()

    return record

def make_index_name(service, namespace):
    # put each service into their own datastream/index
    return f"logs-{service}-{namespace}"

def generate_random_hex(length):
  return ''.join(random.choice('0123456789abcdef') for _ in range(length))

# for a given source correlation_id, return a replacement (either new or cached)
@lru_cache
def regen_correlation_id(correlation_id):
    return generate_random_hex(13)

# this simulates pulling a bunch of log records from some external queue

def reader_ndjson(file, count, correlation_id_field):
    while True:
        with open(file) as f:
            batch = []
            for line in f:
                record = json.loads(line)
                record['@timestamp'] = datetime.now(tz=timezone.utc).isoformat()
                if correlation_id_field in record:
                    record[correlation_id_field] = regen_correlation_id(record[correlation_id_field])
                batch.append(record)
                if len(batch) == count:
                    yield batch
                    batch = []
            if len(batch) > 0:
                yield batch

def reader_sim(count):
    while True:
        records = []
        for i in range(count):
            log_record = make_log_record()
            records.append(log_record)
        yield records

def logs_loop(target_bitrate, ndjson_file, service, namespace, correlation_id_field):
    try:
        logger.info('starting thread')

        elasticsearch_url = os.environ['ELASTICSEARCH_URL']
        headers = {'Authorization': f"ApiKey {os.environ['ELASTICSEARCH_APIKEY']}", 'kbn-xsrf': 'reporting', 'Content-Type': 'application/x-ndjson'}
        if ENABLE_GZIP:
            headers['Content-Encoding'] = 'gzip'

        start = time.time()
        bytes_over_channel = 0
        last_report = time.time()

        successful_inserts = 0
        failed_inserts = 0
        retried_inserts = 0

        records_reader = None
        if ndjson_file is not None:
            records_reader = reader_ndjson(ndjson_file, BATCH_SIZE, correlation_id_field)
        else:
            records_reader = reader_sim(BATCH_SIZE)

        # see https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        for records in records_reader:
            batch = []
            # format per ES _bulk API
            for record in records:
                if service is None and 'service.name' in record:
                    ds_name = record['service.name']
                else:
                    ds_name = service
                batch.append({ "create" : { "_index" : make_index_name(ds_name, namespace) } })
                batch.append(record)
            # ndjson body needs to end with a newline
            payload = ndjson.dumps(batch) + "\r\n"
            # optionally gzip it (typical)
            if ENABLE_GZIP:
                payload = gzip.compress(payload.encode('utf-8'))

            # (only) for 429s (ES busy), we will retry...
            backoff_s = RETRY_BACKOFF_S
            for i in range(RETRIES+1):
                try:
                    bytes_over_channel += len(payload)
                    # try bulk insert
                    resp = requests.post(f"{elasticsearch_url}/_bulk",
                                            data=payload, timeout=TIMEOUT_S,
                                            headers=headers, verify=False)
                    # nothing inserted
                    if resp.status_code != 200:
                        # if 429 (ES busy), retry w/ backoff
                        if resp.status_code == 429:
                            # max retries exceed
                            if i == (RETRIES):
                                raise Exception("max retries exceeded")
                            logger.warning(f'429... attemping retry {i}')
                            retried_inserts += len(records)
                            # backoff before retry
                            time.sleep(backoff_s)
                            backoff_s = backoff_s * 2
                            continue
                        # non-429 error, don't retry
                        else:
                            raise Exception(resp.status_code)
                    # something inserted
                    else:
                        resp_json = resp.json()
                        # some errors
                        if resp_json['errors'] == True:
                            for item in resp_json['items']:
                                # this record was good
                                if item['create']['status'] == 201:
                                    successful_inserts += 1
                                # this record was not
                                else:
                                    logger.error(f"failed to insert doc: {item['create']['status']}: {item['create']['error']['type']} / {item['create']['error']['reason']}")
                                    failed_inserts += 1
                        # no errors
                        else:
                            successful_inserts += len(records)
                        break
                except Exception as inst:
                    logger.error(f'error inserting records: {inst}')
                    failed_inserts += len(records)
                    break

            # throttle overall upload to target bitrate
            duration_in_sec = time.time() - start
            bitrate = (bytes_over_channel * 8) / duration_in_sec
            if bitrate > target_bitrate:
                sleep = ((bytes_over_channel * 8) / target_bitrate) - duration_in_sec
                if sleep > 0:
                    time.sleep(sleep)

            # report status
            if time.time() - last_report > REPORT_S:
                logger.info(f'bps={int(bitrate)}, successful_inserts={successful_inserts}, retries={retried_inserts}, failed_inserts={failed_inserts}')
                last_report = time.time()
        logger.info(f'bps={int(bitrate)}, successful_inserts={successful_inserts}, retries={retried_inserts}, failed_inserts={failed_inserts}')
    except Exception as inst:
        logger.error(f'error inserting records: {inst}')

parser = argparse.ArgumentParser(
                    prog='bulk',
                    description='bulk ingest docs into ES')
parser.add_argument('-f', '--file')
parser.add_argument('-s', '--service')
parser.add_argument('-n', '--namespace')
parser.add_argument('-c', '--correlation_id_field')

if __name__ == "__main__":
    args = parser.parse_args()

    # start N upload threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        for i in range(THREADS):
            # divide overall target bitrate amongst threads
            executor.submit(logs_loop, TARGET_BITRATE/THREADS, args.file, args.service, args.namespace, args.correlation_id_field)
        executor.shutdown()