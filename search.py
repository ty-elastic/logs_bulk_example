import requests
import os
import json
from datetime import datetime, timezone, timedelta
import ndjson
import sys
import logging

# configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(stream=sys.stdout)
format_output = logging.Formatter('%(asctime)s : %(levelname)s : %(message)s')
stdout_handler.setFormatter(format_output)
logger.addHandler(stdout_handler)

# timeout waiting for ES to respond
TIMEOUT_S = 5
# number of records per paged search request
RECORDS_PER_SEARCH = 5000

# see https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html
# note this is a python generator function; it returns an iterable result (per page)
def search(*, service_name=None, timestamp_min=None, timestamp_max=None, log_level=None, message=None):
    try:
        body = {}
        # setup basic bool filters
        query = body['query'] = {}
        query.setdefault('bool', {}).setdefault('must', [])
        query.setdefault('bool', {}).setdefault('must_not', [])

        # if time range specified
        timestamp_constraints = {}
        if timestamp_min is not None:
            timestamp_constraints['gt'] = timestamp_min.isoformat()
        if timestamp_max is not None:
            timestamp_constraints['lt'] = timestamp_max.isoformat()
        if len(timestamp_constraints.keys()) > 0:
            query['bool']['must'].append({  "range" : { "@timestamp" : timestamp_constraints } })

        # if field specified
        if service_name is not None:
            query['bool']['must'].append({ "term": { "service.name": service_name }})        
        if log_level is not None:
            query['bool']['must'].append({ "match": { "log.level": log_level }})
        # this leverages Elasticsearch's fuzzy text search (not an exact match)
        if message is not None:
            query['bool']['must'].append({ "match": { "message": message }})

        # newest first please
        body['sort'] = [
            { "@timestamp" : {"order": "desc"}}
        ]
        
        # this many records per page
        body['size'] = RECORDS_PER_SEARCH
        # we don't need the original doc
        body['_source'] = False
        # return all fields, including the _id
        body['fields'] = ['*', '_id']

        elasticsearch_url = os.environ['ELASTICSEARCH_URL']
        headers = {'Authorization': f'ApiKey {os.environ['ELASTICSEARCH_APIKEY']}', 'kbn-xsrf': 'reporting', 'Content-Type': 'application/json'}

        # support paging
        while True:
            body_json = json.dumps(body)
            logger.info(f'querying: {body_json}')
            resp = requests.post(f"{elasticsearch_url}/logs-*/_search",
                                                data=body_json,
                                                timeout=TIMEOUT_S,
                                                headers=headers)
            resp_json = resp.json()
            if 'hits' in resp_json:
                records = []
                # done paging
                if len(resp_json['hits']['hits']) == 0:
                    break
                # results
                for hit in resp_json['hits']['hits']:
                    # ES always stories values as arrays; strip array formatting (if some fields should be arrays, have an exception list, or just always assume arrays)
                    for k, v in hit['fields'].items():
                        if isinstance(v, list) and len(v) == 1:
                            hit['fields'][k] = v[0]
                    # append to output
                    records.append(hit['fields'])
                # generate page of records upstream
                yield records
                # ask for next page on next query (see https://www.elastic.co/guide/en/elasticsearch/reference/current/paginate-search-results.html)
                body['search_after'] = resp_json['hits']['hits'][-1]['sort']
            else:
                break

    except Exception as inst:
        print(f"unable to get records", inst)
        return []

SEARCH_WINDOW_H = 24

if __name__ == "__main__":
    # example search
    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(hours=SEARCH_WINDOW_H)

    paged_results = search(service_name='processor', message='Syntax', timestamp_min=start)
    # write results to ndjson file for analysis
    with open('results.ndjson', 'w') as f:
        for result in paged_results:
            ndjson.dump(result, f)