# ES log record bulk insert and search example

This project includes a simple example python script ([bulk.py](bulk.py)) showing how to use the [Elasticsearch _bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) to batch upload log records. Most of the code here deals with some sembelence of error handling, retries, throttling, and reporting.

It also includes a simple example python script ([search.py](search.py))showing how to use the [Elasticsearch _search API](https://www.elastic.co/guide/en/elasticsearch/reference/current/search-search.html) to batch search log records. Most of the code here deals with paging.

You can access the Elasticsearch APIs shown here directly from many programming languages (e.g., [Python](https://www.elastic.co/guide/en/elasticsearch/client/python-api/current/index.html)) or via any http request library or via `curl`.

## Environment Variables

This python script requires the following variables to be set:

| Variable Name | Description |
| ------------- | ----------- |
| ELASTICSEARCH_URL | URL (including https://...) of your Elasticsearch server |
| ELASTICSEARCH_APIKEY | an API key created under the `ELASTICSEARCH_USER` (see [API Keys](https://www.elastic.co/guide/en/kibana/current/api-keys.html)) |

## Requirements

* working python3 install
* a target Elasticsearch cluster

## Setup

`pip install -r requirements.txt`

## Bulk Insert Execution

`python3 bulk.py -f data/cal.ndjson -s services -n cal -c com.paypal.cal.correlation_id`

(this will create N threads and run forever)

## Search Execution

`python search.py`

(this will run a search and dump the resulting records to a ndjson file)