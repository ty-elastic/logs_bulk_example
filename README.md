# ES log record bulk insert example

This is a simple example python script showing how to use the [Elasticsearch _bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) to batch upload log records. You can access the _bulk API from any programming language (or `curl`, or `wget`, ...). Most of the code here deals with some sembelence of error handling, retries, throttling, and reporting.

## Environment Variables

This python script requires the following variables to be set:

| Variable Name | Description |
| ------------- | ----------- |
| ELASTICSEARCH_URL | URL (including https://...) of your Elasticsearch server |
| ELASTICSEARCH_APIKEY | an API key created under the `ELASTICSEARCH_USER` |

## Requirements

* working python3 install
* a target Elasticsearch cluster

## Setup

`pip install -r requirements.txt`

## Execution

`python bulk.py`