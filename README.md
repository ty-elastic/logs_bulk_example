# ES log record bulk insert example

This is a simple example python script showing how to use the Elasticsearch _bulk API to batch upload log records in ECS format.

## Environment Variables

This python script requires the following variables to be set:

| Variable Name | Description |
| ------------- | ----------- |
| ELASTICSEARCH_URL | URL (including https://...) of your Elasticsearch server |
| ELASTICSEARCH_APIKEY | an API key created under the `ELASTICSEARCH_USER` |