---
title: My data is in a database
---

Great! GE supports the following Databases:
- [Postgres](#my-data-is-in-postgres)
- Sqlite
- Snowflake
- MsSQL
- MySql
- Oracle


## My Data is in postgres

### First make sure you have the necessary pre-requisites

You will need
1. credentials
2. required packages

There are two ways to add credentials :
- connection_string (link tbd)
- credentials (link tbd)

Make sure you have pre-reqs:
```console
pip install sqlalchemy psycopg2
```

### Here is your configuration

Great here is an example of your configuration:

```python file=../../../../integration/code/query_postgres_runtime_data_connector.py#L19-L33
```

Here is an example of loading a batch from a query.

```python file=../../../../integration/code/query_postgres_runtime_data_connector.py#L38-L44
```
