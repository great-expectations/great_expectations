---
title: My data is in a database
---

Great! GE supports the following Databases:
- Postgres
- Sqlite
- Snowflake
- MsSQL
- MySql
- Oracle


## My Data is in .. Postgres

### First make sure you have the necessary pre-requisites

You will need
1. credentials
2. required packages

There are two ways to add credentials :
- [connection_string](some_document)\
- [credentials](some_document)

Make sure you have pre-reqs:
```python
pip install sqlalchemy
```

### Here is your configuration

Great here is an example of your configuration:

```python file=../../../../integration/code/query_postgres_runtime_data_connector.py#L11-L25
```
