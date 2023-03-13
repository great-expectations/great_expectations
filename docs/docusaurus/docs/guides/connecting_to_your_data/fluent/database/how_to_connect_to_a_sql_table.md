---
title: How to connect to a SQL table
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to a SQL table.
keywords: [Great Expectations, SQL]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

## Introduction

In this guide we will demonstrate how to connect Great Expectations to a generic SQL table.  GX uses SQLAlchemy to connect to SQL data, and therefore supports most SQL dialects that SQLAlchemy does.  For more information on the SQL dialects supported by SQLAlchemy, please see [SQLAlchemy's official documentation on dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).

If you would like to connect to the results of a SQL query instead of the contents of a SQL table, please see [our guide on how to connect to a SQL query](/docs/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data_using_a_query.md), instead.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases.md)
- Source data stored in a SQL database
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

```python title="Python code"
import great_expectations as gx

context = gx.get_context()
```

### 2. Determine your connection string

GX supports a variety of different SQL source data systems.  However, most SQL dialects have their own specifications for how to define a connection string.  You should reference the corresponding dialect's official documentation to determine the connection string for your SQL Database.

:::info Some examples of different connection strings:

Here are some examples of connection strings for various SQL dialects.  GX also has dialect-specific guides on setting up any extra dependencies, configuring credentials, and using the advanced block-config method of connecting to these particular SQL database types.  These guides are included as the links in the following list of connection string formats.

- [AWS Athena](/docs/guides/connecting_to_your_data/database/athena.md): `awsathena+rest://@athena.<REGION>.amazonaws.com/<DATABASE>?s3_staging_dir=<S3_PATH>`
- [BigQuery](/docs/guides/connecting_to_your_data/database/bigquery.md): `bigquery://<GCP_PROJECT>/<BIGQUERY_DATASET>`
- [MSSQL](/docs/guides/connecting_to_your_data/database/mssql.md): `mssql+pyodbc://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?driver=<DRIVER>&charset=utf&autocommit=true`
- [MySQL](/docs/guides/connecting_to_your_data/database/mysql.md): `mysql+pymysql://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>`
- [PostgreSQL](/docs/guides/connecting_to_your_data/database/postgres.md): `postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>`
- [Redshift](/docs/guides/connecting_to_your_data/database/redshift.md): `postgresql+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>`
- [Snowflake](/docs/guides/connecting_to_your_data/database/snowflake.md): `snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>&application=great_expectations_oss`
- [SQLite](/docs/guides/connecting_to_your_data/database/sqlite.md): `sqlite:///<PATH_TO_DB_FILE>`
- [Trino](/docs/guides/connecting_to_your_data/database/trino.md): `trino://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<CATALOG>/<SCHEMA>`

:::

For purposes of this guide's examples, we will connect to a PostgreSQL database.  Here is an example of our connection string, stored in the variable `sql_connection_string` with plain text credentials:


```python title="Python code"
sql_connection_string = "postgresql+psycopg2://username:my_password@localhost/test"
```

:::tip Is there a more secure way to include my credentials?

You can use either environment variables or a key in `config_variables.yml` to safely store any passwords needed by your connection string.  After defining your password in one of those ways, you can reference it in your connection string like this:

```python title="Python code"
connection_string="postgresql+psycopg2://<USERNAME>:${MY_PASSWORD}@<HOST>:<PORT>/<DATABASE>"
```

In the above example `MY_PASSWORD` would be the name of the environment variable or the key to the value in `config_variables.yml` that corresponds to your password.

If you include a password as plain text in your connection string when you define your Datasource, GX will automatically strip it out, add it to `config_variables.yml` and substitute it in the Datasource's saved configuration with a variable as was shown above.

:::

### 3. Create a SQL Datasource

Creating a SQL Datasource is as simple as providing the `add_sql(...)` method a `name` by which to reference it in the future and the `connection_string` with which to access it.

```python title="Python code"
datasource = context.sources.add_sql(name="my_datasource", connection_string=sql_connection_string)
```

### 4. Add a table to the Datasource as a Data Asset

We will indicate a table to connect to by defining a Data Asset.  This is as simple as providing the `add_table_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `table_name` to specify the table we wish the Data Asset to connect to.

```python title="Python code"
table_asset = datasource.add_table_asset(name="my_asset", table_name="yellow_tripdata_sample")
```

### 5. (Optional) Repeat steps 4 as needed to add additional tables

If you wish to connect to additional tables in the same SQL Database, simply repeat the steps above to add them as table Data Assets.

## Next steps

Now that you have a SQL Datasource, you may be interested in:
- How to configure a SQL Data Asset to split its data into multiple Batches
- How to configure a SQL Data Asset to provide a sampling of its full data
- How to request a Batch of data from a Datasource
- How to create Expectations while interactively evaluating a set of data
- How to use a Data Assistant to evaluate data


