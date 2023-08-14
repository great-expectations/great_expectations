---
title: How to connect to a SQL database
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to a SQL database.
keywords: [Great Expectations, SQL]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ## Next steps -->
import AfterCreateSqlDatasource from '/docs/components/connect_to_data/next_steps/_after_create_sql_datasource.md'

In this guide we will demonstrate how to connect Great Expectations to SQL databases.  GX uses SQLAlchemy to connect to SQL data, and therefore supports most SQL dialects that SQLAlchemy does.  For more information on the SQL dialects supported by SQLAlchemy, please see [SQLAlchemy's official documentation on dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- An installation of GX set up to work with SQL
- Source data stored in a SQL database

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Determine your connection string

GX supports a variety of different SQL source data systems.  However, most SQL dialects have their own specifications for how to define a connection string.  You should reference the corresponding dialect's official documentation to determine the connection string for your SQL Database.

:::info Some examples of different connection strings:

The following are examples of connection strings for different SQL dialects:

- AWS Athena: `awsathena+rest://@athena.<REGION>.amazonaws.com/<DATABASE>?s3_staging_dir=<S3_PATH>`
- BigQuery: `bigquery://<GCP_PROJECT>/<BIGQUERY_DATASET>?credentials_path=/path/to/your/credentials.json`
- MSSQL: `mssql+pyodbc://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?driver=<DRIVER>&charset=utf&autocommit=true`
- MySQL: `mysql+pymysql://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>`
- PostGreSQL: `postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>`
- Redshift: `postgresql+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>`
- Snowflake]: `snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>&application=great_expectations_oss`
- SQLite: `sqlite:///<PATH_TO_DB_FILE>`
- Trino: `trino://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<CATALOG>/<SCHEMA>`

:::

For purposes of this guide's examples, we will connect to a PostGreSQL database.  Here is an example of our connection string, stored in the variable `connection_string` with plain text credentials:


```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py sql_connection_string"
```

:::tip Is there a more secure way to include my credentials?

You can use either environment variables or a key in `config_variables.yml` to safely store any passwords needed by your connection string.  After defining your password in one of those ways, you can reference it in your connection string like this:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py connection_string"
```

In the above example `MY_PASSWORD` would be the name of the environment variable or the key to the value in `config_variables.yml` that corresponds to your password.

If you include a password as plain text in your connection string when you define your Data Source, GX will automatically strip it out, add it to `config_variables.yml` and substitute it in the Data Source's saved configuration with a variable as was shown above.

:::

### 3. Create a SQL Data Source

Creating a SQL Data Source is as simple as providing the `add_sql(...)` method a `name` by which to reference it in the future and the `connection_string` with which to access it.


```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py add_sql"
```

## Next steps

Now that you have connected to a SQL database, next you will want to:

<AfterCreateSqlDatasource />
