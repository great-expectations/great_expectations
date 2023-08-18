---
sidebar_label: "Connect to SQL database source data"
title: "Connect to SQL database source data"
id: connect_sql_source_data
description: Connect to source data stored on SQL databases.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'
import AfterCreateSqlDatasource from '/docs/components/connect_to_data/next_steps/_after_create_sql_datasource.md'
import PostgreSqlConfigureCredentialsInConfigVariablesYml from '/docs/components/setup/dependencies/_postgresql_configure_credentials_in_config_variables_yml.md'

import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Use the information provided here to connect to source data stored in SQL databases. Great Expectations (GX) uses SQLAlchemy to connect to SQL source data, and most of the SQL dialects supported by SQLAlchemy are also supported by GX. For more information about the SQL dialects supported by SQLAlchemy, see [Dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).

<Tabs
  groupId="connect-sql-source-data"
  defaultValue='sql'
  values={[
  {label: 'SQL', value:'sql'},
  {label: 'PostgreSQL', value:'postgresql'},
  {label: 'SQLite', value:'sqlite'},
  {label: 'Snowflake', value:'snowflake'},
  {label: 'Databricks SQL', value:'databricks'},
  ]}>
<TabItem value="sql">

## SQL

Connect GX to a SQL database to access source data.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a SQL database

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Determine your connection string

GX supports numerous SQL source data systems.  However, most SQL dialects have their own specifications for defining a connection string. See the dialect documentation to determine the connection string for your SQL database.

:::info Some examples of different connection strings:

The following are some of the connection strings that are available for different SQL dialects:

- AWS Athena: `awsathena+rest://@athena.<REGION>.amazonaws.com/<DATABASE>?s3_staging_dir=<S3_PATH>`
- BigQuery: `bigquery://<GCP_PROJECT>/<BIGQUERY_DATASET>?credentials_path=/path/to/your/credentials.json`
- MSSQL: `mssql+pyodbc://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?driver=<DRIVER>&charset=utf&autocommit=true`
- MySQL: `mysql+pymysql://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>`
- PostgreSQL: `postgresql+psycopg2://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>`
- Redshift: `postgresql+psycopg2://<USER_NAME>:<PASSWORD>@<HOST>:<PORT>/<DATABASE>?sslmode=<SSLMODE>`
- Snowflake: `snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>&application=great_expectations_oss`
- SQLite: `sqlite:///<PATH_TO_DB_FILE>`
- Trino: `trino://<USERNAME>:<PASSWORD>@<HOST>:<PORT>/<CATALOG>/<SCHEMA>`

:::

The following code examples use a PostgreSQL connection string. A PostgreSQL connection string connects GX to the SQL database.

Run the following code to store the connection string in the `connection_string` variable with plain text credentials:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py sql_connection_string"
```

:::tip Is there a more secure way to include my credentials?

You can use environment variables or a key in `config_variables.yml` to store connection string passwords.  After you define your password, you reference it in your connection string similar to this example:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py connection_string"
```

In the previous example `MY_PASSWORD` is the name of the environment variable, or the key to the value in `config_variables.yml` that corresponds to your password.

If you include a password as plain text in your connection string when you define your Data Source, GX automatically removes it, adds it to `config_variables.yml`, and substitutes it in the Data Source saved configuration with a variable.

:::

### Create a SQL Data Source

Run the following Python code to create a SQL Data Source:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data.py add_sql"
```

## PostgreSQL

Connect GX to a PostgreSQL database to access source data.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with PostgreSQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a PostgreSQL database

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Determine your connection string

The following code examples use a PostgreSQL connection string. A PostgreSQL connection string connects GX to the PostgreSQL database.

The following code is an example of a PostgreSQL connection string format:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgreql_data.py connection_string"
```

:::tip Is there a more secure way to store my credentials than plain text in a connection string?

<PostgreSqlConfigureCredentialsInConfigVariablesYml />

:::

### Create a PostgreSQL Data Source

1. Run the following Python code to set the `name` and `connection_string` variables:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py connection_string2"
    ```

2. Run the following Python code to create a PostgreSQL Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_postgres"
    ```

### Connect to a specific set of data with a Data Asset

To connect the Data Source to a specific set of data in the database, you define a Data Asset in the Data Source. A Data Source can contain multiple Data Assets. Each Data Asset acts as the interface between GX and the specific set of data it is configured for.

With SQL databases, you can use Table or Query Data Assets. The Table Data Asset connects GX to the data contained in a single table in the source database. The Query Data Asset connects GX to the data returned by a SQL query.

:::tip Maximum allowable Data Assets for a Data Source

Although there isn't a maximum number of Data Assets you can define for a Data Source, you must create a single Data Asset to allow GX to retrieve data from your Data Source.

:::

### Connect a Data Asset to the data in a table (Optional)

1. Run the following Python code to identify the table to connect to with a Table Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py asset_name"
    ```

2.  Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_table_asset"
    ```

### Connect a Data Asset to the data returned by a query (Optional)

1. Run the following Python code to define a Query Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py asset_query"
    ```

2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_query_asset"
    ```

### Connect to additional tables or queries (Optional)

Repeat the previous steps to add additional Data Assets.

</TabItem>
<TabItem value="sqlite">

## SQLite

Connect GX to a SQLite database to access source data.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQLite](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a SQLite database

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Determine your connection string

The following code examples use a SQLite connection string. A SQLite connection string connects GX to the SQLite database.

The following code is an example of a SQLite connection string format:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py connection_string"
```

### Create a SQLite Data Source

1. Run the following Python code to set the `name` and `connection_string` variables:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py datasource_name"
    ```

2. Run the following Python code to create a SQLite Data Source:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py datasource"
    ```

    :::caution Using `add_sql(...)` instead of `add_sqlite(...)`

    The SQL Data Source created with `add_sql` can connect to data in a SQLite database. However, `add_sqlite(...)` is the preferred method.

    SQLite stores datetime values as strings.  Because of this, a general SQL Data Source sees datetime columns as string columns. A SQLite Data Source has additional handling in place for these fields, and also has additional error reporting for SQLite specific issues.

    If you are working with SQLite source data, use `add_sqlite(...)` to create your Data Source.
    :::

### Connect to the data in a table (Optional)

1. Run the following Python code to set the `asset_name` and `asset_table_name` variables:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py asset_name"
    ```

2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py table_asset"
    ```

### Connect to the data in a query (Optional)

1. Run the following Python code to define a Query Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py asset_query"
    ```
2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py query_table_asset"
    ```

### Add additional tables or queries (Optional)

Repeat the previous steps to add additional Data Assets.


</TabItem>
<TabItem value="snowflake">

## Snowflake

Connect GX to a Snowflake database to access source data.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a Snowflake database

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Determine your connection string

The following code examples use a Snowflake connection string. A Snowflake connection string connects GX to the Snowflake database.

The following code is an example of a Snowflake connection string format:

```python
 my_connection_string = "snowflake://<USER_NAME>:<PASSWORD>@<ACCOUNT_NAME_OR_LOCATOR>/<DATABASE_NAME>/<SCHEMA_NAME>?warehouse=<WAREHOUSE_NAME>&role=<ROLE_NAME>"
```

:::info Account Names and Locators

Snowflake accepts both account names and account locators as valid account identifiers when constructing a connection string. 

Account names uniquely identify an account within your organization and are the preferred method of account identification.

Account locators act in the same manner but are auto-generated by Snowflake based on the cloud platform and region used.

For more information on both methods, please visit [Snowflake's official documentation on account identifiers](https://docs.snowflake.com/en/user-guide/admin-account-identifier)
:::

### Create a Snowflake Data Source

1. Run the following Python code to set the `name` and `connection_string` variables:

    ```python
    datasource_name = "my_snowflake_datasource"
    ```

2. Run the following Python code to create a Snowflake Data Source:

    ```python 
    datasource = context.sources.add_snowflake(
        name=datasource_name, 
        connection_string=my_connection_string, # Or alternatively, individual connection args
    )
    ```

:::info Passing individual connection arguments instead of `connection_string`

Although a connection string is the standard way to yield a connection to a database, the Snowflake datasource supports 
individual connection arguments to be passed in as an alternative.

The following arguments are supported:
  - `account`
  - `user`
  - `password`
  - `database`
  - `schema`
  - `warehouse`
  - `role`
  - `numpy`

Passing these values as keyword args to `add_snowflake` is functionally equivalent to passing in a `connection_string`.

For more information, check out Snowflake's official documentation on [the Snowflake SQLAlchemy toolkit](https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy).

:::


### Connect to the data in a table (Optional)

1. Run the following Python code to set the `asset_name` and `asset_table_name` variables:

    ```python
    asset_name = "my_asset"
    asset_table_name = my_table_name
    ```

2. Run the following Python code to create the Data Asset:

    ```python
    table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
    ```

### Connect to the data in a query (Optional)

1. Run the following Python code to define a Query Data Asset:

    ```python
    asset_name = "my_query_asset"
    query = "SELECT * from yellow_tripdata_sample_2019_01"
    ```
2. Run the following Python code to create the Data Asset:

    ```python
    query_asset = datasource.add_query_asset(name=asset_name, query=query)
    ```

### Add additional tables or queries (Optional)

Repeat the previous steps to add additional Data Assets.


</TabItem>
<TabItem value="databricks">

## Databricks SQL

Connect GX to Databricks to access source data.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a Databricks cluster

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Determine your connection string

The following code examples use a Databricks SQL connection string. A connection string connects GX to Databricks.

The following code is an example of a Databricks SQL connection string format:

```python
my_connection_string = f"databricks://token:{token}@{host}:{port}/{database}?http_path={http_path}&catalog={catalog}&schema={schema}"
```

### Create a Databricks SQL Data Source

1. Run the following Python code to set the `name` and `connection_string` variables:

    ```python
    datasource_name = "my_databricks_sql_datasource"
    ```

2. Run the following Python code to create a Snowflake Data Source:

    ```python 
    datasource = context.sources.add_databricks_sql(
        name=datasource_name, 
        connection_string=my_connection_string,
    )
    ```

### Connect to the data in a table (Optional)

1. Run the following Python code to set the `asset_name` and `asset_table_name` variables:

    ```python
    asset_name = "my_asset"
    asset_table_name = my_table_name
    ```

2. Run the following Python code to create the Data Asset:

    ```python
    table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
    ```

### Connect to the data in a query (Optional)

1. Run the following Python code to define a Query Data Asset:

    ```python
    asset_name = "my_query_asset"
    query = "SELECT * from yellow_tripdata_sample_2019_01"
    ```
2. Run the following Python code to create the Data Asset:

    ```python
    query_asset = datasource.add_query_asset(name=asset_name, query=query)
    ```

### Add additional tables or queries (Optional)

Repeat the previous steps to add additional Data Assets.

</TabItem>
</Tabs>

## Related documentation

- [How to organize Batches in a SQL based Data Asset](/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset)

- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)

- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)

- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)
