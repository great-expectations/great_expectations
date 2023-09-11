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
import TechnicalTag from '@site/docs/term_tags/_tag.mdx';


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
  {label: 'BigQuery SQL', value:'bigquery'},
  ]}>
<TabItem value="sql">

## SQL

Connect GX to a SQL database to access source data.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQL](/docs/guides/setup/installation/install_gx)
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

Run one of the connection strings in your preferred SQL dialect to store the connection string in the `connection_string` variable with plain text credentials. The following code is an example of the PostgreSQL connection string format:

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

</TabItem>
<TabItem value="postgresql">

## PostgreSQL

Connect GX to a PostgreSQL database to access source data.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with PostgreSQL](/docs/guides/setup/installation/install_gx)
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

- [An installation of GX set up to work with SQLite](/docs/guides/setup/installation/install_gx)
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

- [An installation of GX set up to work with SQL](/docs/guides/setup/installation/install_gx)
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

- [An installation of GX set up to work with SQL](/docs/guides/setup/installation/install_gx)
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
<TabItem value="bigquery">

Integrate GX with [Google Cloud Platform](https://cloud.google.com/gcp) (GCP).

The following scripts and configuration files are used in the examples:

- The local GX configuration is located in the [`great-expectations` GIT repository](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/fixtures/gcp_deployment/).

- The script to test the BigQuery configuration is located in [gcp_deployment_patterns_file_bigquery.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py).

- The script to test the GCS configuration is located in [gcp_deployment_patterns_file_gcs.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py).

## Prerequisites

<Prerequisites>

- [An installation of GX set up to work with SQL](/docs/guides/setup/installation/install_gx).
- Familiarity with Google Cloud Platform features and functionality.
- A GCP project with a running Google Cloud Storage container that is accessible from your region.
- Read/write access to a BigQuery database.
- Access to a GCP [Service Account](https://cloud.google.com/iam/docs/service-accounts) with permission to access and read objects in Google Cloud Storage.

</Prerequisites>


:::caution Installing Great Expectations in Google Cloud Composer

  When installing GX in Composer 1 and Composer 2 environments the following packages must be pinned:

  `[tornado]==6.2`
  `[nbconvert]==6.4.5`
  `[mistune]==0.8.4`

:::

GX recommends that you use the following services to integrate GX with GCP:

  - [Google Cloud Composer](https://cloud.google.com/composer) (GCC) for managing workflow orchestration including validating your data. GCC is built on [Apache Airflow](https://airflow.apache.org/).

  - [BigQuery](https://cloud.google.com/bigquery) or files in [Google Cloud Storage](https://cloud.google.com/storage) (GCS) as your <TechnicalTag tag="datasource" text="Data Source"/>.

  - [GCS](https://cloud.google.com/storage) for storing metadata (<TechnicalTag tag="expectation_suite" text="Expectation Suites"/>, <TechnicalTag tag="validation_result" text="Validation Results"/>, <TechnicalTag tag="data_docs" text="Data Docs"/>).

  - [Google App Engine](https://cloud.google.com/appengine) (GAE) for hosting and controlling access to <TechnicalTag tag="data_docs" text="Data Docs"/>.

The following diagram shows the recommended components for a GX deployment in GCP:

![Screenshot of Data Docs](../../../../deployment_patterns/images/ge_and_gcp_diagram.png)

## Upgrade your GX version (Optional)

Run the following code to upgrade your GX version:

```bash
pip install great-expectations --upgrade
```

## Get DataContext

Run the following code to create a new <TechnicalTag relative="../../../" tag="data_context" text="Data Context" />: 

```YAML name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py get_context"
```

The `full_path_to_project_directory` parameter can be an empty directory where you intend to build your GX configuration.

## Connect to GCP Metadata Stores 

The code examples are located in the [`great-expectations` repository](https://github.com/great-expectations/great_expectations/tree/develop/tests/integration/fixtures/gcp_deployment/).

:::note Trailing Slashes in Metadata Store prefixes

  When specifying `prefix` values for Metadata Stores in GCS, don't add a trailing slash `/`. For example, `prefix: my_prefix/`. When you add a trailing slash, an additional folder with the name `/` is created and metadata is stored in the `/` folder instead of `my_prefix`.

:::

### Add an Expectations Store

By default, newly profiled Expectations are stored in JSON format in the `expectations/` subdirectory of your `great_expectations/` folder. A new Expectations Store can be configured by adding the following lines to your `great_expectations.yml` file. Replace the `project`, `bucket` and `prefix` with your values.

```YAML name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py expected_expectation_store"
```

To configure GX to use this new Expectations Store, `expectations_GCS_store`, set the `expectations_store_name` value in the `great_expectations.yml` file.

```YAML name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py new_expectation_store"
```

### Add a Validations Store

By default, Validations are stored in JSON format in the `uncommitted/validations/` subdirectory of your `great_expectations/` folder. You can connfigure a new Validations Store by adding the following lines to your `great_expectations.yml` file. Replace the `project`, `bucket` and `prefix` with your values.

```YAML name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py expected_validations_store"
```

To configure GX to use the new `validations_GCS_store` Validations Store, set the `validations_store_name` value in the `great_expectations.yml` file.

```YAML name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py new_validations_store"
```

### Add a Data Docs Store

To host and share Data Docs on GCS, see [Host and share Data Docs](../../../setup/configuring_data_docs/host_and_share_data_docs.md).

After you have hosted and shared Data Docs, your `great-expectations.yml` contains the following configuration below `data_docs_sites`:

```YAML name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py new_data_docs_store"
```

Run the following command in the gcloud CLI to view the deployed DataDocs site:

```bash
gcloud app browse
```

The URL to your app appears and opens in a new browser window. You can view the index page of your Data Docs site.

## Connect to source data

Connect to source data stored on a GCS or .

<Tabs
  groupId="connect-to-data-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

Run the following code to add the name of your GCS bucket to the `add_pandas_gcs` function:

```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py datasource"
```

In the example, you've added a Data Source that connects to data in GCS using a Pandas dataframe. The name of the new datasource is `gcs_datasource` and it refers to a GCS bucket named `test_docs_data`.

For more information about configuring a Data Source, see [How to connect to data on GCS using Pandas](/docs/0.15.50/guides/connecting_to_your_data/cloud/gcs/pandas).

</TabItem>
<TabItem value="bigquery">

:::note

Tables that are created by BigQuery queries are automatically set to expire after one day.

:::

Run the following code to create a Data Source that connects to data in BigQuery:

```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py add_bigquery_datasource"
```

In the example, you created a Data Source named `my_bigquery_datasource`, using the `add_or_update_sql` method and passed it in a connection string.

To configure the BigQuery Data Source, see [How to connect to a BigQuery database](/docs/0.15.50/guides/connecting_to_your_data/database/bigquery).

</TabItem>
</Tabs>

## Create Assets
<Tabs
  groupId="connect-to-data-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

Use the `add_csv_asset` function to add a CSV `Asset` to your `Datasource`.

Configure the `prefix` and `batching_regex`. The `prefix` is the path to the GCS bucket where the files are located. `batching_regex` is a regular expression that indicates which files should be treated as Batches in the Asset, and how to identify them. For example:

```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py prefix_and_batching_regex"
```

In the example, the pattern `r"data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_(?P<year>\d{4})-(?P<month>\d{2})\.csv"` builds a Batch for the following files in the GCS bucket:

```bash
test_docs_data/data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv
test_docs_data/data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-02.csv
test_docs_data/data/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-03.csv
```
The `batching_regex` pattern matches the four digits of the year portion and assigns it to the `year` domain, and then matches the two digits of the month portion and assigns it to the `month` domain.

Run the following code to use the `add_csv_asset` function to add an `Asset` named `csv_taxi_gcs_asset` to your Data Source: 

```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py asset"
```

</TabItem>
<TabItem value="bigquery">

You can add a BigQuery `Asset` into your `Datasource` as a table asset or query asset.

In the following example, a table `Asset` named `my_table_asset` is built by naming the table in your BigQuery Database. 

```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py add_bigquery_table_asset"
```

In the following example, a query `Asset` named `my_query_asset` is built by submitting a query to the `taxi_data` table.

```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py add_bigquery_query_asset"
```

</TabItem>
</Tabs>

## Get a Batch and Create Expectation Suite

<Tabs
  groupId="connect-to-data-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

1. Use the `add_or_update_expectation_suite` method on your Data Context to create an Expectation Suite:

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py add_expectation_suite"
    ```

2. Use the `Validator` method to run Expectations on the Batch and automatically add them to the Expectation Suite. 

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py validator_calls"
    ```
    In this example, you're adding `expect_column_values_to_not_be_null` and `expect_column_values_to_be_between`. You can replace the `passenger_count` and `congestion_surcharge` test data columns with your own data columns.

3. Run the following code to save the Expectation Suite:

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py save_expectation_suite"
    ```

To configure the RuntimeBatchRequest and learn how you can load data by specifying a GCS path to a single CSV, see [How to connect to data on GCS using Pandas](/docs/0.15.50/guides/connecting_to_your_data/cloud/gcs/pandas).

</TabItem>
<TabItem value="bigquery">

1. Use the `table_asset` you created previously to build a `BatchRequest`: 

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py batch_request"
    ```

2. Use the `add_or_update_expectation_suite` method on your Data Context to create an Expectation Suite and get a `Validator`: 

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py add_or_update_expectation_suite"
    ```

3. Use the `Validator` to run expectations on the batch and automatically add them to the Expectation Suite: 

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py validator_calls"
    ```
    In this example, you're adding `expect_column_values_to_not_be_null` and `expect_column_values_to_be_between`. You can replace the `passenger_count` and `congestion_surcharge` test data columns with your own data columns.
    
    
4. Run the following code to save the Expectation Suite containing the two Expectations:

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py save_expectation_suite"
    ```

To configure the BatchRequest and learn how you can load data by specifying a table name, see [How to connect to a BigQuery database](/docs/0.15.50/guides/connecting_to_your_data/database/bigquery).

</TabItem>
</Tabs>

## Build and run a Checkpoint

<Tabs
  groupId="connect-to-data-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

1. Run the following code to add the `gcs_checkpoint` Checkpoint to the Data Context:  

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py checkpoint"
    ```
    In this example, you're using the `BatchRequest` and `ExpectationSuite` names that you used when you created your Validator.

2. Run the Checkpoint by calling `checkpoint.run()`:

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_gcs.py run_checkpoint"
    ```

</TabItem>
<TabItem value="bigquery">

1. Run the following code to add the `bigquery_checkpoint` Checkpoint to the Data Context:

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py checkpoint"
    ```
    In this example, you're using the `BatchRequest` and `ExpectationSuite` names that you used when you created your Validator.

2. Run the Checkpoint by calling `checkpoint.run()`:

    ```python name="tests/integration/docusaurus/deployment_patterns/gcp_deployment_patterns_file_bigquery.py run_checkpoint"
    ```

</TabItem>
</Tabs>

## Migrate your local configuration to Cloud Composer

Migrate your local GX configuration to a Cloud Composer environment to automate the workflow. You can use one of the following methods to run GX in Cloud Composer or Airflow:

- [Using a `bash operator`](../../../../deployment_patterns/how_to_use_great_expectations_with_airflow.md)

- [Using a `python operator`](../../../../deployment_patterns/how_to_use_great_expectations_with_airflow.md#option-2-running-the-checkpoint-script-output-with-a-pythonoperator)

- [Using a `Airflow operator`](https://github.com/great-expectations/airflow-provider-great-expectations)

In this example, you'll use the `bash operator` to run the Checkpoint. A video overview of this process is also available in this [video](https://drive.google.com/file/d/1YhEMqSRkp5JDIQA_7fleiKTTlEmYx2K8/view?usp=sharing).

### Create and Configure a GCP Service Account

To create a GCP Service Account, see [Service accounts overview](https://cloud.google.com/iam/docs/service-accounts).

To run GX in a Cloud Composer environment, the following privileges are required for your Service Account:

- `Composer Worker`
- `Logs Viewer`
- `Logs Writer`
- `Storage Object Creator`
- `Storage Object Viewer`

If you are accessing data in BigQuery, the following privileges are required for your Service Account:

- `BigQuery Data Editor`
- `BigQuery Job User`
- `BigQuery Read Session User`

### Create a Cloud Composer environment

See [Create Cloud Composer environments](https://cloud.google.com/composer/docs/composer-2/create-environments).

### Install Great Expectations in Cloud Composer

You can use the Composer web Console (recommended), `gcloud`, or a REST query to install Python dependencies in Cloud Composer. To install `great-expectations` in Cloud Composer, see [Installing Python dependencies in Google Cloud](https://cloud.google.com/composer/docs/how-to/using/installing-python-dependencies#install-package). If you are connecting to data in BigQuery, make sure `sqlalchemy-bigquery` is also installed in your Cloud Composer environment.

:::info Troubleshooting your installation
If you run into trouble when you install GX in Cloud Composer, see [Troubleshooting PyPI package installation](https://cloud.google.com/composer/docs/troubleshooting-package-installation).
:::

### Move your local configuration to Cloud Composer

Cloud Composer uses Cloud Storage to store Apache Airflow DAGs (also known as workflows), with each Environment having an associated Cloud Storage bucket. Typically, the bucket name uses this pattern: `[region]-[composer environment name]-[UUID]-bucket`.

To migrate your local configuration, you can move the local `great_expectations/` folder to the Cloud Storage bucket where Composer can access the configuration.

1. Open the **Environments** page in the Cloud Console and then click the environment name to open the **Environment details** page. The name of the Cloud Storage bucket is located to the right of the DAGs folder on the **Configuration** tab.

    This is the folder where DAGs are stored. You can access it from the Airflow worker nodes at: `/home/airflow/gcsfuse/dags`. The location where you'll upload `great_expectations/` is **one level above the `/dags` folder**.

2. Upload the local `great_expectations/` folder by dragging and dropping it into the window, using [`gsutil cp`](https://cloud.google.com/storage/docs/gsutil/commands/cp), or by clicking the `Upload Folder` button.

    After the `great_expectations/` folder is uploaded to the Cloud Storage bucket, it is mapped to the Airflow instances in your Cloud Composer and is accessible from the Airflow Worker nodes at: `/home/airflow/gcsfuse/great_expectations`.

### Write the DAG and add it to Cloud Composer
<Tabs
  groupId="connect-to-data-gcs-bigquery"
  defaultValue='gcs'
  values={[
  {label: 'Data in GCS', value:'gcs'},
  {label: 'Data in BigQuery', value:'bigquery'},
  ]}>
<TabItem value="gcs">

1. Run the following code to create a DAG with a single node (`t1`) that runs a `BashOperator`:

    ```python name="tests/integration/fixtures/gcp_deployment/ge_checkpoint_gcs.py full"
    ```
    The DAG is stored in a file named: [`ge_checkpoint_gcs.py`](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/fixtures/gcp_deployment/ge_checkpoint_gcs.py)

    The `BashOperator` changes the directory to `/home/airflow/gcsfuse/great_expectations`, where you uploaded your local configuration.

2. Run the following command in the gcloud CLI to run the Checkpoint locally:

    ```bash
    great_expectations checkpoint run gcs_checkpoint
    ````

To add the DAG to Cloud Composer, you move `ge_checkpoint_gcs.py` to the environment's DAGs folder in Cloud Storage. For more information about adding or updating DAGs, see [Add or update a DAG](https://cloud.google.com/composer/docs/how-to/using/managing-dags#adding).

1. Open the **Environments** page in the Cloud Console and then click the environment name to open the **Environment details** page.

2. On the **Configuration** tab, click the Cloud Storage bucket name located to the right of the DAGs folder. 

3. Upload the local copy of the DAG.

</TabItem>
<TabItem value="bigquery">

1. Run the following code to create a DAG with a single node (`t1`) that runs a `BashOperator`

    ```python name="tests/integration/fixtures/gcp_deployment/ge_checkpoint_bigquery.py full"
    ```
    The DAG is stored in a file named: [`ge_checkpoint_bigquery.py`](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/fixtures/gcp_deployment/ge_checkpoint_bigquery.py)

    The `BashOperator` changes the directory to `/home/airflow/gcsfuse/great_expectations`, where you uploaded your local configuration.

2. Run the following command in the gcloud CLI to run the Checkpoint locally::

    ```bash
    great_expectations checkpoint run bigquery_checkpoint
    ```

To add the DAG to Cloud Composer, you move `ge_checkpoint_bigquery.py` to the environment's DAGs folder in Cloud Storage. For more information about adding or updating DAGs, see [Add or update a DAG](https://cloud.google.com/composer/docs/how-to/using/managing-dags#adding).

1. Open the **Environments** page in the Cloud Console and then click the environment name to open the **Environment details** page.

2. On the **Configuration** tab, click the Cloud Storage bucket name located to the right of the DAGs folder. 

3. Upload the local copy of the DAG.

</TabItem>
</Tabs>


### Run the DAG and the Checkpoint

Use one of the following methods to [trigger the DAG](https://cloud.google.com/composer/docs/triggering-dags):

- [Manually](https://cloud.google.com/composer/docs/triggering-dags#manually)

- [On a schedule](https://cloud.google.com/composer/docs/triggering-dags#schedule)

- [In response to events](http://airflow.apache.org/docs/apache-airflow/stable/concepts/sensors.html)

To trigger the DAG manually:

1. Open the **Environments** page in the Cloud Console and then click the environment name to open the **Environment details** page. 

2. In the **Airflow webserver** column, click the Airflow link for your environment to open the Airflow web interface for your Cloud Composer environment. 

3. Click **Trigger Dag** to run your DAG configuration.

    When the DAG run is successful, the `Success` status appears on the DAGs page of the Airflow Web UI. You can also check that new Data Docs have been generated by accessing the URL to your `gcloud` app.

</TabItem>
</Tabs>

## Related documentation

- [How to organize Batches in a SQL based Data Asset](/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset)

- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)

- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)

- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)

- [Configure Expectation Stores](../../../setup/configuring_metadata_stores/configure_expectation_stores.md)

- [Configure Validation Result Stores](../../../setup/configuring_metadata_stores/configure_result_stores.md)

- [Host and share Data Docs](../../../setup/configuring_data_docs/host_and_share_data_docs.md)

- [Configure credentials](../../../setup/configuring_data_contexts/how_to_configure_credentials.md)
