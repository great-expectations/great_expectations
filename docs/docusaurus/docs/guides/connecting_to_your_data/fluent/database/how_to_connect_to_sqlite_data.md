---
title: How to connect to SQLite data
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to data in a SQLite database.
keywords: [Great Expectations, SQLite, SQL]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

## Introduction

In this guide we will demonstrate how to connect Great Expectations to data in a SQLite database.  We will demonstrate how to create a SQLite Datasource.  With our SQLite Datasource we will then show the methods for connecting to data in a SQLite table and connecting to data from a SQLite query.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a SQLite database
- A passion for data quality

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

```python title="Python code"
import great_expectations as gx

context = gx.get_context()
```

### 2. Determine your connection string

For this example we will use a connection string to connect to our PostgreSQL database.  In PostgreSQL, connection strings are formatted like:

```python title="Python code"
my_connection_string = "sqlite:///<PATH_TO_DB_FILE>"
```

### 3. Create a SQLite Datasource

Creating a PostgreSQL Datasource is as simple as providing the `add_sqlite(...)` method a `name` by which to reference it in the future and the `connection_string` with which to access it.

```python title="Python code"
datasource_name = "my_datasource"
my_connection_string = "sqlite:///<PATH_TO_DB_FILE>"
```

With these two values, we can create our Datasource:

```python title="Python code"
datasource = context.sources.add_sqlite(name=datasource_name, connection_string=my_connection_string)
```

:::caution Using `add_sql(...)` vs `add_sqlite(...)` to create a Datasource

The basic SQL Datasource created with `add_sql` can connect to data in a SQLite database, but it won't work as well as a SQLite Datasource from `add_sqlite(...)`.

SQLite stores datetime values as strings.  Because of this, a general SQL Datasource will see datetime columns as string columns, instead.  The SQLite Datasource has additional handling in place for these fields, and also has additional error reporting for SQLite specific issues.

If you are working with SQLite data, you should always use `add_sqlite(...)` to create your Datasource!  The `add_sql(...)` method may connect to your SQLite database, but it won't handle datetime columns properly or report errors as clearly.

:::

### 4. (Optional) Connect to the data in a table

We will indicate a table to connect to with a Table Data Asset.  This is done by providing the `add_table_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `table_name` to specify the table we wish the Data Asset to connect to.

```python title="Python code"
asset_name = "my_asset"
asset_table_name = "yellow_tripdata_sample"
```

With these two values, we can create our Data Asset:

```python title="Python code"
table_asset = datasource.add_table_asset(name=asset_name, table_name=asset_table_name)
```

### 5. (Optional) Connect to the data in a query

To indicate the query that provides data to connect to we will define a Query Data Asset.  This done by providing the `add_query_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `query` which will provide the data we wish the Data Asset to connect to.

```python title = "Python code"
asset_name = "my_asset"
assest_query = "SELECT * from yellow_tripdata_sample"
```

Once we have these two values, we can create our Data Asset with:

```python title="Python code"
table_asset = datasource.add_query_asset(name=asset_name, query=asset_query)
```

### 6. (Optional) Repeat steps 4 or 5 as needed to add additional tables or queries

If you wish to connect to additional tables or queries in the same PostgreSQL Database, simply repeat the steps above to add them as table Data Assets.

## Next steps

Now that you have connected to a SQLite Database and created a Data Asset, you may want to look into:

### Configuring SQL Data Assets further
- [How to organize Batches in a SQL based Data Asset](/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset)

### Requesting Data from a Data Asset
- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)

### Using Data Assets to create Expectations
- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)


