---
title: How to connect to SQLite data
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to data in a SQLite database.
keywords: [Great Expectations, SQLite, SQL]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

In this guide we will demonstrate how to connect Great Expectations to data in a SQLite database.  We will demonstrate how to create a SQLite Data Source.  With our SQLite Data Source we will then show the methods for connecting to data in a SQLite table and connecting to data from a SQLite query.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with SQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a SQLite database

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Determine your connection string

For this example we will use a connection string to connect to our PostgreSQL database.  In PostgreSQL, connection strings are formatted like:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py connection_string"
```

### 3. Create a SQLite Data Source

Creating a PostgreSQL Data Source is as simple as providing the `add_sqlite(...)` method a `name` by which to reference it in the future and the `connection_string` with which to access it.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py datasource_name"
```

With these two values, we can create our Data Source:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py datasource"
```

:::caution Using `add_sql(...)` vs `add_sqlite(...)` to create a Data Source

The basic SQL Data Source created with `add_sql` can connect to data in a SQLite database, but it won't work as well as a SQLite Data Source from `add_sqlite(...)`.

SQLite stores datetime values as strings.  Because of this, a general SQL Data Source will see datetime columns as string columns, instead.  The SQLite Data Source has additional handling in place for these fields, and also has additional error reporting for SQLite specific issues.

If you are working with SQLite data, you should always use `add_sqlite(...)` to create your Data Source!  The `add_sql(...)` method may connect to your SQLite database, but it won't handle datetime columns properly or report errors as clearly.

:::

### 4. (Optional) Connect to the data in a table

We will indicate a table to connect to with a Table Data Asset.  This is done by providing the `add_table_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `table_name` to specify the table we wish the Data Asset to connect to.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py asset_name"
```

With these two values, we can create our Data Asset:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py table_asset"
```

### 5. (Optional) Connect to the data in a query

To indicate the query that provides data to connect to we will define a Query Data Asset.  This done by providing the `add_query_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `query` which will provide the data we wish the Data Asset to connect to.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py asset_query"
```

Once we have these two values, we can create our Data Asset with:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sqlite_data.py query_table_asset"
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


