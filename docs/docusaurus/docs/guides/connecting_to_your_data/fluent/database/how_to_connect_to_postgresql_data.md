---
title: How to connect to a PostgreSQL database
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to data in a PostgreSQL database.
keywords: [Great Expectations, Postgres, PostgreSQL, SQL]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

import PostgreSqlConfigureCredentialsInConfigVariablesYml from '/docs/components/setup/dependencies/_postgresql_configure_credentials_in_config_variables_yml.md'

In this guide we will demonstrate how to connect Great Expectations to data in a PostgreSQL database.  We will demonstrate how to create a PostgreSQL Datasource.  With our PostgreSQL Datasource we will then show the methods for connecting to data in a PostgreSQL table and connecting to data from a PostgreSQL query.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- [An installation of GX set up to work with PostgreSQL](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- Source data stored in a PostgreSQL database

</Prerequisites> 

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Determine your connection string

For this example we will use a connection string to connect to our PostgreSQL database.  In PostgreSQL, connection strings are formatted like:

```pythonname="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py connection_string
```

:::tip Is there a more secure way to store my credentials than plain text in a connection string?

<PostgreSqlConfigureCredentialsInConfigVariablesYml />

:::

### 3. Create a PostgreSQL Datasource

Creating a PostgreSQL Datasource is as simple as providing the `add_postgres(...)` method a `name` by which to reference it in the future and the `connection_string` with which to access it.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py connection_string2
```

With these two values, we can create our Datasource:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_postgres
```

### 4. Connect to a specific set of data with a Data Asset

Now that our Datasource has been created, we will use it to connect to a specific set of data in the database it is configured for.  This is done by defining a Data Asset in the Datasource.  A Datasource may contain multiple Data Assets, each of which will serve as the interface between GX and the specific set of data it has been configured for.

With SQL databases, there are two types of Data Assets that can be used.  The first is a Table Data Asset, which connects GX to the data contained in a single table in the source database.  The other is a Query Data Asset, which connects GX to the data returned by a SQL query.  We will demonstrate how to create both of these in the following steps.  

:::tip How many Data Assets can my Datasource contain?

Although there is no set maximum number of Data Assets you can define for a datasource, there is a functional minimum.  In order for GX to retrieve data from your Datasource you will need to create *at least one* Data Asset.

:::

### 5. (Optional) Connect a Data Asset to the data in a table

We will indicate a table to connect to with a Table Data Asset.  This is done by providing the `add_table_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `table_name` to specify the table we wish the Data Asset to connect to.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py asset_name
```

With these two values, we can create our Data Asset:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_table_asset
```

### 6. (Optional) Connect a Data Asset to the data returned by a query

To indicate the query that provides data to connect to we will define a Query Data Asset.  This done by providing the `add_query_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `query` which will provide the data we wish the Data Asset to connect to.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py asset_query
```

Once we have these two values, we can create our Data Asset with:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_postgresql_data.py add_query_asset
```

### 7. (Optional) Repeat steps 5 and 6 as needed to connect to additional tables or queries

If you wish to connect to additional tables or queries in the same PostgreSQL Database, simply repeat the step above to add them as additional Data Assets.

## Next steps

Now that you have connected to a PostgreSQL database and created a Data Asset, you may want to look into:

### Configuring SQL Data Assets further
- [How to organize Batches in a SQL based Data Asset](/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset)

### Requesting Data from a Data Asset
- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)

### Using Data Assets to create Expectations
- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)





