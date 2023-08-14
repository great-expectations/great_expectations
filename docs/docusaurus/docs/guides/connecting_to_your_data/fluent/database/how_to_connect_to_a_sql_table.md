---
title: How to connect to a SQL table
tag: [how-to, connect to data]
description: A technical guide demonstrating how to connect Great Expectations to a SQL table.
keywords: [Great Expectations, SQL]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import ConnectingToSqlDatasourcesFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_sql_datasources_fluently.md'

In this guide we will demonstrate how to connect Great Expectations to a generic SQL table.  GX uses SQLAlchemy to connect to SQL data, and therefore supports most SQL dialects that SQLAlchemy does.  For more information on the SQL dialects supported by SQLAlchemy, please see [SQLAlchemy's official documentation on dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).

If you would like to connect to the results of a SQL query instead of the contents of a SQL table, please see [our guide on how to connect to SQL data using a query](/docs/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data_using_a_query), instead.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- An installation of GX set up to work with SQL
- Source data stored in a SQL database
- A SQL-based Data Source

</Prerequisites> 

<details>
<summary>

### If you still need to set up and install GX...

</summary>

Please reference the appropriate one of these guides:

<SetupAndInstallForSqlData />

</details>

<details>
<summary>

### If you still need to connect a Data Source to a SQL database...

</summary>

Please reference the appropriate one of these guides:

<ConnectingToSqlDatasourcesFluently />

</details>

:::caution Datasources defined with the block-config method

If you're using a Data Source that was created with the advanced block-config method, see [How to configure a SQL Data Source with the block-config method](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource).

:::

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Retrieve a SQL Data Source

For this guide, we will use a previously defined Data Source named `"my_datasource"` which connects to a SQL database.

To retrieve this Data Source, we will supply the `get_datasource(...)` method of our Data Context with the name of the Data Source we wish to retrieve:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py datasource
```

### 3. Add a table to the Data Source as a Data Asset

We will indicate a table to connect to by defining a Data Asset.  This is as simple as providing the `add_table_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `table_name` to specify the table we wish the Data Asset to connect to.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py create_datasource
```

### 4. (Optional) Repeat step 3 as needed to add additional tables

If you wish to connect to additional tables in the same SQL Database, simply repeat the step above to add them as table Data Assets.

## Next steps

Now that you have connected to a SQL table, you may want to look into:

### Configuring SQL Data Assets further
- [How to organize Batches in a SQL based Data Asset](/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset)

### Requesting Data from a Data Asset
- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)

### Using Data Assets to create Expectations
- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)

