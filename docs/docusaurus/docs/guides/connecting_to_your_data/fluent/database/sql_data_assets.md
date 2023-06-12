---
sidebar_label: "Manage SQL Data Assets"
title: "Manage SQL Data Assets"
id: sql_data_assets
description: Connect Great Expectations to SQL Data Assets.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'
import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import ConnectingToSqlDatasourcesFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_sql_datasources_fluently.md'
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';


A Data Asset is a collection of records within a Datasource that define how Great Expectations (GX) organizes data into Batches. Use the information provided here to connect GX to SQL tables and data returned by SQL database queries and learn how to organize Batches in a SQL Data Asset.

 Great Expectations (GX) uses SQLAlchemy to connect to SQL Source Data, and most of the SQL dialects supported by SQLAlchemy are also supported by GX. For more information about the SQL dialects supported by SQLAlchemy, see [Dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).

 :::caution Datasources defined with the block-config method

If you're using a Datasource created with the block-config method, see [How to configure a SQL Datasource with the block-config method](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource).

:::

<Tabs
  groupId="manage-sql-data-assets"
  defaultValue='table'
  values={[
  {label: 'Connect to a SQL table', value:'table'},
  {label: 'Connect to SQL data using a query', value:'query'},
  {label: 'Organize Batches', value:'batches'},
  ]}>
<TabItem value="table">

## Connect to a SQL table

Connect GX to a SQL table to access Source Data. To connect to the results of a SQL query, see [our guide on how to connect to SQL data using a query](/docs/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data_using_a_query).

The following code examples use a previously defined Datasource named `"my_datasource"` to connect to a SQL database.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- An installation of GX set up to work with SQL
- Source data stored in a SQL database
- A SQL-based Datasource

</Prerequisites>

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Retrieve a SQL Datasource

Run the following Python code to retrieve the Datasource:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py datasource
```

### Add a table to the Datasource as a Data Asset

You create a Data Asset to identify the table to connect to. This is as simple as providing the `add_table_asset(...)` method a `name` by which we will reference the Data Asset in the future and a `table_name` to specify the table we wish the Data Asset to connect to.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py create_datasource
```

### Add additional tables (Optional)

To connect to additional tables in the same SQL Database, repeat the previous steps to add them as table Data Assets.

### Related documentation

- [How to organize Batches in a SQL based Data Asset](/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset)
- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)
- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)

</TabItem>
<TabItem value="query">

</TabItem>
<TabItem value="batches">

</TabItem>
</Tabs>

## Related documentation

- [How to set up GX to work with SQL databases](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- [How to connect to SQL data](/docs/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sql_data)
- [How to connect to PostgreSQL data](/docs/guides/connecting_to_your_data/fluent/database/how_to_connect_to_postgresql_data)
- [How to connect to SQLite data](/docs/guides/connecting_to_your_data/fluent/database/how_to_connect_to_sqlite_data)

