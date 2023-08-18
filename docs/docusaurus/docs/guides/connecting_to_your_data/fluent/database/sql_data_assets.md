---
sidebar_label: "Manage SQL Data Assets"
title: "Manage SQL Data Assets"
id: sql_data_assets
description: Connect Great Expectations to SQL Data Assets.
toc_min_heading_level: 2
toc_max_heading_level: 2
keywords: [Great Expectations, Data Asset, Batch Request, fluent configuration method, SQL]
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'
import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import ConnectingToSqlDatasourcesFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_sql_datasources_fluently.md'
import AfterRequestDataFromADataAsset from '/docs/components/connect_to_data/next_steps/_after_request_data_from_a_data_asset.md'
import AfterCreateAndConfigureDataAsset from '/docs/components/connect_to_data/next_steps/_after_create_and_configure_data_asset.md'
import TechnicalTag from '/docs/term_tags/_tag.mdx';
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';


A Data Asset is a collection of records within a Data Source that define how Great Expectations (GX) organizes data into Batches. Use the information provided here to connect GX to SQL tables and data returned by SQL database queries and learn how to organize Batches in a SQL Data Asset.

 Great Expectations (GX) uses SQLAlchemy to connect to SQL source data, and most of the SQL dialects supported by SQLAlchemy are also supported by GX. For more information about the SQL dialects supported by SQLAlchemy, see [Dialects](https://docs.sqlalchemy.org/en/20/dialects/index.html).

:::caution Datasources defined with the block-config method

If you're using a Data Source created with the block-config method, see [How to configure a SQL Data Source with the block-config method](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource).

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

Connect GX to a SQL table to access source data.

The following code examples use a previously defined Data Source named `"my_datasource"` to connect to a SQL database.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- An installation of GX set up to work with SQL
- Source data stored in a SQL database
- A SQL-based Data Source

</Prerequisites>

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Retrieve a SQL Data Source

Run the following Python code to retrieve the Data Source:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_a_sql_table.py datasource
```

### Add a table to the Data Source as a Data Asset

You create a Data Asset to identify the table to connect to. 

Run the following Python code to define the `name` and `table_name` variables:

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

## Connect to SQL data using a query

Connect GX to the data returned by a query in a SQL database.

The following code examples use a previously defined Data Source named `"my_datasource"` to connect to a SQL database.

### Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {false} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- An installation of GX set up to work with SQL. See [How to set up GX to work with SQL databases](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases).
- Source data stored in a SQL database.

</Prerequisites> 

### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Retrieve a SQL Data Source

Run the following Python code to retrieve the Data Source:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data_using_a_query.py datasource"
```

### Add a query to the Data Source as a Data Asset

Run the following Python code to define a Data Asset and the `name` and `query` variables:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_sql_data_using_a_query.py add_query_asset"
```

### Add additional queries (Optional)

To connect to the contents of additional queries in the same SQL Database, repeat the previous steps to add them as query Data Assets.

### Related documentation

- [How to organize Batches in a SQL based Data Asset](/docs/guides/connecting_to_your_data/fluent/data_assets/how_to_organize_batches_in_a_sql_based_data_asset)
- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)
- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)

</TabItem>
<TabItem value="batches">

## Organize Batches

Organize Batches in a SQL-based Data Asset. This includes using Splitters to divide the data in a table or query based on the contents of a provided field and adding Batch Sorters to a Data Asset to specify the order in which Batches are returned.

The following code examples use a previously defined Data Source named `"my_datasource"` to connect to a SQL database.

### Prerequisites

<Prerequisites>

- A working installation of Great Expectations
- A Data Asset in a SQL-based Data Source

</Prerequisites>


### Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### Retrieve a SQL Data Source and Data Asset

Run the following Python code to retrieve the Data Source:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py my_datasource"
```

### Add a Splitter to the Data Asset

Run the following Python code to split the TableAsset into Batches:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py add_splitter_year_and_month"
```

### Add Batch Sorters to the Data Asset (Optional) 

Adding Batch Sorters to your Data Asset lets you explicitly state the order in which your Batches are returned when you request data from the Data Asset. To add Batch Sorters, pass a list of sorters to the `add_sorters(...)` method of your Data Asset.

Run the following Python code to split the `"pickup_datetime"` column on `"year"` and `"month"`, so your list of sorters can have up to two elements. The code also adds an ascending sorter based on the contents of the splitter group `"year"` and a descending sorter based on the contents of the splitter group `"month"`:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py add_sorters"
```

### Use a Batch Request to verify Data Asset functionality

Run the following Python code to verify that your Data Asset returns the necessary files as Batches:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py my_batch_list"
```

A Batch List can contain a lot of metadata. To verify which files were included in the returned Batches, run the following Python code to review the `batch_spec` for each returned Batch:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py print_batch_spec"
```

### Related documentation

- [How to request data from a Data Asset](/docs/guides/connecting_to_your_data/fluent/batch_requests/how_to_request_data_from_a_data_asset)
- [Use a Data Asset to create Expectations while interactively evaluating a set of data](/docs/guides/expectations/how_to_create_and_edit_expectations_with_instant_feedback_from_a_sample_batch_of_data)
- [Use the Onboarding Data Assistant to evaluate one or more Batches of data and create Expectations](/docs/guides/expectations/data_assistants/how_to_create_an_expectation_suite_with_the_onboarding_data_assistant)

</TabItem>
</Tabs>

## Related documentation

- [How to set up GX to work with SQL databases](/docs/guides/setup/optional_dependencies/sql_databases/how_to_setup_gx_to_work_with_sql_databases)
- [How to connect to SQL data](/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data)
- [How to connect to PostgreSQL data](/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data)
- [How to connect to SQLite data](/docs/guides/connecting_to_your_data/fluent/database/connect_sql_source_data)

