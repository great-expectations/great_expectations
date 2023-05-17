---
title: How to organize Batches in a SQL-based Data Asset
tag: [how-to, connect to data]
description: A technical guide demonstrating how to split the data returned by a SQL Data Asset into multiple Batches and explicitly sort those Batches.
keywords: [Great Expectations, Data Asset, Batch Request, fluent configuration method, SQL]
---

import TechnicalTag from '/docs/term_tags/_tag.mdx';

import AfterRequestDataFromADataAsset from '/docs/components/connect_to_data/next_steps/_after_request_data_from_a_data_asset.md'

<!-- ## Introduction -->

<!-- ## Prerequisites -->
import Prerequisites from '/docs/components/_prerequisites.jsx'
import SetupAndInstallForSqlData from '/docs/components/setup/link_lists/_setup_and_install_for_sql_data.md'
import ConnectingToSqlDatasourcesFluently from '/docs/components/connect_to_data/link_lists/_connecting_to_sql_datasources_fluently.md'

<!-- ### Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ## Next steps -->
import AfterCreateAndConfigureDataAsset from '/docs/components/connect_to_data/next_steps/_after_create_and_configure_data_asset.md'

In this guide we will demonstrate the ways in which Batches can be organized in a SQL-based Data Asset.  We will discuss how to use Splitters to divide the data in a table or query based on the contents of a provided field.  We will also show how to add Batch Sorters to a Data Asset in order to specify the order in which Batches are returned.

## Prerequisites

<Prerequisites>

- A working installation of Great Expectations
- A Data Asset in a SQL-based Datasource

</Prerequisites>


<!-- TODO <details>
<summary>

### If you still need to set up and install GX...

</summary>

Please reference the appropriate one of these guides:

<SetupAndInstallForSqlData />

</details>

<details>
<summary>

### If you still need to connect a Datasource to a SQL database...

</summary>

Please reference the appropriate one of these guides:

<ConnectingToSqlDatasourcesFluently />

</details>
-->

:::caution Datasources defined with the block-config method

If you're using a Datasource that was created with the advanced block-config method, see [How to configure a SQL Datasource with the block-config method](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource).

:::

## Steps

### 1. Import GX and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

### 2. Retrieve a SQL Datasource and Data Asset

For this guide, we will use a previously defined SQL Datasource named `"my_datasource"` with a Table Data Asset called `"my_asset"` which points to a table with taxi data. 

To retrieve this Datasource, we will supply the `get_datasource(...)` method of our Data Context with the name of the Datasource we wish to retrieve:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py my_datasource"
```

### 3. Add a Splitter to the Data Asset

Our table has a datetime column called "`pickup_datetime`" which we will use to split our TableAsset into Batches.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py add_splitter_year_and_month"
```

### 4. (Optional) Add Batch Sorters to the Data Asset

We will now add Batch Sorters to our Data Asset.  This will allow us to explicitly state the order in which our Batches are returned when we request data from the Data Asset.  To do this, we will pass a list of sorters to the `add_sorters(...)` method of our Data Asset.

In this example we split `"pickup_datetime"` column on `"year"` and `"month"`, so our list of sorters can have up to two elements.  We will add an ascending sorter based on the contents of the splitter group `"year"` and a descending sorter based on the contents of the splitter group `"month"`:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py add_sorters"
```

### 5. Use a Batch Request to verify the Data Asset works as desired

To verify that our Data Asset will return the desired files as Batches, we will define a quick Batch Request that will include all the Batches available in the Data asset.  Then we will use that Batch Request to get a list of the returned Batches.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py my_batch_list"
```

Because a Batch List contains a lot of metadata, it will be easiest to verify which files were included in the returned Batches if we only look at the `batch_spec` of each returned Batch:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/organize_batches_in_sqlite_datasource.py print_batch_spec"
```

## Next steps

Now that you have further configured a file-based Data Asset, you may want to look into:

<AfterRequestDataFromADataAsset />