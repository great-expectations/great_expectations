---
title: How to instantiate a specific Filesystem Data Context
tag: [how-to, setup]
keywords: [Great Expectations, Data Context, Filesystem]

---

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import Great Expectations -->
import GxImport from '/docs/components/setup/python_environment/_gx_import.md'

<!--- ### 3. Verify the content of the Data Context -->
import DataContextVerifyContents from '/docs/components/setup/data_context/_data_context_verify_contents.md'

A <TechnicalTag tag="data_context" text="Data Context" /> contains the configurations for <TechnicalTag tag="expectation" text="Expectations" />, <TechnicalTag tag="store" text="Metadata Stores" />, <TechnicalTag tag="data_docs" text="Data Docs" />, <TechnicalTag tag="checkpoint" text="Checkpoints" />, and all things related to working with Great Expectations.  

If you are using GX for multiple projects you may wish to utilize a different Data Context for each one.  This guide will demonstrate how to instantiate a specific Filesystem Data Context so that you can switch between sets of previously defined GX configurations.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- A previously initialized Filesystem Data Context. See [How to initialize a Filesystem Data Context in Python](/docs/guides/setup/configuring_data_contexts/initializing_data_contexts/how_to_initialize_a_filesystem_data_context_in_python).

</Prerequisites>

## Steps

### 1. Import Great Expectations

<GxImport />

### 2. Specify a folder containing a previously initialized Filesystem Data Context

Each Filesystem Data Context has a root folder in which it was initialized.  This root folder will be used to indicate which specific Filesystem Data Context should be instantiated.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py path_to_project_root"
```

### 2. Run GX's `get_context(...)` method

We provide our Filesystem Data Context's root folder path to the GX library's `get_context(...)` method as the `project_root_dir` parameter.  Because we are providing a path to an existing Data Context, the `get_context(...)` method will instantiate and return the Data Context at that location.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_instantiate_a_specific_filesystem_data_context.py get_filesystem_data_context"
```

:::info What if the folder does not contain a Data Context?
If the `project_root_dir` provided to the `get_context(...)` method points to a folder that does not already have a Data Context present, the `get_context(...)` method will initialize a new Filesystem Data Context at that location.

The `get_context(...)` method will then instantiate and return the newly initialized Data Context.
:::


### 3. Verify the content of the returned Data Context

<DataContextVerifyContents />

## Next steps

For guidance on further customizing your Data Context's configurations for Metadata Stores and Data Docs, please see:
- [How to configure an Expectation Store on a filesystem](/docs/guides/setup/configuring_metadata_stores/how_to_configure_an_expectation_store_on_a_filesystem)
- [How to configure a Validation Result Store on a filesystem](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_validation_result_store_on_a_filesystem)
- [How to configure and use a Metric Store](/docs/guides/setup/configuring_metadata_stores/how_to_configure_a_metricsstore)
- [How to host and share Data Docs on a filesystem](/docs/guides/setup/configuring_data_docs/how_to_host_and_share_data_docs_on_a_filesystem)

If you are content with the default configuration of your Data Context, you can move on to connecting GX to your source data:
- [How to configure a Pandas Datasource](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_pandas_datasource)
- [How to configure a Spark Datasource](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_spark_datasource)
- [How to configure a SQL Datasource](/docs/0.15.50/guides/connecting_to_your_data/datasource_configuration/how_to_configure_a_sql_datasource)

## Additional information

### Related guides

To initialize and instantiate a temporary Data Context, see: [How to instantiate an Ephemeral Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/how_to_explicitly_instantiate_an_ephemeral_data_context).

<!-- TODO
To instantiate an existing Data Context, reference:
- How to quickly instantiate a Data Context
- How to instantiate a specific Filesystem Data Context

-->

<!-- TODO
### Code examples

To see the full source code used for the examples in this guide, please reference the following scripts in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)
-->