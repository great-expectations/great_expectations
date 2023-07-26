---
title: How to initialize a Filesystem Data Context in Python
tag: [how-to, setup]
keywords: [Great Expectations, Data Context, Filesystem]

---

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import Great Expectations -->
import GxImport from '/docs/components/setup/python_environment/_gx_import.md'

<!--- ### 2. Verify the content of the Data Context -->
import DataContextVerifyContents from '/docs/components/setup/data_context/_data_context_verify_contents.md'

A <TechnicalTag tag="data_context" text="Data Context" /> is required in almost all Python scripts utilizing GX, and when using the <TechnicalTag tag="cli" text="CLI" />.

Use the information provided here to use Python code to initialize, instantiate, and verify the contents of a Filesystem Data Context.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

</Prerequisites>

## Steps

### 1. Import Great Expectations

<GxImport />

### 2. Determine the folder to initialize the Data Context in

For purposes of this example, we will assume that we have an empty folder to initialize our Filesystem Data Context in:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_initialize_a_filesystem_data_context_in_python.py path_to_empty_folder"
```

### 3. Create a GX context

We will provide our empty folder's path to the GX library's `FileDataContext.create(...)` method as the `project_root_dir` parameter.  Because we are providing a path to an empty folder `FileDataContext.create(...)` will initialize a Filesystem Data Context at that location.

For convenience, the `FileDataContext.create(...)` method will then instantiate and return the newly initialized Data Context, which we can keep in a Python variable.

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_initialize_a_filesystem_data_context_in_python.py initialize_filesystem_data_context"
```

:::info What if the folder is not empty?
If the `project_root_dir` provided to the `FileDataContext.create(...)` method points to a folder that does not already have a Data Context present, the `FileDataContext.create(...)` method will initialize a Filesystem Data Context at that location even if other files and folders are present.  This allows you to easily initialize a Filesystem Data Context in a folder that contains your source data or other project related contents.

If a Data Context already exists at the provided `project_root_dir`, the `FileDataContext.create(...)` method will not re-initialize it.  Instead, `FileDataContext.create(...)` will simply instantiate and return the existing Data Context as is.
:::


### 4. Verify the content of the returned Data Context

<DataContextVerifyContents />

## Next steps

For guidance on further customizing your Data Context's configurations for <TechnicalTag tag="store" text="Metadata Stores" /> and <TechnicalTag tag="data_docs" text="Data Docs" />, please see:
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

To initialize and instantiate a temporary Data Context, see: [How to instantiate an Ephemeral Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context).
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