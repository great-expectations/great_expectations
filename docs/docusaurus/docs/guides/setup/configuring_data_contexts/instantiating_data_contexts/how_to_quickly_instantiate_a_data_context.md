---
title: How to quickly instantiate a Data Context
tag: [how-to, setup]
keywords: [Great Expectations, Data Context, Filesystem]

---

import TechnicalTag from '/docs/term_tags/_tag.mdx';
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import Great Expectations -->
import GxImport from '/docs/components/setup/python_environment/_gx_import.md'

<!--- ### 3. Verify the content of the Data Context -->
import DataContextVerifyContents from '/docs/components/setup/data_context/_data_context_verify_contents.md'

import AdmonitionConvertToFileContext from '/docs/components/setup/data_context/_admonition_convert_to_file_context.md'

A <TechnicalTag tag="data_context" text="Data Context" /> contains the configurations for <TechnicalTag tag="expectation" text="Expectations" />, <TechnicalTag tag="store" text="Metadata Stores" />, <TechnicalTag tag="data_docs" text="Data Docs" />, <TechnicalTag tag="checkpoint" text="Checkpoints" />, and all things related to working with Great Expectations.  This guide will demonstrate how to instantiate an existing Filesystem Data Context so that you can continue working with previously defined GX configurations.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {false} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>


</Prerequisites>

## Steps

### 1. Import Great Expectations

<GxImport />

### 2. Run GX's `get_context(...)` method

To quickly acquire a Data Context, we can use the `get_context(...)` method without any defined parameters.

```python title="Python code"
context = gx.get_context()
```

This functions as a convenience method for initializing, instantiating, and returning a Data Context.  In the absence of parameters defining its behaviour, calling `get_context()` will return either a Cloud Data Context, a Filesystem Data Context, or an Ephemeral Data Context depending on what type of Data Context has previously been initialized with your GX install.

If you have GX Cloud configured on your system, `get_context()` will instantiate and return a Cloud Data Context. Otherwise, `get_context()` will attempt to instantiate and return the last accessed Filesystem Data Context. Finally, if a previously initialized Filesystem Data Context cannot be found, `get_context()` will initialize, instantiate, and return a temporary in-memory Ephemeral Data Context.


:::info Saving the contents of an Ephemeral Data Context for future use

<AdmonitionConvertToFileContext />

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

- To initialize and instantiate a temporary Data Context, see [Instantiate an Ephemeral Data Context](/docs/guides/setup/configuring_data_contexts/instantiating_data_contexts/instantiate_data_context)

<!-- TODO
To instantiate an existing Data Context, reference:
- How to quickly instantiate a Data Context
- How to instantiate a specific Filesystem Data Context

To initialize and instantiate a temporary Data Context, see:
- How to instantiate an in-memory Ephemeral Data Context
-->

<!-- TODO
### Code examples

To see the full source code used for the examples in this guide, please reference the following scripts in our GitHub repository:
- [script_name.py](https://path/to/the/script/on/github.com)
-->