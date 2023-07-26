---
title: Connect to in-memory Source Data using Pandas
tag: [how-to, connect to data]
description: Connect Great Expectations to a Pandas in-memory DataFrame.
keywords: [Great Expectations, Pandas, Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ## Next steps -->
import AfterCreateInMemoryDataAsset from '/docs/components/connect_to_data/next_steps/_after_create_in_memory_data_asset.md'

In this guide we will demonstrate how to connect to an in-memory Pandas DataFrame.  Pandas can read many types of data into its DataFrame class, but in our example we will use data originating in a parquet file.

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to data that can be read into a Pandas DataFrame

</Prerequisites> 

## Import the Great Expectations module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

## Create a Data Source

Run the following Python code to create a Pandas Data Source:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py datasource"
```

## Read your source data into a Pandas DataFrame

In the following example, a parquet file is read into a Pandas DataFrame that will be used in subsequent code examples.

Run the following Python code to create the Pandas DataFrame:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py dataframe"
```

## Add a Data Asset to the Data Source

The following information is required when you create a Pandas DataFrame Data Asset:

- `name`: The Data Source name.

- `dataframe`: The Pandas DataFrame containing the Source Data.

The DataFrame you created previously is the value you'll enter for `dataframe` parameter.  

1. Run the following Python code to define the `name` parameter and store it as a Python variable:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py name"
    ```

2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py data_asset"
    ```

    For `dataframe` Data Assets, the `dataframe` is always specified as the argument of one API method. For example:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_pandas.py build_batch_request_with_dataframe"
    ```

## Next steps

<AfterCreateInMemoryDataAsset />

## Related documentation

For more information on Pandas read methods, see [the Pandas Input/Output documentation](https://pandas.pydata.org/docs/reference/io.html).

<!-- TODO: Enable this and update links after the conceptual guides are revised
### Related reading

For more information on the concepts and reasoning employed by this guide, please reference the following informational guides:

- [What does a Data Source do behind the scenes?]
- [What are use the use cases for single vs multiple Batch Data Assets?]
-->