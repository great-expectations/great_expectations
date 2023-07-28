---
title: Connect to in-memory source data using Spark
tag: [how-to, connect to data]
description: Connect Great Expectations to in-memory source data using Spark.
keywords: [Great Expectations, Spark, Filesystem]
---

<!-- Import statements start here. -->
import Prerequisites from '/docs/components/_prerequisites.jsx'

<!-- ### 1. Import GX and instantiate a Data Context -->
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'

<!-- ## Next steps -->
import AfterCreateInMemoryDataAsset from '/docs/components/connect_to_data/next_steps/_after_create_in_memory_data_asset.md'

Use the information provided here to learn how to connect to in-memory source data using Spark. 

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to data that can be read into a Spark

</Prerequisites> 

## Import the Great Expectations module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

## Create a Datasource

Run the following Python code to create a Spark Datasource:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_Spark.py datasource"
```

## Read your source data into a Spark DataFrame

In the following example, a parquet file is read into a Spark DataFrame that will be used in subsequent code examples.

Run the following Python code to create the Spark DataFrame:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_Spark.py dataframe"
```

## Add a Data Asset to the Datasource

The following information is required when you create a Spark DataFrame Data Asset:

- `name`: The Datasource name.

- `dataframe`: The Spark DataFrame containing the source data.

The DataFrame you created previously is the value you'll enter for `dataframe` parameter.  

1. Run the following Python code to define the `name` parameter and store it as a Python variable:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_Spark.py name"
    ```

2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_Spark.py data_asset"
    ```

    For `dataframe` Data Assets, the `dataframe` is always specified as the argument of one API method. For example:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_Spark.py build_batch_request_with_dataframe"
    ```

## Next steps

<AfterCreateInMemoryDataAsset />

## Related documentation

For more information on Spark read methods, see [the Spark Input/Output documentation](https://Spark.pydata.org/docs/reference/io.html).