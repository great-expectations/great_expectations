---
sidebar_label: "Connect to in-memory source data"
title: "Connect to in-memory source data"
id: connect_in_memory_data
description: Connect to source data stored in-memory.
toc_min_heading_level: 2
toc_max_heading_level: 2
---

import Prerequisites from '/docs/components/_prerequisites.jsx'
import ImportGxAndInstantiateADataContext from '/docs/components/setup/data_context/_import_gx_and_instantiate_a_data_context.md'
import AfterCreateInMemoryDataAsset from '/docs/components/connect_to_data/next_steps/_after_create_in_memory_data_asset.md'
import TabItem from '@theme/TabItem';
import Tabs from '@theme/Tabs';

Use the information provided here to connect to an in-memory pandas or Spark DataFrame. Great Expectations (GX) uses the term source data when referring to data in its original format, and the term source data system when referring to the storage location for source data.

<Tabs
  groupId="connect-in-memory-source-data"
  defaultValue='pandas'
  values={[
  {label: 'pandas', value:'pandas'},
  {label: 'Spark', value:'spark'},
  ]}>
<TabItem value="pandas">

pandas can read many types of data into its DataFrame class, but the following examples use data originating in a parquet file.

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

- `dataframe`: The Pandas DataFrame containing the source data.

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

</TabItem>
<TabItem value="spark">

Connect to in-memory source data using Spark. 

## Prerequisites

<Prerequisites requirePython = {false} requireInstallation = {true} requireDataContext = {true} requireSourceData = {null} requireDatasource = {false} requireExpectationSuite = {false}>

- Access to data that can be read into a Spark
- An active Spark Context

</Prerequisites> 

## Import the Great Expectations module and instantiate a Data Context

<ImportGxAndInstantiateADataContext />

## Create a Datasource

Run the following Python code to create a Spark Datasource:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py datasource"
```

## Read your source data into a Spark DataFrame

In the following example, a simple Spark DataFrame is created that will be used in subsequent code examples.

Run the following Python code to create the Spark DataFrame:

```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py dataframe"
```

## Add a Data Asset to the Datasource

The following information is required when you create a Spark DataFrame Data Asset:

- `name`: The Datasource name.

- `dataframe`: The Spark DataFrame containing the source data.

The DataFrame you created previously is the value you'll enter for `dataframe` parameter.  

1. Run the following Python code to define the `name` parameter and store it as a Python variable:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py name"
    ```

2. Run the following Python code to create the Data Asset:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py data_asset"
    ```

    For `dataframe` Data Assets, the `dataframe` is always specified as the argument of one API method. For example:

    ```python name="tests/integration/docusaurus/connecting_to_your_data/fluent_datasources/how_to_connect_to_in_memory_data_using_spark.py build_batch_request_with_dataframe"
    ```

## Next steps

<AfterCreateInMemoryDataAsset />

## Related documentation

For more information on Spark read methods, see [the Spark Input/Output documentation](https://Spark.pydata.org/docs/reference/io.html).

</TabItem>
</Tabs>