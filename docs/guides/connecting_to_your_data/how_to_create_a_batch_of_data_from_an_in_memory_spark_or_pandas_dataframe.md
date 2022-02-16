---
title: How to create a Batch of data from an in-memory Spark or Pandas dataframe or path
---
import Prerequisites from '../connecting_to_your_data/components/prerequisites.jsx'
import Tabs from '@theme/Tabs'
import TabItem from '@theme/TabItem'

This guide will help you load the following as Batches for use in creating Expectations:
1. **Pandas DataFrames**
2. **Spark DataFrames**


What used to be called a “Batch” in the old API was replaced with [Validator](../../reference/validation.md). A Validator knows how to validate a particular Batch of data on a particular [Execution Engine](../../reference/execution_engine.md) against a particular [Expectation Suite](../../reference/expectations/expectations.md). In interactive mode, the Validator can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis.

You can read more about the core classes that make Great Expectations run in our [Core Concepts reference guide](../../reference/core_concepts.md).


<Tabs
     groupId='spark-or-pandas'
     defaultValue='spark'
     values={[
     {label: 'Spark DataFrame', value:'spark'},
     {label: 'Pandas DataFrame', value:'pandas'},
     ]}>
     <TabItem value='spark'>

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../tutorials/getting_started/intro.md)
- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- Configured a [Spark Datasource](../../guides/connecting_to_your_data/filesystem/spark.md)
- Identified an in-memory Spark DataFrame that you would like to use as the data to validate **OR**
- Identified a filesystem or S3 path to a file that contains the data you would like to use to validate.
  
</Prerequisites>

1. **Load or create a Data Context**

     The ``context`` referenced below can be loaded from disk or configured in code.

     First, import these necessary packages and modules.

     ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L1-L9
     ```

     Load an on-disk Data Context (ie. from a `great_expectations.yml` configuration) via the `get_context()` command:

     ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L11
     ```

    If you are working in an environment without easy access to a local filesystem (e.g. AWS Spark EMR, Databricks, etc.), load an in-code Data Context using these instructions: [How to instantiate a Data Context without a yml file](../../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md)

2. **Obtain an Expectation Suite**
   
    If you have not already created an Expectation Suite, you can do so now.

     ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L20-L22
     ```

     The Expectation Suite can then be loaded into memory by using `get_expectation_suite()`.

     ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L23-L25
     ```

3. **Construct a RuntimeBatchRequest**

    We will create a ``RuntimeBatchRequest`` and pass it our Spark DataFrame or path via the ``runtime_parameters`` argument, under either the ``batch_data`` or ``path`` key. The ``batch_identifiers`` argument is required and must be a non-empty dictionary containing all of the Batch Identifiers specified in your Runtime Data Connector configuration.
    
    If you are providing a filesystem path instead of a materialized DataFrame, you may use either an absolute or relative path (with respect to the current working directory). Under the hood, Great Expectations will instantiate a Spark Dataframe using the appropriate ``spark.read.*`` method, which will be inferred from the file extension. If your file names do not have extensions, you can specify the appropriate reader method explicitly via the ``batch_spec_passthrough`` argument. Any Spark reader options (i.e. ``delimiter`` or ``header``) that are required to properly read your data can also be specified with the ``batch_spec_passthrough`` argument, in a dictionary nested under a key named ``reader_options``.

    Here is an example Datasource configuration in YAML.
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L27-L40
    ```
   
    Save the configuration into your DataContext by using the `add_datasource()` function.
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L42
    ```
     
    If you have a file in the following location:
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L45
    ```

    Then the file can be read as a Spark Dataframe using:
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L50
    ```
   
    Here is a Runtime Batch Request using an in-memory DataFrame:
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L48-L57
    ```

    Here is a Runtime Batch Request using a path:
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L63-L72
    ```

    :::note Best Practice
    Though not strictly required, we recommend that you make every Data Asset Name **unique**. Choosing a unique Data Asset Name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an Execution Engine.
    :::

4. **Construct a Validator**

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L78-L82
    ```

    Alternatively, you may skip step 2 and pass the same Runtime Batch Request instantiation arguments, along with the Expectation Suite (or name), directly to to the ``get_validator`` method.

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L86-L101
    ```

5. **Check your data**

    You can check that the first few lines of your Batch are what you expect by running:

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py#L102
    ```
   
    Now that you have a Validator, you can use it to create Expectations or validate the data.


</TabItem>
<TabItem value='pandas'>

<Prerequisites>

- [Set up a working deployment of Great Expectations](../../tutorials/getting_started/intro.md)
- [Configured and loaded a Data Context](../../tutorials/getting_started/initialize_a_data_context.md)
- Configured a [Pandas/filesystem Datasource](../../guides/connecting_to_your_data/filesystem/pandas.md)
- Identified a Pandas DataFrame that you would like to use as the data to validate.
  
</Prerequisites>

1. **Load or create a Data Context**

   The ``context`` referenced below can be loaded from disk or configured in code.
   
   First, import these necessary packages and modules.
   ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L1-L8
   ```

   Load an on-disk Data Context (ie. from a `great_expectations.yml` configuration) via the `get_context()` command:
   
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L10
    ```
   
    If you are working in an environment without easy access to a local filesystem (e.g. AWS Spark EMR, Databricks, etc.), load an in-code Data Context using these instructions: [How to instantiate a Data Context without a yml file](../../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md)

2. **Obtain an Expectation Suite**
    If you have not already created an Expectation Suite, you can do so now.

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L13-L15
    ```

     The Expectation Suite can then be loaded into memory by using `get_expectation_suite()`.

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L16-L18
    ```

3. **Construct a Runtime Batch Request**

   We will create a ``RuntimeBatchRequest`` and pass it our DataFrame or path via the ``runtime_parameters`` argument, under either the ``batch_data`` or ``path`` key. The ``batch_identifiers`` argument is required and must be a non-empty dictionary containing all of the Batch Identifiers specified in your Runtime Data Connector configuration. 
   
   If you are providing a filesystem path instead of a materialized DataFrame, you may use either an absolute or relative path (with respect to the current working directory). Under the hood, Great Expectations will instantiate a Pandas Dataframe using the appropriate ``pandas.read_*`` method, which will be inferred from the file extension. If your file names do not have extensions, you can specify the appropriate reader method explicitly via the ``batch_spec_passthrough`` argument. Any Pandas reader options (i.e. ``sep`` or ``header``) that are required to properly read your data can also be specified with the ``batch_spec_passthrough`` argument, in a dictionary nested under a key named ``reader_options``.
   
   Here is an example Datasource configuration in YAML.
   ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L20-L33
   ```
   
   Save the configuration into your DataContext by using the `add_datasource()` function.
   ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L35
   ```
   
   If you have a file in the following location:
   ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L38
   ```
   Then the file can be read as a Pandas Dataframe using
   ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L43
   ```

    Here is a Runtime Batch Request using an in-memory DataFrame:
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L44-L53
    ```

    Here is a Runtime Batch Request using a path:
    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L56-L69
    ```
   
   :::note Best Practice 
   Though not strictly required, we recommend that you make every Data Asset Name **unique**. Choosing a unique Data Asset Name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an Execution Engine.
   :::

4. **Construct a Validator**

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L72-L76
    ```
      Alternatively, you may skip step 2 and pass the same Runtime Batch Request instantiation arguments, along with the Expectation Suite (or name), directly to to the ``get_validator`` method.

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L80-L95
    ```

5. **Check your data**

    You can check that the first few lines of your Batch are what you expect by running:

    ```python file=../../../tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py#L96
    ```

    Now that you have a Validator, you can use it to create Expectations or validate the data.


</TabItem>
</Tabs>


## Additional Notes

To view the full scripts used in this page, see them on GitHub:

- [in_memory_spark_dataframe_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_spark_dataframe.py)
- [in_memory_pandas_dataframe_example.py](https://github.com/great-expectations/great_expectations/blob/develop/tests/integration/docusaurus/connecting_to_your_data/how_to_create_a_batch_of_data_from_an_in_memory_pandas_dataframe.py)
