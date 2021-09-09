---
title: How to create a Batch of data from an in-memory Spark or Pandas dataframe
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

      Load an on-disk Data Context via:

      ```python
      import pyspark

      import great_expectations as ge
      from great_expectations import DataContext
      from great_expectations.core import ExpectationSuite
      from great_expectations.core.batch import RuntimeBatchRequest
      from great_expectations.core.util import get_or_create_spark_application
      from great_expectations.validator.validator import Validator

      context = ge.get_context()
      ```

      Create an in-code Data Context using these instructions: [How to instantiate a Data Context without a yml file](../../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md)

  2. **Obtain an Expectation Suite**

      ```python
      suite: ExpectationSuite = context.get_expectation_suite("insert_your_expectation_suite_name_here")
      ```

      Alternatively, you can simply use the name of the Expectation Suite.

      ```python
      suite_name: str = "insert_your_expectation_suite_name_here"
      ```

      If you have not already created an Expectation Suite, you can do so now.

      ```python
      suite: ExpectationSuite = context.create_expectation_suite("insert_your_expectation_suite_name_here")
      ```

  3. **Construct a Runtime Batch Request**

      We will create a ``RuntimeBatchRequest`` and pass it our Spark DataFrame or path via the ``runtime_parameters`` argument, under either the ``batch_data`` or ``path`` key. The ``batch_identifiers`` argument is required and must be a non-empty dictionary containing all of the Batch Identifiers specified in your Runtime Data Connector configuration.

      If you are providing a filesystem path instead of a materialized DataFrame, you may use either an absolute or relative path (with respect to the current working directory). Under the hood, Great Expectations will instantiate a Spark Dataframe using the appropriate ``spark.read.*`` method, which will be inferred from the file extension. If your file names do not have extensions, you can specify the appropriate reader method explicitly via the ``batch_spec_passthrough`` argument. Any Spark reader options (i.e. ``delimiter`` or ``header``) that are required to properly read your data can also be specified with the ``batch_spec_passthrough`` argument, in a dictionary nested under a key named ``reader_options``.

      Example ``great_expectations.yml`` Datasource configuration:

      ```yaml
      my_spark_datasource:
        execution_engine:
          module_name: great_expectations.execution_engine
          class_name: SparkDFExecutionEngine
        module_name: great_expectations.datasource
        class_name: Datasource
        data_connectors:
          my_runtime_data_connector:
            class_name: RuntimeDataConnector
            batch_identifiers:
              - some_key_maybe_pipeline_stage
              - some_other_key_maybe_airflow_run_id
      ```

      Example Runtime Batch Request using an in-memory DataFrame:

      ```python
      spark_application: pyspark.sql.session.SparkSession = get_or_create_spark_application()
      df: pyspark.sql.dataframe.DataFrame = spark_application.read.csv("some_path.csv")
      runtime_batch_request = RuntimeBatchRequest(
          datasource_name="my_spark_datasource",
          data_connector_name="my_runtime_data_connector",
          data_asset_name="insert_your_data_asset_name_here",
          runtime_parameters={
            "batch_data": df
          },
          batch_identifiers={
              "some_key_maybe_pipeline_stage": "ingestion step 1",
              "some_other_key_maybe_airflow_run_id": "run 18"
          }
      )
      ```

      Example Runtime Batch Request using a path:

      ```python
      path = "some_csv_file_with_no_file_extension"
      runtime_batch_request = RuntimeBatchRequest(
          datasource_name="my_spark_datasource",
          data_connector_name="my_runtime_data_connector",
          data_asset_name="insert_your_data_asset_name_here",
          runtime_parameters={
              "path": path
          },
          batch_identifiers={
              "some_key_maybe_pipeline_stage": "ingestion step 1",
              "some_other_key_maybe_airflow_run_id": "run 18"
          },
          batch_spec_passthrough={
              "reader_method": "csv",
              "reader_options": {
                  "delimiter": ",",
                  "header": True
              }
          }
      )
      ```

      :::note Best Practice
        Though not strictly required, we recommend that you make every Data Asset Name **unique**. Choosing a unique Data Asset Name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an Execution Engine.
      :::

  4. **Construct a Validator**

      ```python
      my_validator: Validator = context.get_validator(
          batch_request=runtime_batch_request,
          expectation_suite=suite,  # OR
          # expectation_suite_name=suite_name
      )
      ```

      Alternatively, you may skip step 2 and pass the same Runtime Batch Request instantiation arguments, along with the Expectation Suite (or name), directly to to the ``get_validator`` method.

      ```python
      my_validator: Validator = context.get_validator(
         datasource_name="my_spark_datasource",
         data_connector_name="my_runtime_data_connector",
         data_asset_name="insert_your_data_asset_name_here",
         runtime_parameters={
             "path": path
         },
         batch_identifiers={
             "some_key_maybe_pipeline_stage": "ingestion step 1",
             "some_other_key_maybe_airflow_run_id": "run 18"
         },
         batch_spec_passthrough={
             "reader_method": "csv",
             "reader_options": {
                 "delimiter": ",",
                 "header": True
             }
         },
         expectation_suite=suite,  # OR
         # expectation_suite_name=suite_name
       )
      ```

  5. **Check your data**

      You can check that the first few lines of your Batch are what you expect by running:

      ```python
      my_validator.head()
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

      Load an on-disk Data Context via:

      ```python
      import pandas as pd

      import great_expectations as ge
      from great_expectations import DataContext
      from great_expectations.core import ExpectationSuite
      from great_expectations.core.batch import RuntimeBatchRequest
      from great_expectations.validator.validator import Validator

      context: DataContext = ge.get_context()
      ```

      Create an in-code Data Context using these instructions: [How to instantiate a Data Context without a yml file](../../guides/setup/configuring_data_contexts/how_to_instantiate_a_data_context_without_a_yml_file.md)

  2. **Obtain an Expectation Suite**

      ```python
      suite: ExpectationSuite = context.get_expectation_suite("insert_your_expectation_suite_name_here")
      ```

      Alternatively, you can simply use the name of the Expectation Suite.

      ```python
      suite_name: str = "insert_your_expectation_suite_name_here"
      ```

      If you have not already created an Expectation Suite, you can do so now.

      ```python
      suite: ExpectationSuite = context.create_expectation_suite("insert_your_expectation_suite_name_here")
      ```

  3. **Construct a Runtime Batch Request**

      We will create a ``RuntimeBatchRequest`` and pass it our DataFrame or path via the ``runtime_parameters`` argument, under either the ``batch_data`` or ``path`` key. The ``batch_identifiers`` argument is required and must be a non-empty dictionary containing all of the Batch Identifiers specified in your Runtime Data Connector configuration.

      If you are providing a filesystem path instead of a materialized DataFrame, you may use either an absolute or relative path (with respect to the current working directory). Under the hood, Great Expectations will instantiate a Pandas Dataframe using the appropriate ``pandas.read_*`` method, which will be inferred from the file extension. If your file names do not have extensions, you can specify the appropriate reader method explicitly via the ``batch_spec_passthrough`` argument. Any Pandas reader options (i.e. ``sep`` or ``header``) that are required to properly read your data can also be specified with the ``batch_spec_passthrough`` argument, in a dictionary nested under a key named ``reader_options``.

      Example ``great_expectations.yml`` Datsource configuration:

      ```yaml
      my_pandas_datasource:
        execution_engine:
          module_name: great_expectations.execution_engine
          class_name: PandasExecutionEngine
        module_name: great_expectations.datasource
        class_name: Datasource
        data_connectors:
          my_runtime_data_connector:
            class_name: RuntimeDataConnector
            batch_identifiers:
              - some_key_maybe_pipeline_stage
                - some_other_key_maybe_airflow_run_id
      ```

      Example Runtime Batch Request using an in-memory DataFrame:

      ```python
      df: pd.DataFrame = pd.read_csv("some_path.csv")
      runtime_batch_request = RuntimeBatchRequest(
        datasource_name="my_pandas_datasource",
        data_connector_name="my_runtime_data_connector",
        data_asset_name="insert_your_data_asset_name_here",
        runtime_parameters={
          "batch_data": df
        },
        batch_identifiers={
            "some_key_maybe_pipeline_stage": "ingestion step 1",
            "some_other_key_maybe_airflow_run_id": "run 18"
        }
      )
      ```

      Example Runtime Batch Request using a path:

      ```python
      path = "some_csv_file_with_no_file_extension"
      runtime_batch_request = RuntimeBatchRequest(
          datasource_name="my_pandas_datasource",
          data_connector_name="my_runtime_data_connector",
          data_asset_name="insert_your_data_asset_name_here",
          runtime_parameters={
              "path": path
          },
          batch_identifiers={
              "some_key_maybe_pipeline_stage": "ingestion step 1",
              "some_other_key_maybe_airflow_run_id": "run 18"
          },
          batch_spec_passthrough={
              "reader_method": "read_csv",
              "reader_options": {
                  "sep": ",",
                  "header": 0
              }
          }
        )
      ```

      :::note Best Practice

        Though not strictly required, we recommend that you make every Data Asset Name **unique**. Choosing a unique Data Asset Name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an Execution Engine.
      :::

  4. **Construct a Validator**

      ```python
      my_validator: Validator = context.get_validator(
          batch_request=runtime_batch_request,
          expectation_suite=suite,  # OR
          # expectation_suite_name=suite_name
        )
      ```

      Alternatively, you may skip step 2 and pass the same Runtime Batch Request instantiation arguments, along with the Expectation Suite (or name), directly to to the ``get_validator`` method.

      ```python
      my_validator: Validator = context.get_validator(
          datasource_name="my_pandas_datasource",
          data_connector_name="my_runtime_data_connector",
          data_asset_name="insert_your_data_asset_name_here",
          runtime_parameters={
              "path": path
          },
          batch_identifiers={
              "some_key_maybe_pipeline_stage": "ingestion step 1",
              "some_other_key_maybe_airflow_run_id": "run 18"
          },
          batch_spec_passthrough={
              "reader_method": "read_csv",
              "reader_options": {
                  "sep": ",",
                  "header": 0
              }
          },
          expectation_suite=suite,  # OR
          # expectation_suite_name=suite_name
        )
      ```

  5. **Check your data**

      You can check that the first few lines of your Batch are what you expect by running:

      ```python
      my_validator.head()
      ```

    Now that you have a Validator, you can use it to create Expectations or validate the data.


  </TabItem>
</Tabs>

