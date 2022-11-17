.. _how_to_guides__creating_batches__how_to_load_a_spark_dataframe_as_a_batch:

How to load a Spark DataFrame as a Batch
=========================================

This guide will help you load a Spark DataFrame as a Batch for use in creating Expectations.

.. content-tabs::

    .. tab-container:: tab0
        :title: Show Docs for V2 (Batch Kwargs) API

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Configured and loaded a DataContext <how_to_guides__configuring_data_contexts>`
            - Configured a :ref:`Spark Datasource <how_to_guides__configuring_datasources>`
            - Identified a Spark DataFrame that you would like to use as the data to validate.

        Steps
        -----

        0. Load or create a Data Context

            The ``context`` referenced below can be loaded from disk or configured in code.

            Load an on-disk Data Context via:

            .. code-block:: python

                import great_expectations as ge
                context = ge.get_context()

            Create an in-code Data Context using these instructions: :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>`


        1. Obtain an Expectation Suite

            .. code-block:: python

                suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

            Alternatively, if you have not already created a suite, you can do so now.

            .. code-block:: python

                suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")


        2. Construct batch_kwargs and get a Batch

            ``batch_kwargs`` describe the data you plan to validate. Here we are using a Datasource you have configured and are passing in a DataFrame under the ``"dataset"`` key.

            .. code-block:: python

                batch_kwargs = {
                    "datasource": "insert_your_datasource_name_here",
                    "dataset": insert_your_dataframe_here
                    "data_asset_name": "optionally_insert_your_data_asset_name_here",
                }

            Then we get the Batch via:

            .. code-block:: python

                batch = context.get_batch(
                    batch_kwargs=batch_kwargs,
                    expectation_suite_name=suite
                )


        3. Check your data

            You can check that the first few lines of your Batch are what you expect by running:

            .. code-block:: python

                batch.head()


        Now that you have a :ref:`Batch <reference__core_concepts__datasources>`, you can use it to create :ref:`Expectations <expectations>` or validate the data.


    .. tab-container:: tab1
        :title: Show Docs for V3 (Batch Request) API


        What used to be called a “Batch” in the old API was replaced with :ref:`Validator <reference__core_concepts__validation>`. A Validator knows how to validate a particular Batch of data on a particular :ref:`Execution Engine <reference__core_concepts>` against a particular :ref:`Expectation Suite <reference__core_concepts__expectations__expectation_suites>`. In interactive mode, the Validator can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis.

        You can read more about the core classes that make Great Expectations run in our :ref:`Core Concepts reference guide <reference__core_concepts>`.

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Configured and loaded a Data Context <how_to_guides__configuring_data_contexts>`
            - Configured a :ref:`Spark Datasource<how_to_guides__configuring_datasources__how_to_configure_a_spark_filesystem_datasource>`
            - Identified an in-memory Spark DataFrame that you would like to use as the data to validate. **OR**
            - Identified a filesystem or S3 path to a file that contains the data you would like to use to validate.

        Steps
        -----

        0. Load or create a Data Context

          The ``context`` referenced below can be loaded from disk or configured in code.

          Load an on-disk Data Context via:

          .. code-block:: python

            import pyspark

            import great_expectations as ge
            from great_expectations import DataContext
            from great_expectations.core import ExpectationSuite
            from great_expectations.core.batch import RuntimeBatchRequest
            from great_expectations.core.util import get_or_create_spark_application
            from great_expectations.validator.validator import Validator

            context = ge.get_context()

          Create an in-code Data Context using these instructions: :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>`

        1. Obtain an Expectation Suite

          .. code-block:: python

            suite: ExpectationSuite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

          Alternatively, you can simply use the name of the Expectation Suite.

          .. code-block:: python

            suite_name: str = "insert_your_expectation_suite_name_here"

          If you have not already created an Expectation Suite, you can do so now.

          .. code-block:: python

            suite: ExpectationSuite = context.create_expectation_suite("insert_your_expectation_suite_name_here")

        2. Construct a Runtime Batch Request

          We will create a ``RuntimeBatchRequest`` and pass it our Spark DataFrame or path via the ``runtime_parameters`` argument, under either the ``batch_data`` or ``path`` key. The ``batch_identifiers`` argument is required and must be a non-empty dictionary containing all of the Batch Identifiers specified in your Runtime Data Connector configuration.

          If you are providing a filesystem path instead of a materialized DataFrame, you may use either an absolute or relative path (with respect to the current working directory). Under the hood, Great Expectations will instantiate a Spark Dataframe using the appropriate ``spark.read.*`` method, which will be inferred from the file extension. If your file names do not have extensions, you can specify the appropriate reader method explicitly via the ``batch_spec_passthrough`` argument. Any Spark reader options (i.e. ``delimiter`` or ``header``) that are required to properly read your data can also be specified with the ``batch_spec_passthrough`` argument, in a dictionary nested under a key named ``reader_options``.

          Example ``great_expectations.yml`` Datsource configuration:

          .. code-block:: yaml

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

          Example Runtime Batch Request using an in-memory DataFrame:

          .. code-block:: python

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

          Example Runtime Batch Request using a path:

          .. code-block:: python

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

          .. admonition:: Best Practice

            Though not strictly required, we recommend that you make every Data Asset Name **unique**. Choosing a unique Data Asset Name makes it easier to navigate quickly through Data Docs and ensures your logical Data Assets are not confused with any particular view of them provided by an Execution Engine.

        3. Construct a Validator

          .. code-block:: python

            my_validator: Validator = context.get_validator(
                batch_request=runtime_batch_request,
                expectation_suite=suite,  # OR
                # expectation_suite_name=suite_name
            )

          Alternatively, you may skip step 2 and pass the same Runtime Batch Request instantiation arguments, along with the Expectation Suite (or name), directly to to the ``get_validator`` method.

          .. code-block:: python

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

        4. Check your data

          You can check that the first few lines of your Batch are what you expect by running:

          .. code-block:: python

            my_validator.head()

        Now that you have a Validator, you can use it to create Expectations or validate the data.

.. discourse::
    :topic_identifier: 191
