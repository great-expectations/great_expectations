.. _how_to_guides__creating_batches__how_to_load_a_spark_dataframe_as_a_batch:

How to load a Spark dataframe as a batch
========================================

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


        What used to be called a “batch” in the old API was replaced with a :ref:`Validator <reference__core_concepts__validation>`. A Validator knows how to validate a particular batch of data on a particular :ref:`Execution Engine <reference__core_concepts>` against a particular :ref:`Expectation Suite <reference__core_concepts__expectations__expectation_suites>`. In interactive mode, the Validator can store and update an Expectation Suite while conducting Data Discovery or Exploratory Data Analysis.

        You can read more about the core classes that make Great Expectations run in our :ref:`Core Concepts reference guide <reference__core_concepts>`.

        .. admonition:: Prerequisites: This how-to guide assumes you have already:

            - :ref:`Set up a working deployment of Great Expectations <tutorials__getting_started>`
            - :ref:`Configured and loaded a DataContext <how_to_guides__configuring_data_contexts>`
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


        1. Configure a Datasource

            Configure a :ref:`Datasource <reference__core_concepts__datasources>` using the :ref:`RuntimeDataConnector <reference__core_concepts__datasources>` to connect to your DataFrame. Since we are reading a Spark DataFrame, we use the ``SparkExecutionEngine``. You can use ``runtime_keys`` to define what data you are able to attach as additional metadata to your DataFrame using the ``partition_request`` parameter (shown in step 3). You can use the below example Datasource as a template, and paste it into your great_expectations.yml

            .. code-block:: yaml

                insert_your_spark_datasource_name_here:
                  class_name: Datasource
                  execution_engine:
                    class_name: SparkDFExecutionEngine
                  data_connectors:
                    insert_your_runtime_data_connector_name_here:
                      module_name: great_expectations.datasource.data_connector
                      class_name: RuntimeDataConnector
                      runtime_keys:
                        - some_key_maybe_pipeline_stage
                        - some_other_key_maybe_run_id


        2. Obtain an Expectation Suite

            .. code-block:: python

                suite = context.get_expectation_suite("insert_your_expectation_suite_name_here")

            Alternatively, if you have not already created a suite, you can do so now.

            .. code-block:: python

                suite = context.create_expectation_suite("insert_your_expectation_suite_name_here")

        3. Construct a BatchRequest

            We will create a ``BatchRequest`` and pass it our DataFrame via the ``batch_data`` argument.

            Attributes inside the ``partition_request`` are optional - you can use them to attach additional metadata to your DataFrame. When configuring the Data Connector, you used ``runtime_keys`` to define which keys are allowed.

            NOTE: for now, ``data_asset_name`` can only be set to this predefined string: ``“IN_MEMORY_DATA_ASSET”``. We will fix it very soon and will allow you to specify your own name.

            .. code-block:: python

                from great_expectations.core.batch import BatchRequest

                batch_request = BatchRequest(
                    datasource_name="insert_your_spark_datasource_name_here",
                    data_connector_name="insert_your_runtime_data_connector_name_here",
                    batch_data=insert_your_dataframe_here,
                    data_asset_name="IN_MEMORY_DATA_ASSET",
                    partition_request={
                        "partition_identifiers": {
                            "some_key_maybe_pipeline_stage": "ingestion step 1",
                            "some_other_key_maybe_run_id": "run 18"
                        }
                    }
                )


        4. Construct a Validator

            .. code-block:: python

                my_validator = context.get_validator(
                    batch_request=batch_request,
                    expectation_suite=suite
                )


        5. Check your data

            You can check that the first few lines of your Batch are what you expect by running:

            .. code-block:: python

                my_validator.active_batch.head()

        Now that you have a Validator, you can use it to create Expectations or validate the data.

.. discourse::
    :topic_identifier: 191
