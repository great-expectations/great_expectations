.. _how_to_instantiate_a_data_context_on_a_databricks_spark_cluster:

How to instantiate a Data Context on Databricks Spark cluster
=========================================================

This guide will help you instantiate a Data Context on an Databricks Spark cluster.

The guide demonstrates the recommended path for instantiating a Data Context without a full configuration directory and without using the Great Expectations :ref:`command line interface (CLI) <command_line>`.

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Followed the Getting Started tutorial and have a basic familiarity with the Great Expectations configuration <getting_started>`.

Steps
-----

This how-to-guide assumes that you are using a Databricks Notebook, and using the Databricks File Store (DBFS) as the Metadata Store and DataDocs store. The `DBFS is a file store <https://docs.databricks.com/data/databricks-file-system.html>`_ that is native to Databricks clusters and Notebooks. Files on DBFS can be written and read as if they were on a local filesystem, just by `adding the /dbfs/ prefix to the path <https://docs.databricks.com/data/databricks-file-system.html#local-file-apis>`_. For information on how to configure Databricks for filesystems on Azure and AWS, please see the associated documentation in the Additional Notes section below.

1. **Install Great Expectations on your Databricks Spark cluster.**

   Copy this code snippet into a cell in your Databricks Spark notebook and run it:

   .. code-block:: python

      dbutils.library.installPyPI("great_expectations")


2. **Configure a Data Context in code.**

    Follow the steps for creating an in-code Data Context in :ref:`How to instantiate a Data Context without a yml file <how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file>` using the FilesystemStoreBackendDefaults or configuring stores as in the code block below.

    .. note::
       If you are using DBFS for your stores, make sure to set the ``root_directory`` of FilesystemStoreBackendDefaults to ``/dbfs/`` or ``/dbfs/FileStore/`` to make sure you are writing to DBFS and not the Spark driver node filesystem. If you have mounted another file store (e.g. s3 bucket) to use instead of DBFS, you can use that path here instead.

    .. content-tabs::

        .. tab-container:: tab0
            :title: Show Docs for V2 (Batch Kwargs) API

            .. code-block:: python
                :linenos:

                from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
                from great_expectations.data_context import BaseDataContext

                # Example filesystem Datasource
                my_spark_datasource_config = DatasourceConfig(
                    class_name="SparkDFDatasource",
                    batch_kwargs_generators={
                        "subdir_reader": {
                            "class_name": "SubdirReaderBatchKwargsGenerator",
                            "base_directory": "/FileStore/tables/",
                        }
                    },
                )

                data_context_config = DataContextConfig(
                    datasources={"my_spark_datasource": my_spark_datasource_config},
                    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/dbfs/FileStore/"),
                )
                context = BaseDataContext(project_config=data_context_config)

        .. tab-container:: tab1
            :title: Show Docs for V3 (Batch Request) API

            .. code-block:: python
                :linenos:

                from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
                from great_expectations.data_context import BaseDataContext

                # Example RuntimeDataConnector for use with a dataframe batch
                my_spark_datasource_config = DatasourceConfig(
                    class_name="Datasource",
                    execution_engine={"class_name": "SparkDFExecutionEngine"},
                    data_connectors={
                        "insert_your_runtime_data_connector_name_here": {
                            "module_name": "great_expectations.datasource.data_connector",
                            "class_name": "RuntimeDataConnector",
                            "batch_identifiers": [
                                "some_key_maybe_pipeline_stage",
                                "some_other_key_maybe_run_id"
                            ]
                        }
                    }
                )

                data_context_config = DataContextConfig(
                    datasources={"my_spark_datasource": my_spark_datasource_config},
                    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/dbfs/FileStore/"),
                )
                context = BaseDataContext(project_config=data_context_config)

    You can have more fine-grained control over where your stores are located by passing the ``stores`` parameter to DataContextConfig as in the following example.

    .. content-tabs::

        .. tab-container:: tab0
            :title: Docs for Legacy Checkpoints (<=0.13.7)

            .. code-block:: python
                :linenos:

                data_context_config = DataContextConfig(
                    datasources={"my_spark_datasource": my_spark_datasource_config},
                    stores={
                        "insert_your_custom_expectations_store_name_here": {
                            "class_name": "ExpectationsStore",
                            "store_backend": {
                                "class_name": "TupleFilesystemStoreBackend",
                                "base_directory": "/dbfs/FileStore/path_to_your_expectations_store/",
                            },
                        },
                        "insert_your_custom_validations_store_name_here": {
                            "class_name": "ValidationsStore",
                            "store_backend": {
                                "class_name": "TupleFilesystemStoreBackend",
                                "base_directory": "/dbfs/FileStore/path_to_your_validations_store/",
                            },
                        },
                        "insert_your_custom_evaluation_parameter_store_name_here": {
                            "class_name": "EvaluationParameterStore"
                        },
                    },
                    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/dbfs/FileStore/"),
                    )

        .. tab-container:: tab1
            :title: Docs for Class-Based Checkpoints (>=0.13.8)

            .. code-block:: python
                :linenos:

                data_context_config = DataContextConfig(
                    datasources={"my_spark_datasource": my_spark_datasource_config},
                    stores={
                        "insert_your_custom_expectations_store_name_here": {
                            "class_name": "ExpectationsStore",
                            "store_backend": {
                                "class_name": "TupleFilesystemStoreBackend",
                                "base_directory": "/dbfs/FileStore/path_to_your_expectations_store/",
                            },
                        },
                        "insert_your_custom_validations_store_name_here": {
                            "class_name": "ValidationsStore",
                            "store_backend": {
                                "class_name": "TupleFilesystemStoreBackend",
                                "base_directory": "/dbfs/FileStore/path_to_your_validations_store/",
                            },
                        },
                        "insert_your_custom_evaluation_parameter_store_name_here": {
                            "class_name": "EvaluationParameterStore"
                        },
                        "insert_your_custom_checkpoint_store_name_here": {
                            "class_name": "CheckpointStore",
                            "store_backend": {
                                "class_name": "TupleFilesystemStoreBackend",
                                "base_directory": "/dbfs/FileStore/path_to_your_checkpoints_store/",
                            },
                        },
                    },
                    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/dbfs/FileStore/"),
                    )

3. **Test your configuration.**

   After you have created your Data Context, copy this code snippet into a cell in your Databricks Spark notebook, run it and verify that no error is displayed:

   .. code-block:: python

      context.list_datasources()


Additional notes
----------------

- If you're continuing to work in a Databricks notebook, the following code-snippet could be used to load and run Expectations on a ``csv`` file that lives in DBFS.

    .. content-tabs::

        .. tab-container:: tab0
            :title: Show Docs for V2 (Batch Kwargs) API

            Please note that this code-snippet assumes that you have already installed Great Expectations and configured a :ref:`Datasource <reference__core_concepts__datasources>`.

            .. code-block:: python
                :linenos:

                from great_expectations.data_context import BaseDataContext

                file_location = "/FileStore/tables/dc_wikia_data.csv"
                file_type = "csv"

                # CSV options
                infer_schema = "false"
                first_row_is_header = "false"
                delimiter = ","

                # The applied options are for CSV files. For other file types, these will be ignored.
                df = spark.read.format(file_type) \
                    .option("inferSchema", infer_schema) \
                    .option("header", first_row_is_header) \
                    .option("sep", delimiter) \
                    .load(file_location)

                # NOTE: project_config is a DataContextConfig set up as in the examples above.
                context = BaseDataContext(project_config=project_config)
                context.create_expectation_suite("insert_your_suite_name_here")

                my_batch = context.get_batch({
                    "dataset": df,
                    "datasource": "insert_your_datasource_name_here",
                }, "insert_your_suite_name_here")

                my_batch.expect_table_row_count_to_equal(140)


        .. tab-container:: tab1
            :title: Show Docs for V3 (Batch Request) API

            .. code-block:: python
                :linenos:

                from great_expectations.data_context import BaseDataContext
                from great_expectations.data_context.types.base import DatasourceConfig
                from great_expectations.core.batch import BatchRequest

                # Load your data into a dataframe

                file_location = "/FileStore/tables/dc_wikia_data.csv"
                file_type = "csv"

                # CSV options
                infer_schema = "false"
                first_row_is_header = "false"
                delimiter = ","

                # The applied options are for CSV files. For other file types, these will be ignored.
                df = spark.read.format(file_type) \
                    .option("inferSchema", infer_schema) \
                    .option("header", first_row_is_header) \
                    .option("sep", delimiter) \
                    .load(file_location)

                # Create a DataContext in code from a DataContextConfig with DatasourceConfig

                my_spark_datasource_config = DatasourceConfig(
                    class_name="Datasource",
                    execution_engine={"class_name": "SparkDFExecutionEngine"},
                    data_connectors={
                      "insert_your_runtime_data_connector_name_here": {
                        "module_name": "great_expectations.datasource.data_connector",
                        "class_name": "RuntimeDataConnector",
                        "batch_identifiers": [
                          "some_key_maybe_pipeline_stage",
                          "some_other_key_maybe_run_id"
                        ]
                      }
                    }
                  )

                project_config = DataContextConfig(
                    datasources={"insert_your_datasource_name_here": my_spark_datasource_config},
                    store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/dbfs/FileStore/")
                )

                context = BaseDataContext(project_config=project_config)

                # Create a RuntimeBatchRequest

                batch_request = RuntimeBatchRequest(
                    datasource_name="insert_your_datasource_name_here",
                    data_connector_name="insert_your_runtime_data_connector_name_here",
                    data_asset_name="insert_your_data_asset_name_here",
                    runtime_parameters: {
                      batch_data=df,
                    },
                    data_connector_query={
                        "batch_filter_parameters": {
                            "some_key_maybe_pipeline_stage": "ingestion step 1",
                            "some_other_key_maybe_run_id": "run 18"
                        }
                    }
                )

                # Create or load your Expectation Suite
                # NOTE: You should either create or load, this try/except block is for convenience

                from great_expectations.exceptions import DataContextError
                try:
                  suite = context.create_expectation_suite("insert_your_suite_name_here")
                except DataContextError:
                  suite = context.get_expectation_suite("insert_your_suite_name_here")

                # Get a Validator

                my_validator = context.get_validator(
                    batch_request=batch_request,
                    expectation_suite=suite
                )

                # Add Expectations

                my_validator.expect_table_row_count_to_equal(140)
                my_validator.expect_column_values_to_not_be_null("_c0")

                # Save the Expectation Suite to the Expectation Store

                my_validator.save_expectation_suite(discard_failed_expectations=False)



Additional resources
--------------------
- How to create a Data Source in :ref:`Databricks AWS <how_to_guides__configuring_datasources__how_to_configure_a_databricks_aws_datasource>`
- How to create a Data Source in :ref:`Databricks Azure <how_to_guides__configuring_datasources__how_to_configure_a_databricks_azure_datasource>`

.. discourse::
    :topic_identifier: 320
