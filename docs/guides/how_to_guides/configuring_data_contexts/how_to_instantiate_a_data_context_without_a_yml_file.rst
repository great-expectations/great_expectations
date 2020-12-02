.. _how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file:

How to instantiate a Data Context without a yml file
====================================================

This guide will help you instantiate a Data Context without a yml file, aka configure a Data Context in code. If you are working in an environment without easy access to a local filesystem (e.g. AWS Spark EMR, Databricks, etc.) you may wish to configure your Data Context in code, within your notebook or workflow tool (e.g. Airflow DAG node).

Steps
-----

1. Create a DataContextConfig

    The DataContextConfig holds all of the associated configuration parameters to build a DataContext. There are defaults set for you to minimize configuration in typical cases, but please note that every parameter is configurable and all defaults are overridable. Also note that DatasourceConfig also has defaults which can be overridden.

    Here we will show a few examples of common configurations, using the ``store_backend_defaults`` parameter. Note that you can continue with the existing API sans defaults by omitting this parameter, and you can override all of the parameters as in the last example. Note that a parameter set in ``DataContextConfig`` will override a parameter set in ``store_backend_defaults`` if both are used.

    This example shows a Data Context configuration with an sqlalchemy datasource and an AWS s3 bucket for all metadata stores, using default prefixes. Note that you can still substitute environment variables as in the YAML based configuration to keep sensitive credentials out of your code.

    .. code-block:: python

        data_context_config = DataContextConfig(
            datasources={
                "my_sqlalchemy_datasource": DatasourceConfig(
                    class_name="SqlAlchemyDatasource",
                    credentials={
                        "drivername": "custom_drivername",
                        "host": "custom_host",
                        "port": "custom_port",
                        "username": "${USERNAME_FROM_ENVIRONMENT_VARIABLE}",
                        "password": "${PASSWORD_FROM_ENVIRONMENT_VARIABLE}",
                        "database": "custom_database",
                    },
                )
            },
            store_backend_defaults=S3StoreBackendDefaults(default_bucket_name="my_default_bucket"),
        )

    This example shows a Data Context configuration with a pandas datasource and local filesystem defaults for metadata stores.

    .. code-block:: python

        data_context_config = DataContextConfig(
            datasources={
                "my_pandas_datasource": DatasourceConfig(
                    class_name="PandasDatasource",
                    batch_kwargs_generators={
                        "subdir_reader": {
                            "class_name": "SubdirReaderBatchKwargsGenerator",
                            "base_directory": "../data/",
                        }
                    },
                )
            },
            store_backend_defaults=FilesystemStoreBackendDefaults(),
    )

    This example shows setting overrides for many of the parameters available to you when creating a DataContextConfig and a Datasource

    .. code-block:: python

        project_config = DataContextConfig(
            config_version=2,
            plugins_directory=None,
            config_variables_file_path=None,
            datasources={
                "my_spark_datasource": {
                    "data_asset_type": {
                        "class_name": "SparkDFDataset",
                        "module_name": "great_expectations.dataset",
                    },
                    "class_name": "SparkDFDatasource",
                    "module_name": "great_expectations.datasource",
                    "batch_kwargs_generators": {},
                }
            },
            stores={
                "expectations_S3_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": "REPLACE ME",  # TODO: replace with your value
                        "prefix": "REPLACE ME",  # TODO: replace with your value
                    },
                },
                "validations_S3_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": "REPLACE ME",  # TODO: replace with your value
                        "prefix": "REPLACE ME",  # TODO: replace with your value
                    },
                },
                "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            },
            expectations_store_name="expectations_S3_store",
            validations_store_name="validations_S3_store",
            evaluation_parameter_store_name="evaluation_parameter_store",
            data_docs_sites={
                "s3_site": {
                    "class_name": "SiteBuilder",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket":  "REPLACE ME",  # TODO: replace with your value
                    },
                    "site_index_builder": {
                        "class_name": "DefaultSiteIndexBuilder",
                        "show_cta_footer": True,
                    },
                }
            },
            validation_operators={
                "action_list_operator": {
                    "class_name": "ActionListValidationOperator",
                    "action_list": [
                        {
                            "name": "store_validation_result",
                            "action": {"class_name": "StoreValidationResultAction"},
                        },
                        {
                            "name": "store_evaluation_params",
                            "action": {"class_name": "StoreEvaluationParametersAction"},
                        },
                        {
                            "name": "update_data_docs",
                            "action": {"class_name": "UpdateDataDocsAction"},
                        },
                    ],
                }
            },
            anonymous_usage_statistics={
              "enabled": True
            }
        )


2. Pass this DataContextConfig as a project_config to BaseDataContext

    .. code-block:: python

        context = BaseDataContext(project_config=data_context_config)

3. Use this BaseDataContext instance as your DataContext


Additional resources
--------------------

- :ref:`How to instantiate a Data Context on an EMR Spark Cluster <how_to_instantiate_a_data_context_on_an_emr_spark_cluster>`
- :ref:`How to instantiate a Data Context on Databricks Spark cluster <how_to_instantiate_a_data_context_on_a_databricks_spark_cluster>`

.. discourse::
    :topic_identifier: 163
