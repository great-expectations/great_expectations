.. _how_to_guides__configuring_data_contexts__how_to_instantiate_a_data_context_without_a_yml_file:

How to instantiate a Data Context without a yml file
====================================================

This guide will help you instantiate a Data Context without a yml file, aka configure a Data Context in code. If you are working in an environment without easy access to a local filesystem (e.g. AWS Spark EMR, Databricks, etc.) you may wish to configure your Data Context in code, within your notebook or workflow tool (e.g. Airflow DAG node).

.. admonition:: Prerequisites: This how-to guide assumes you have already:

    - :ref:`Followed the Getting Started tutorial and have a basic familiarity with the Great Expectations configuration<tutorials__getting_started>`.

.. note::

    See also our companion video for this guide: `Data Contexts In Code <https://youtu.be/4VMOYpjHNhM>`_.

Steps
-----

1. Create a DataContextConfig

    The DataContextConfig holds all of the associated configuration parameters to build a DataContext. There are defaults set for you to minimize configuration in typical cases, but please note that every parameter is configurable and all defaults are overridable. Also note that DatasourceConfig also has defaults which can be overridden.

    Here we will show a few examples of common configurations, using the ``store_backend_defaults`` parameter. Note that you can continue with the existing API sans defaults by omitting this parameter, and you can override all of the parameters as shown in the last example. Note that a parameter set in ``DataContextConfig`` will override a parameter set in ``store_backend_defaults`` if both are used.

    The following ``store_backend_defaults`` are currently available:
        - :py:class:`~great_expectations.data_context.types.base.S3StoreBackendDefaults`
        - :py:class:`~great_expectations.data_context.types.base.GCSStoreBackendDefaults`
        - :py:class:`~great_expectations.data_context.types.base.DatabaseStoreBackendDefaults`
        - :py:class:`~great_expectations.data_context.types.base.FilesystemStoreBackendDefaults`

    The following example shows a Data Context configuration with an SQLAlchemy datasource and an AWS s3 bucket for all metadata stores, using default prefixes. Note that you can still substitute environment variables as in the YAML based configuration to keep sensitive credentials out of your code.

    .. code-block:: python

        from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig
        from great_expectations.data_context import BaseDataContext

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

    The following example shows a Data Context configuration with a Pandas datasource and local filesystem defaults for metadata stores. Note: imports are omitted in the following examples. Note: You may add an optional root_directory parameter to set the base location for the Store Backends.

    .. code-block:: python

        data_context_config = DataContextConfig(
            datasources={
                "my_pandas_datasource": DatasourceConfig(
                    class_name="PandasDatasource",
                    batch_kwargs_generators={
                        "subdir_reader": {
                            "class_name": "SubdirReaderBatchKwargsGenerator",
                            "base_directory": "/path/to/data",
                        }
                    },
                )
            },
            store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="optional/absolute/path/for/stores"),
        )


    The following example shows a Data Context configuration with an SQLAlchemy datasource and two GCS buckets for metadata stores, using some custom and some default prefixes. Note that you can still substitute environment variables as in the YAML based configuration to keep sensitive credentials out of your code. ``default_bucket_name``, ``default_project_name`` sets the default value for all stores that are not specified individually.

    The resulting DataContextConfig from the following example creates an Expectations store and Data Docs using the ``my_default_bucket`` and ``my_default_project`` parameters since their bucket and project is not specified explicitly. The validations store is created using the explicitly specified ``my_validations_bucket`` and ``my_validations_project``. Further, the prefixes are set for the Expectations store and validations store, while data docs use the default ``data_docs`` prefix.

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
            store_backend_defaults=GCSStoreBackendDefaults(
                default_bucket_name="my_default_bucket",
                default_project_name="my_default_project",
                validations_store_bucket_name="my_validations_bucket",
                validations_store_project_name="my_validations_project",
                validations_store_prefix="my_validations_store_prefix",
                expectations_store_prefix="my_expectations_store_prefix",
            ),
        )


    The following example sets overrides for many of the parameters available to you when creating a DataContextConfig and a Datasource

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
                        "bucket": "my_expectations_store_bucket",
                        "prefix": "my_expectations_store_prefix",
                    },
                },
                "validations_S3_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "TupleS3StoreBackend",
                        "bucket": "my_validations_store_bucket",
                        "prefix": "my_validations_store_prefix",
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
                        "bucket":  "my_data_docs_bucket",
                        "prefix":  "my_optional_data_docs_prefix",
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

    If you are using Airflow, you may wish to pass this Data Context to your GreatExpectationsOperator as a parameter. See the following guide for more details:

    - :ref:`deployment_airflow`


Additional resources
--------------------

- :ref:`How to instantiate a Data Context on an EMR Spark Cluster <how_to_instantiate_a_data_context_on_an_emr_spark_cluster>`
- :ref:`How to instantiate a Data Context on Databricks Spark cluster <how_to_instantiate_a_data_context_on_a_databricks_spark_cluster>`

.. discourse::
    :topic_identifier: 163
