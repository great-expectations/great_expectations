from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigSchema,
    DatasourceConfig,
    DefaultInMemoryDataContextConfig,
    S3BackendEcosystem,
)
from great_expectations.datasource import PandasDatasource
from great_expectations.datasource.batch_kwargs_generator import (
    SubdirReaderBatchKwargsGenerator,
)

"""
What does this test and why?

This file will hold various tests to ensure that the UI functions as expected when creating a DataContextConfig object. It will ensure that the appropriate defaults are used, including when the backend_ecosystem parameter is set.
"""


def test_DataContextConfig_with_simple_defaults():
    """
    What does this test and why?
    Ensure that a very simple DataContextConfig setup with many defaults is created accurately
    """

    data_context_config = DefaultInMemoryDataContextConfig(
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
    )

    data_context_id = data_context_config.anonymous_usage_statistics.data_context_id

    data_context_config_schema = DataContextConfigSchema()

    assert data_context_config_schema.dump(data_context_config) == {
        "anonymous_usage_statistics": {
            "data_context_id": data_context_id,
            "enabled": True,
        },
        "config_variables_file_path": None,
        "config_version": 2.0,
        "data_docs_sites": {
            "local_site": {
                "class_name": "SiteBuilder",
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "uncommitted/data_docs/local_site/",
                },
                "show_how_to_buttons": True,
            }
        },
        "datasources": {
            "my_pandas_datasource": {
                "batch_kwargs_generators": {
                    "subdir_reader": {
                        "base_directory": "../data/",
                        "class_name": "SubdirReaderBatchKwargsGenerator",
                    }
                },
                "class_name": "PandasDatasource",
                "data_asset_type": {
                    "class_name": "PandasDataset",
                    "module_name": "great_expectations.dataset",
                },
                "module_name": "great_expectations.datasource",
            }
        },
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "expectations_store_name": "expectations_store",
        "notebooks": None,
        "plugins_directory": None,
        "stores": {
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "expectations/",
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": "uncommitted/validations/",
                },
            },
        },
        "validation_operators": {
            "action_list_operator": {
                "action_list": [
                    {
                        "action": {"class_name": "StoreValidationResultAction"},
                        "name": "store_validation_result",
                    },
                    {
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                        "name": "store_evaluation_params",
                    },
                    {
                        "action": {"class_name": "UpdateDataDocsAction"},
                        "name": "update_data_docs",
                    },
                ],
                "class_name": "ActionListValidationOperator",
            }
        },
        "validations_store_name": "validations_store",
    }


def test_DefaultInMemoryDataContextConfig_with_s3_defaults():
    """
    What does this test and why?
    TODO: docstring
    """

    data_context_config = DefaultInMemoryDataContextConfig(
        datasources={
            "my_pandas_datasource": DatasourceConfig(
                class_name="PandasDatasource",
                module_name="great_expectations.datasource",
                data_asset_type={
                    "module_name": "great_expectations.dataset",
                    "class_name": "PandasDataset",
                },
                batch_kwargs_generators={
                    "subdir_reader": {
                        "class_name": "SubdirReaderBatchKwargsGenerator",
                        "base_directory": "../data/",
                    }
                },
            )
        },
        backend_ecosystem=S3BackendEcosystem(default_bucket_name="my_default_bucket"),
    )

    data_context_id = data_context_config.anonymous_usage_statistics.data_context_id

    data_context_config_schema = DataContextConfigSchema()

    assert data_context_config_schema.dump(data_context_config) == {
        "anonymous_usage_statistics": {
            "data_context_id": data_context_id,
            "enabled": True,
        },
        "config_variables_file_path": None,
        "config_version": 2.0,
        "data_docs_sites": {
            "s3_site": {
                "class_name": "SiteBuilder",
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
                "store_backend": {
                    "bucket": "my_default_bucket",
                    "class_name": "TupleS3StoreBackend",
                    "prefix": "data_docs",
                },
            }
        },
        "datasources": {
            "my_pandas_datasource": {
                "batch_kwargs_generators": {
                    "subdir_reader": {
                        "base_directory": "../data/",
                        "class_name": "SubdirReaderBatchKwargsGenerator",
                    }
                },
                "class_name": "PandasDatasource",
                "data_asset_type": {
                    "class_name": "PandasDataset",
                    "module_name": "great_expectations.dataset",
                },
                "module_name": "great_expectations.datasource",
            }
        },
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "expectations_store_name": "expectations_S3_store",
        "notebooks": None,
        "plugins_directory": None,
        "stores": {
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "expectations_S3_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "bucket": "my_default_bucket",
                    "class_name": "TupleS3StoreBackend",
                    "prefix": "expectations",
                },
            },
            "validations_S3_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "bucket": "my_default_bucket",
                    "class_name": "TupleS3StoreBackend",
                    "prefix": "validations",
                },
            },
        },
        "validation_operators": {
            "action_list_operator": {
                "action_list": [
                    {
                        "action": {"class_name": "StoreValidationResultAction"},
                        "name": "store_validation_result",
                    },
                    {
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                        "name": "store_evaluation_params",
                    },
                    {
                        "action": {"class_name": "UpdateDataDocsAction"},
                        "name": "update_data_docs",
                    },
                ],
                "class_name": "ActionListValidationOperator",
            }
        },
        "validations_store_name": "validations_S3_store",
    }


# TODO: Other tests:
#  Other backend ecosystems
#  Overrides work correctly (e.g. specifying an expectations_store_name in the constructor overrides
#  one specified in the BackendEcosystem, probably should either throw warning or error here)
#  Also test if additional parameters are passed e.g. credentials for SqlAlchemyDatasource
#  What about substituted variables? Will they just work?
