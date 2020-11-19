from typing import Dict

import pytest

from great_expectations import DataContext
from great_expectations.data_context.types.base import (
    DatabaseBackendEcosystem,
    DataContextConfig,
    DataContextConfigDefaults,
    DataContextConfigSchema,
    DatasourceConfig,
    FilesystemBackendEcosystem,
    GCSBackendEcosystem,
    S3BackendEcosystem,
)

"""
What does this test and why?

This file will hold various tests to ensure that the UI functions as expected when creating a DataContextConfig object. It will ensure that the appropriate defaults are used, including when the backend_ecosystem parameter is set.
"""


@pytest.fixture(scope="function")
def construct_data_context_config():
    """
    Construct a DataContextConfig fixture given the modifications in the input parameters
    Returns:
        Dictionary representation of a DataContextConfig to compare in tests
    """

    def _construct_data_context_config(
        data_context_id: str,
        datasources: Dict,
        config_version: float = float(
            DataContextConfigDefaults.DEFAULT_CONFIG_VERSION.value
        ),
        expectations_store_name: str = DataContextConfigDefaults.DEFAULT_EXPECTATIONS_STORE_NAME.value,
        validations_store_name: str = DataContextConfigDefaults.DEFAULT_VALIDATIONS_STORE_NAME.value,
        evaluation_parameter_store_name: str = DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value,
        stores: Dict = DataContextConfigDefaults.DEFAULT_STORES.value,
        validation_operators=DataContextConfigDefaults.DEFAULT_VALIDATION_OPERATORS.value,
        data_docs_sites: Dict = DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value,
    ):
        return {
            "config_version": config_version,
            "datasources": datasources,
            "expectations_store_name": expectations_store_name,
            "validations_store_name": validations_store_name,
            "evaluation_parameter_store_name": evaluation_parameter_store_name,
            "plugins_directory": None,
            "validation_operators": validation_operators,
            "stores": stores,
            "data_docs_sites": data_docs_sites,
            "notebooks": None,
            "config_variables_file_path": None,
            "anonymous_usage_statistics": {
                "data_context_id": data_context_id,
                "enabled": True,
            },
        }

    return _construct_data_context_config


@pytest.fixture()
def default_pandas_datasource_config():
    return {
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
    }


@pytest.fixture()
def default_spark_datasource_config():
    return {
        "my_spark_datasource": {
            "batch_kwargs_generators": {},
            "class_name": "SparkDFDatasource",
            "data_asset_type": {
                "class_name": "SparkDFDataset",
                "module_name": "great_expectations.dataset",
            },
            "module_name": "great_expectations.datasource",
        }
    }


# TODO: Create other datasource config fixtures


def test_DataContextConfig_with_BaseBackendEcosystem_and_simple_defaults(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Ensure that a very simple DataContextConfig setup with many defaults is created accurately
    and produces a valid DataContextConfig
    """

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
    )

    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources=default_pandas_datasource_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_S3BackendEcosystem(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that using S3BackendEcosystem as the backend_ecosystem applies appropriate
    defaults, including default_bucket_name getting propagated to all stores.
    """

    data_context_config = DataContextConfig(
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

    # Create desired config
    desired_stores_config = {
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
    }
    desired_data_docs_sites_config = {
        "s3_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
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
    }

    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources=default_pandas_datasource_config,
        expectations_store_name="expectations_S3_store",
        validations_store_name="validations_S3_store",
        evaluation_parameter_store_name=DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value,
        stores=desired_stores_config,
        data_docs_sites=desired_data_docs_sites_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_S3BackendEcosystem_using_all_parameters(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that S3BackendEcosystem parameters are handled appropriately
    E.g. Make sure that default_bucket_name is ignored if individual bucket names are passed
    """

    data_context_config = DataContextConfig(
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
        backend_ecosystem=S3BackendEcosystem(
            default_bucket_name="custom_default_bucket_name",
            expectations_store_bucket_name="custom_expectations_store_bucket_name",
            validations_store_bucket_name="custom_validations_store_bucket_name",
            data_docs_bucket_name="custom_data_docs_store_bucket_name",
            expectations_store_prefix="custom_expectations_store_prefix",
            validations_store_prefix="custom_validations_store_prefix",
            data_docs_prefix="custom_data_docs_prefix",
            expectations_store_name="custom_expectations_S3_store_name",
            validations_store_name="custom_validations_S3_store_name",
            evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        ),
    )

    # Create desired config
    desired_stores_config = {
        "custom_evaluation_parameter_store_name": {
            "class_name": "EvaluationParameterStore"
        },
        "custom_expectations_S3_store_name": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "bucket": "custom_expectations_store_bucket_name",
                "class_name": "TupleS3StoreBackend",
                "prefix": "custom_expectations_store_prefix",
            },
        },
        "custom_validations_S3_store_name": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "bucket": "custom_validations_store_bucket_name",
                "class_name": "TupleS3StoreBackend",
                "prefix": "custom_validations_store_prefix",
            },
        },
    }
    desired_data_docs_sites_config = {
        "s3_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "store_backend": {
                "bucket": "custom_data_docs_store_bucket_name",
                "class_name": "TupleS3StoreBackend",
                "prefix": "custom_data_docs_prefix",
            },
        }
    }

    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources=default_pandas_datasource_config,
        expectations_store_name="custom_expectations_S3_store_name",
        validations_store_name="custom_validations_S3_store_name",
        evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        stores=desired_stores_config,
        data_docs_sites=desired_data_docs_sites_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_FilesystemBackendEcosystem_and_simple_defaults(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Ensure that a very simple DataContextConfig setup using FilesystemBackendEcosystem is created accurately
    """

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
        backend_ecosystem=FilesystemBackendEcosystem(),
    )

    # Create desired config
    data_context_id = data_context_config.anonymous_usage_statistics.data_context_id
    desired_config = construct_data_context_config(
        data_context_id=data_context_id, datasources=default_pandas_datasource_config
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_GCSBackendEcosystem(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that using GCSBackendEcosystem as the backend_ecosystem applies appropriate
    defaults, including default_bucket_name & default_project_name getting propagated
    to all stores.
    """

    data_context_config = DataContextConfig(
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
        backend_ecosystem=GCSBackendEcosystem(
            default_bucket_name="my_default_bucket",
            default_project_name="my_default_project",
        ),
    )

    # Create desired config
    data_context_id = data_context_config.anonymous_usage_statistics.data_context_id
    desired_stores_config = {
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        "expectations_GCS_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "bucket": "my_default_bucket",
                "project": "my_default_project",
                "class_name": "TupleGCSStoreBackend",
                "prefix": "expectations",
            },
        },
        "validations_GCS_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "bucket": "my_default_bucket",
                "project": "my_default_project",
                "class_name": "TupleGCSStoreBackend",
                "prefix": "validations",
            },
        },
    }
    desired_data_docs_sites_config = {
        "gcs_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "store_backend": {
                "bucket": "my_default_bucket",
                "project": "my_default_project",
                "class_name": "TupleGCSStoreBackend",
                "prefix": "data_docs",
            },
        }
    }

    desired_config = construct_data_context_config(
        data_context_id=data_context_id,
        datasources=default_pandas_datasource_config,
        expectations_store_name="expectations_GCS_store",
        validations_store_name="validations_GCS_store",
        evaluation_parameter_store_name=DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value,
        stores=desired_stores_config,
        data_docs_sites=desired_data_docs_sites_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_GCSBackendEcosystem_using_all_parameters(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that GCSBackendEcosystem parameters are handled appropriately
    E.g. Make sure that default_bucket_name is ignored if individual bucket names are passed
    """

    data_context_config = DataContextConfig(
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
        backend_ecosystem=GCSBackendEcosystem(
            default_bucket_name="custom_default_bucket_name",
            default_project_name="custom_default_project_name",
            expectations_store_bucket_name="custom_expectations_store_bucket_name",
            validations_store_bucket_name="custom_validations_store_bucket_name",
            data_docs_bucket_name="custom_data_docs_store_bucket_name",
            expectations_store_project_name="custom_expectations_store_project_name",
            validations_store_project_name="custom_validations_store_project_name",
            data_docs_project_name="custom_data_docs_store_project_name",
            expectations_store_prefix="custom_expectations_store_prefix",
            validations_store_prefix="custom_validations_store_prefix",
            data_docs_prefix="custom_data_docs_prefix",
            expectations_store_name="custom_expectations_GCS_store_name",
            validations_store_name="custom_validations_GCS_store_name",
            evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        ),
    )

    # Create desired config
    desired_stores_config = {
        "custom_evaluation_parameter_store_name": {
            "class_name": "EvaluationParameterStore"
        },
        "custom_expectations_GCS_store_name": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "bucket": "custom_expectations_store_bucket_name",
                "project": "custom_expectations_store_project_name",
                "class_name": "TupleGCSStoreBackend",
                "prefix": "custom_expectations_store_prefix",
            },
        },
        "custom_validations_GCS_store_name": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "bucket": "custom_validations_store_bucket_name",
                "project": "custom_validations_store_project_name",
                "class_name": "TupleGCSStoreBackend",
                "prefix": "custom_validations_store_prefix",
            },
        },
    }
    desired_data_docs_sites_config = {
        "gcs_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "store_backend": {
                "bucket": "custom_data_docs_store_bucket_name",
                "project": "custom_data_docs_store_project_name",
                "class_name": "TupleGCSStoreBackend",
                "prefix": "custom_data_docs_prefix",
            },
        }
    }
    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources=default_pandas_datasource_config,
        expectations_store_name="custom_expectations_GCS_store_name",
        validations_store_name="custom_validations_GCS_store_name",
        evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        stores=desired_stores_config,
        data_docs_sites=desired_data_docs_sites_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_DatabaseBackendEcosystem(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that using DatabaseBackendEcosystem as the backend_ecosystem applies appropriate
    defaults, including default_credentials getting propagated to stores and not data_docs
    """

    data_context_config = DataContextConfig(
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
        backend_ecosystem=DatabaseBackendEcosystem(
            default_credentials={
                "drivername": "postgresql",
                "host": "localhost",
                "port": "65432",
                "username": "ge_tutorials",
                "password": "ge_tutorials",
                "database": "ge_tutorials",
            },
        ),
    )

    # Create desired config
    desired_stores_config = {
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        "expectations_database_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "drivername": "postgresql",
                    "host": "localhost",
                    "port": "65432",
                    "username": "ge_tutorials",
                    "password": "ge_tutorials",
                    "database": "ge_tutorials",
                },
            },
        },
        "validations_database_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "drivername": "postgresql",
                    "host": "localhost",
                    "port": "65432",
                    "username": "ge_tutorials",
                    "password": "ge_tutorials",
                    "database": "ge_tutorials",
                },
            },
        },
    }
    desired_data_docs_sites_config = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "store_backend": {
                "base_directory": "uncommitted/data_docs/local_site/",
                "class_name": "TupleFilesystemStoreBackend",
            },
        }
    }

    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources=default_pandas_datasource_config,
        expectations_store_name="expectations_database_store",
        validations_store_name="validations_database_store",
        evaluation_parameter_store_name=DataContextConfigDefaults.DEFAULT_EVALUATION_PARAMETER_STORE_NAME.value,
        stores=desired_stores_config,
        data_docs_sites=desired_data_docs_sites_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_DatabaseBackendEcosystem_using_all_parameters(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that DatabaseBackendEcosystem parameters are handled appropriately
    E.g. Make sure that default_credentials is ignored if individual store credentials are passed
    """

    data_context_config = DataContextConfig(
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
        backend_ecosystem=DatabaseBackendEcosystem(
            default_credentials={
                "drivername": "postgresql",
                "host": "localhost",
                "port": "65432",
                "username": "ge_tutorials",
                "password": "ge_tutorials",
                "database": "ge_tutorials",
            },
            expectations_store_credentials={
                "drivername": "custom_expectations_store_drivername",
                "host": "custom_expectations_store_host",
                "port": "custom_expectations_store_port",
                "username": "custom_expectations_store_username",
                "password": "custom_expectations_store_password",
                "database": "custom_expectations_store_database",
            },
            validations_store_credentials={
                "drivername": "custom_validations_store_drivername",
                "host": "custom_validations_store_host",
                "port": "custom_validations_store_port",
                "username": "custom_validations_store_username",
                "password": "custom_validations_store_password",
                "database": "custom_validations_store_database",
            },
            expectations_store_name="custom_expectations_database_store_name",
            validations_store_name="custom_validations_database_store_name",
            evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        ),
    )

    # Create desired config
    desired_stores_config = {
        "custom_evaluation_parameter_store_name": {
            "class_name": "EvaluationParameterStore"
        },
        "custom_expectations_database_store_name": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "database": "custom_expectations_store_database",
                    "drivername": "custom_expectations_store_drivername",
                    "host": "custom_expectations_store_host",
                    "password": "custom_expectations_store_password",
                    "port": "custom_expectations_store_port",
                    "username": "custom_expectations_store_username",
                },
            },
        },
        "custom_validations_database_store_name": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "DatabaseStoreBackend",
                "credentials": {
                    "database": "custom_validations_store_database",
                    "drivername": "custom_validations_store_drivername",
                    "host": "custom_validations_store_host",
                    "password": "custom_validations_store_password",
                    "port": "custom_validations_store_port",
                    "username": "custom_validations_store_username",
                },
            },
        },
    }
    desired_data_docs_sites_config = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "store_backend": {
                "base_directory": "uncommitted/data_docs/local_site/",
                "class_name": "TupleFilesystemStoreBackend",
            },
        }
    }

    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources=default_pandas_datasource_config,
        expectations_store_name="custom_expectations_database_store_name",
        validations_store_name="custom_validations_database_store_name",
        evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        stores=desired_stores_config,
        data_docs_sites=desired_data_docs_sites_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_override_general_defaults(
    construct_data_context_config,
    default_pandas_datasource_config,
    default_spark_datasource_config,
):
    """
    What does this test and why?
    A DataContextConfig should be able to be created by passing items into the constructor that override any defaults.
    It should also be able to handle multiple datasources, even if they are configured with a dictionary or a DatasourceConfig.
    """

    data_context_config = DataContextConfig(
        config_version=999,
        plugins_directory="custom_plugins_directory",
        config_variables_file_path="custom_config_variables_file_path",
        datasources={
            "my_spark_datasource": {
                "data_asset_type": {
                    "class_name": "SparkDFDataset",
                    "module_name": "great_expectations.dataset",
                },
                "class_name": "SparkDFDatasource",
                "module_name": "great_expectations.datasource",
                "batch_kwargs_generators": {},
            },
            "my_pandas_datasource": DatasourceConfig(
                class_name="PandasDatasource",
                batch_kwargs_generators={
                    "subdir_reader": {
                        "class_name": "SubdirReaderBatchKwargsGenerator",
                        "base_directory": "../data/",
                    }
                },
            ),
        },
        stores={
            "expectations_S3_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",  # TODO: replace with your value
                    "prefix": "REPLACE_ME",  # TODO: replace with your value
                },
            },
            "expectations_S3_store2": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",  # TODO: replace with your value
                    "prefix": "REPLACE_ME",  # TODO: replace with your value
                },
            },
            "validations_S3_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",  # TODO: replace with your value
                    "prefix": "REPLACE_ME",  # TODO: replace with your value
                },
            },
            "validations_S3_store2": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",  # TODO: replace with your value
                    "prefix": "REPLACE_ME",  # TODO: replace with your value
                },
            },
            "custom_evaluation_parameter_store": {
                "class_name": "EvaluationParameterStore"
            },
        },
        expectations_store_name="custom_expectations_store_name",
        validations_store_name="custom_validations_store_name",
        evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        data_docs_sites={
            "s3_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",  # TODO: replace with your value
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
            },
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                    "show_cta_footer": True,
                },
                "store_backend": {
                    "base_directory": "uncommitted/data_docs/local_site/",
                    "class_name": "TupleFilesystemStoreBackend",
                },
            },
        },
        validation_operators={
            "custom_action_list_operator": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "custom_store_validation_result",
                        "action": {"class_name": "CustomStoreValidationResultAction"},
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
        anonymous_usage_statistics={"enabled": True},
    )

    desired_stores = {
        "custom_evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        "expectations_S3_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "bucket": "REPLACE_ME",
                "class_name": "TupleS3StoreBackend",
                "prefix": "REPLACE_ME",
            },
        },
        "expectations_S3_store2": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "bucket": "REPLACE_ME",
                "class_name": "TupleS3StoreBackend",
                "prefix": "REPLACE_ME",
            },
        },
        "validations_S3_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "bucket": "REPLACE_ME",
                "class_name": "TupleS3StoreBackend",
                "prefix": "REPLACE_ME",
            },
        },
        "validations_S3_store2": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "bucket": "REPLACE_ME",
                "class_name": "TupleS3StoreBackend",
                "prefix": "REPLACE_ME",
            },
        },
    }

    desired_data_docs_sites_config = {
        "local_site": {
            "class_name": "SiteBuilder",
            "show_how_to_buttons": True,
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "store_backend": {
                "base_directory": "uncommitted/data_docs/local_site/",
                "class_name": "TupleFilesystemStoreBackend",
            },
        },
        "s3_site": {
            "class_name": "SiteBuilder",
            "site_index_builder": {
                "class_name": "DefaultSiteIndexBuilder",
                "show_cta_footer": True,
            },
            "store_backend": {
                "bucket": "REPLACE_ME",
                "class_name": "TupleS3StoreBackend",
            },
        },
    }
    desired_validation_operators = {
        "custom_action_list_operator": {
            "class_name": "ActionListValidationOperator",
            "action_list": [
                {
                    "name": "custom_store_validation_result",
                    "action": {"class_name": "CustomStoreValidationResultAction"},
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
    }

    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources={
            **default_pandas_datasource_config,
            **default_spark_datasource_config,
        },
        config_version=999.0,
        expectations_store_name="custom_expectations_store_name",
        validations_store_name="custom_validations_store_name",
        evaluation_parameter_store_name="custom_evaluation_parameter_store_name",
        stores=desired_stores,
        validation_operators=desired_validation_operators,
        data_docs_sites=desired_data_docs_sites_config,
    )
    desired_config["config_variables_file_path"] = "custom_config_variables_file_path"
    desired_config["plugins_directory"] = "custom_plugins_directory"

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


# TODO: Other tests:
#  1. Other backend ecosystems DONE
#  2. Overrides of BackendEcosystem work correctly (e.g. specifying an expectations_store_name in the constructor overrides
#  one specified in the BackendEcosystem, probably should either throw warning or error here) DONE
#  3. Overrides of general defaults work correctly e.g. validation_operators DONE
#  4. Also test if additional parameters are passed e.g. credentials for SqlAlchemyDatasource
#  5. What about substituted variables? Will they just work?
#  6. Test with other DataSources
#  7. Make sure these are all valid configs DataContext.validate_config(cls, project_config) DONE
#  8. Make sure multiple datasources and stores are handled DONE
