import copy
from typing import Dict, Optional

import pytest

from great_expectations import DataContext
from great_expectations.data_context import BaseDataContext
from great_expectations.data_context.types.base import (
    BaseStoreBackendDefaults,
    DatabaseStoreBackendDefaults,
    DataContextConfig,
    DataContextConfigDefaults,
    DataContextConfigSchema,
    DatasourceConfig,
    FilesystemStoreBackendDefaults,
    GCSStoreBackendDefaults,
    S3StoreBackendDefaults,
)

"""
What does this test and why?

This file will hold various tests to ensure that the UI functions as expected when creating a DataContextConfig object. It will ensure that the appropriate defaults are used, including when the store_backend_defaults parameter is set.
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
        plugins_directory: Optional[str] = None,
        stores: Optional[Dict] = None,
        validation_operators: Optional[Dict] = None,
        data_docs_sites: Optional[Dict] = None,
    ):
        if stores is None:
            stores = copy.deepcopy(DataContextConfigDefaults.DEFAULT_STORES.value)
        if validation_operators is None:
            validation_operators = copy.deepcopy(
                DataContextConfigDefaults.DEFAULT_VALIDATION_OPERATORS.value
            )
        if data_docs_sites is None:
            data_docs_sites = copy.deepcopy(
                DataContextConfigDefaults.DEFAULT_DATA_DOCS_SITES.value
            )

        return {
            "config_version": config_version,
            "datasources": datasources,
            "expectations_store_name": expectations_store_name,
            "validations_store_name": validations_store_name,
            "evaluation_parameter_store_name": evaluation_parameter_store_name,
            "plugins_directory": plugins_directory,
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


def test_DataContextConfig_with_BaseStoreBackendDefaults_and_simple_defaults(
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
        store_backend_defaults=BaseStoreBackendDefaults(),
    )

    desired_config = construct_data_context_config(
        data_context_id=data_context_config.anonymous_usage_statistics.data_context_id,
        datasources=default_pandas_datasource_config,
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_S3StoreBackendDefaults(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that using S3StoreBackendDefaults as the store_backend_defaults applies appropriate
    defaults, including default_bucket_name getting propagated to all stores.
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
        store_backend_defaults=S3StoreBackendDefaults(
            default_bucket_name="my_default_bucket"
        ),
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


def test_DataContextConfig_with_S3StoreBackendDefaults_using_all_parameters(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that S3StoreBackendDefaults parameters are handled appropriately
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
        store_backend_defaults=S3StoreBackendDefaults(
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


def test_DataContextConfig_with_FilesystemStoreBackendDefaults_and_simple_defaults(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Ensure that a very simple DataContextConfig setup using FilesystemStoreBackendDefaults is created accurately
    This test sets the root_dir parameter
    """

    test_root_directory = "test_root_dir"

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
        store_backend_defaults=FilesystemStoreBackendDefaults(
            root_directory=test_root_directory
        ),
    )

    # Create desired config
    data_context_id = data_context_config.anonymous_usage_statistics.data_context_id
    desired_config = construct_data_context_config(
        data_context_id=data_context_id, datasources=default_pandas_datasource_config
    )
    # Add root_directory to stores and data_docs
    desired_config["stores"][desired_config["expectations_store_name"]][
        "store_backend"
    ]["root_directory"] = test_root_directory
    desired_config["stores"][desired_config["validations_store_name"]]["store_backend"][
        "root_directory"
    ] = test_root_directory
    desired_config["data_docs_sites"]["local_site"]["store_backend"][
        "root_directory"
    ] = test_root_directory

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_FilesystemStoreBackendDefaults_and_simple_defaults_no_root_directory(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Ensure that a very simple DataContextConfig setup using FilesystemStoreBackendDefaults is created accurately
    This test does not set the optional root_directory parameter
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
        store_backend_defaults=FilesystemStoreBackendDefaults(),
    )

    # Create desired config
    data_context_id = data_context_config.anonymous_usage_statistics.data_context_id
    desired_config = construct_data_context_config(
        data_context_id=data_context_id, datasources=default_pandas_datasource_config
    )

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_GCSStoreBackendDefaults(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that using GCSStoreBackendDefaults as the store_backend_defaults applies appropriate
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
        store_backend_defaults=GCSStoreBackendDefaults(
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


def test_DataContextConfig_with_GCSStoreBackendDefaults_using_all_parameters(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that GCSStoreBackendDefaults parameters are handled appropriately
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
        store_backend_defaults=GCSStoreBackendDefaults(
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


def test_DataContextConfig_with_DatabaseStoreBackendDefaults(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that using DatabaseStoreBackendDefaults as the store_backend_defaults applies appropriate
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
        store_backend_defaults=DatabaseStoreBackendDefaults(
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


def test_DataContextConfig_with_DatabaseStoreBackendDefaults_using_all_parameters(
    construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Make sure that DatabaseStoreBackendDefaults parameters are handled appropriately
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
        store_backend_defaults=DatabaseStoreBackendDefaults(
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
                    "bucket": "REPLACE_ME",
                    "prefix": "REPLACE_ME",
                },
            },
            "expectations_S3_store2": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",
                    "prefix": "REPLACE_ME",
                },
            },
            "validations_S3_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",
                    "prefix": "REPLACE_ME",
                },
            },
            "validations_S3_store2": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": "REPLACE_ME",
                    "prefix": "REPLACE_ME",
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
                    "bucket": "REPLACE_ME",
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            },
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
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
        plugins_directory="custom_plugins_directory",
    )
    desired_config["config_variables_file_path"] = "custom_config_variables_file_path"

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)


def test_DataContextConfig_with_S3StoreBackendDefaults_and_simple_defaults_with_variable_sub(
    monkeypatch, construct_data_context_config, default_pandas_datasource_config
):
    """
    What does this test and why?
    Ensure that a very simple DataContextConfig setup with many defaults is created accurately
    and produces a valid DataContextConfig
    """

    monkeypatch.setenv("SUBSTITUTED_BASE_DIRECTORY", "../data/")

    data_context_config = DataContextConfig(
        datasources={
            "my_pandas_datasource": DatasourceConfig(
                class_name="PandasDatasource",
                batch_kwargs_generators={
                    "subdir_reader": {
                        "class_name": "SubdirReaderBatchKwargsGenerator",
                        "base_directory": "${SUBSTITUTED_BASE_DIRECTORY}",
                    }
                },
            )
        },
        store_backend_defaults=S3StoreBackendDefaults(
            default_bucket_name="my_default_bucket"
        ),
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

    desired_config["datasources"]["my_pandas_datasource"]["batch_kwargs_generators"][
        "subdir_reader"
    ]["base_directory"] = "${SUBSTITUTED_BASE_DIRECTORY}"

    data_context_config_schema = DataContextConfigSchema()
    assert data_context_config_schema.dump(data_context_config) == desired_config
    assert DataContext.validate_config(project_config=data_context_config)

    data_context = BaseDataContext(project_config=data_context_config)
    assert (
        data_context.datasources["my_pandas_datasource"]
        .get_batch_kwargs_generator("subdir_reader")
        ._base_directory
        == "../data/"
    )
