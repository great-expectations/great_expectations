import pytest

from great_expectations.data_context.types.base import (
    DatasourceConfig,
    DatasourceConfigSchema,
)
from tests.data_context.test_data_context_config_ui import (
    default_pandas_datasource_config,
    default_spark_datasource_config,
)


@pytest.fixture()
def default_sql_alchemy_datasource_config():
    return {
        "my_sql_alchemy_datasource": {
            "class_name": "SqlAlchemyDatasource",
            "data_asset_type": {
                "class_name": "SqlAlchemyDataset",
                "module_name": "great_expectations.dataset",
            },
            "module_name": "great_expectations.datasource",
            "credentials": {
                "drivername": "custom_drivername",
                "host": "custom_host",
                "port": "custom_port",
                "username": "custom_username",
                "password": "custom_password",
                "database": "custom_database",
            },
        }
    }


def test_PandasDatasource_config(default_pandas_datasource_config):

    datasource_config = DatasourceConfig(
        class_name="PandasDatasource",
        batch_kwargs_generators={
            "subdir_reader": {
                "class_name": "SubdirReaderBatchKwargsGenerator",
                "base_directory": "../data/",
            }
        },
    )

    desired_config = default_pandas_datasource_config["my_pandas_datasource"]

    datasource_config_schema = DatasourceConfigSchema()
    assert datasource_config_schema.dump(datasource_config) == desired_config


def test_SqlAlchemyDatasource_config(default_sql_alchemy_datasource_config):

    datasource_config = DatasourceConfig(
        class_name="SqlAlchemyDatasource",
        credentials={
            "drivername": "custom_drivername",
            "host": "custom_host",
            "port": "custom_port",
            "username": "custom_username",
            "password": "custom_password",
            "database": "custom_database",
        },
    )

    desired_config = default_sql_alchemy_datasource_config["my_sql_alchemy_datasource"]

    datasource_config_schema = DatasourceConfigSchema()
    assert datasource_config_schema.dump(datasource_config) == desired_config


def test_SparkDatasource_config(default_spark_datasource_config):

    datasource_config = DatasourceConfig(
        class_name="SparkDFDatasource",
        batch_kwargs_generators={},
    )

    desired_config = default_spark_datasource_config["my_spark_datasource"]

    datasource_config_schema = DatasourceConfigSchema()
    assert datasource_config_schema.dump(datasource_config) == desired_config
