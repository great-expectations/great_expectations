import pytest

from great_expectations.data_context.types.base import DatasourceConfig


@pytest.fixture
def datasource_name() -> str:
    return "my_first_datasource"


@pytest.fixture
def datasource_store_name() -> str:
    return "datasource_store"


@pytest.fixture
def datasource_config() -> DatasourceConfig:
    return DatasourceConfig(
        class_name="Datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        data_connectors={
            "tripdata_monthly_configured": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "base_directory": "/path/to/trip_data",
                "assets": {
                    "yellow": {
                        "class_name": "Asset",
                        "module_name": "great_expectations.datasource.data_connector.asset",
                        "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv$",
                        "group_names": ["year", "month"],
                    }
                },
            }
        },
    )


@pytest.fixture
def datasource_config_with_id() -> DatasourceConfig:
    return DatasourceConfig(
        class_name="Datasource",
        execution_engine={
            "class_name": "PandasExecutionEngine",
            "module_name": "great_expectations.execution_engine",
        },
        id="foobarbaz",
        data_connectors={
            "tripdata_monthly_configured": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "base_directory": "/path/to/trip_data",
                "assets": {
                    "yellow": {
                        "class_name": "Asset",
                        "module_name": "great_expectations.datasource.data_connector.asset",
                        "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv$",
                        "group_names": ["year", "month"],
                    }
                },
            }
        },
    )
