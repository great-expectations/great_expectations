import pytest

from great_expectations.core.data_context_key import StringKey
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.types.base import DatasourceConfig


@pytest.fixture
def datasource_name() -> str:
    return "my_first_datasource"


@pytest.fixture
def datasource_store_name() -> str:
    return "datasource_store"


@pytest.fixture
def datasource_key(datasource_name: str) -> StringKey:
    return StringKey(key=datasource_name)


@pytest.fixture
def empty_datasource_store(datasource_store_name: str) -> DatasourceStore:
    return DatasourceStore(datasource_store_name)


@pytest.fixture
def datasource_config() -> DatasourceConfig:
    return DatasourceConfig(
        class_name="Datasource",
        execution_engine={"class_name": "PandasExecutionEngine"},
        data_connectors={
            "tripdata_monthly_configured": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": "/path/to/trip_data",
                "assets": {
                    "yellow": {
                        "pattern": r"yellow_tripdata_(\d{4})-(\d{2})\.csv$",
                        "group_names": ["year", "month"],
                    }
                },
            }
        },
    )
