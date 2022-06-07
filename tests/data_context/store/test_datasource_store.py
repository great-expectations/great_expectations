from unittest.mock import PropertyMock, patch

import pytest

from great_expectations.core.data_context_key import DataContextVariableKey, StringKey
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.types.base import DatasourceConfig
from great_expectations.data_context.types.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier


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
    return DatasourceStore(store_name=datasource_store_name)


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


def test_datasource_store_with_bad_key_raises_error(
    empty_datasource_store: DatasourceStore, datasource_config: DatasourceConfig
) -> None:
    store: DatasourceStore = empty_datasource_store

    error_msg: str = "key must be an instance of DataContextVariableKey"

    with pytest.raises(TypeError) as e:
        store.set(key="my_bad_key", value=datasource_config)
    assert error_msg in str(e.value)

    with pytest.raises(TypeError) as e:
        store.get(key="my_bad_key")
    assert error_msg in str(e.value)


def test_datasource_store_retrieval(
    empty_datasource_store: DatasourceStore, datasource_config: DatasourceConfig
) -> None:
    store: DatasourceStore = empty_datasource_store

    key: DataContextVariableKey = DataContextVariableKey(
        resource_type=DataContextVariableSchema.DATASOURCES,
        resource_name="my_datasource",
    )
    store.set(key=key, value=datasource_config)
    res: DatasourceConfig = store.get(key=key)

    assert isinstance(res, DatasourceConfig)
    assert res.to_json_dict() == datasource_config.to_json_dict()


def test_datasource_store_retrieval_cloud_mode(
    datasource_config: DatasourceConfig,
    ge_cloud_base_url: str,
    ge_cloud_access_token: str,
    ge_cloud_organization_id: str,
) -> None:
    ge_cloud_store_backend_config: dict = {
        "class_name": "GeCloudStoreBackend",
        "ge_cloud_base_url": ge_cloud_base_url,
        "ge_cloud_resource_type": "datasource",
        "ge_cloud_credentials": {
            "access_token": ge_cloud_access_token,
            "organization_id": ge_cloud_organization_id,
        },
        "suppress_store_backend_id": True,
    }

    store: DatasourceStore = DatasourceStore(
        store_name="my_cloud_datasource_store",
        store_backend=ge_cloud_store_backend_config,
    )

    key: GeCloudIdentifier = GeCloudIdentifier(
        resource_type="datasource", ge_cloud_id="foobarbaz"
    )

    with patch("requests.patch", autospec=True) as mock_patch:
        type(mock_patch.return_value).status_code = PropertyMock(return_value=200)

        store.set(key=key, value=datasource_config)

        mock_patch.assert_called_with(
            "https://app.test.greatexpectations.io/organizations/bd20fead-2c31-4392-bcd1-f1e87ad5a79c/datasources/foobarbaz",
            json={
                "data": {
                    "type": "datasource",
                    "id": "foobarbaz",
                    "attributes": {
                        "datasource_config": datasource_config.to_dict(),
                        "organization_id": ge_cloud_organization_id,
                    },
                }
            },
            headers={
                "Content-Type": "application/vnd.api+json",
                "Authorization": "Bearer 6bb5b6f5c7794892a4ca168c65c2603e",
            },
        )


def test_datasource_store_with_inline_store_backend(
    datasource_config: DatasourceConfig, empty_data_context: DataContext
) -> None:
    inline_store_backend_config: dict = {
        "class_name": "InlineStoreBackend",
        "data_context": empty_data_context,
        "suppress_store_backend_id": True,
    }

    store: DatasourceStore = DatasourceStore(
        store_name="my_datasource_store",
        store_backend=inline_store_backend_config,
    )

    key: DataContextVariableKey = DataContextVariableKey(
        resource_type=DataContextVariableSchema.DATASOURCES,
        resource_name="my_datasource",
    )

    store.set(key=key, value=datasource_config)
    res: DatasourceConfig = store.get(key=key)

    assert isinstance(res, DatasourceConfig)
    assert res.to_json_dict() == datasource_config.to_json_dict()
