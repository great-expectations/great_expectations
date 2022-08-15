import copy
from typing import cast
from unittest.mock import PropertyMock, patch

import pytest

from great_expectations.core.data_context_key import DataContextVariableKey
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.data_context_variables import (
    DataContextVariableSchema,
)
from great_expectations.data_context.store.datasource_store import DatasourceStore
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import (
    DatasourceConfig,
    datasourceConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import GeCloudIdentifier


@pytest.fixture
def empty_datasource_store(datasource_store_name: str) -> DatasourceStore:
    return DatasourceStore(store_name=datasource_store_name)


@pytest.fixture
def datasource_store_with_single_datasource(
    datasource_name: str,
    datasource_config: DatasourceConfig,
    empty_datasource_store: DatasourceStore,
) -> DatasourceStore:
    key = DataContextVariableKey(
        resource_name=datasource_name,
    )
    empty_datasource_store.set(key=key, value=datasource_config)
    return empty_datasource_store


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

    key = DataContextVariableKey(
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
    shared_called_with_request_kwargs: dict,
) -> None:
    ge_cloud_store_backend_config: dict = {
        "class_name": "GeCloudStoreBackend",
        "ge_cloud_base_url": ge_cloud_base_url,
        "ge_cloud_resource_type": GeCloudRESTResource.DATASOURCE,
        "ge_cloud_credentials": {
            "access_token": ge_cloud_access_token,
            "organization_id": ge_cloud_organization_id,
        },
        "suppress_store_backend_id": True,
    }

    store = DatasourceStore(
        store_name="my_cloud_datasource_store",
        store_backend=ge_cloud_store_backend_config,
    )

    key = GeCloudIdentifier(
        resource_type=GeCloudRESTResource.DATASOURCE, ge_cloud_id="foobarbaz"
    )

    with patch("requests.put", autospec=True) as mock_put:
        type(mock_put.return_value).status_code = PropertyMock(return_value=200)

        store.set(key=key, value=datasource_config)

        expected_datasource_config = datasourceConfigSchema.dump(datasource_config)

        mock_put.assert_called_with(
            "https://app.test.greatexpectations.io/organizations/bd20fead-2c31-4392-bcd1-f1e87ad5a79c/datasources/foobarbaz",
            json={
                "data": {
                    "type": "datasource",
                    "id": "foobarbaz",
                    "attributes": {
                        "datasource_config": expected_datasource_config,
                        "organization_id": ge_cloud_organization_id,
                    },
                }
            },
            **shared_called_with_request_kwargs,
        )


def test_datasource_store_with_inline_store_backend(
    datasource_config: DatasourceConfig, empty_data_context: DataContext
) -> None:
    inline_store_backend_config: dict = {
        "class_name": "InlineStoreBackend",
        "resource_type": DataContextVariableSchema.DATASOURCES,
        "data_context": empty_data_context,
        "suppress_store_backend_id": True,
    }

    store = DatasourceStore(
        store_name="my_datasource_store",
        store_backend=inline_store_backend_config,
    )

    key = DataContextVariableKey(
        resource_name="my_datasource",
    )

    store.set(key=key, value=datasource_config)
    res: DatasourceConfig = store.get(key=key)

    assert isinstance(res, DatasourceConfig)
    assert res.to_json_dict() == datasource_config.to_json_dict()


def test_datasource_store_set_by_name(
    empty_datasource_store: DatasourceStore,
    datasource_config: DatasourceConfig,
    datasource_name: str,
) -> None:
    assert len(empty_datasource_store.list_keys()) == 0

    empty_datasource_store.set_by_name(
        datasource_name=datasource_name, datasource_config=datasource_config
    )

    assert len(empty_datasource_store.list_keys()) == 1


def test_datasource_store_retrieve_by_name(
    datasource_name: str,
    datasource_config: DatasourceConfig,
    datasource_store_with_single_datasource: DatasourceStore,
) -> None:
    actual_config: DatasourceConfig = (
        datasource_store_with_single_datasource.retrieve_by_name(
            datasource_name=datasource_name
        )
    )
    assert datasource_config.to_dict() == actual_config.to_dict()


def test_datasource_store_delete_by_name(
    datasource_name: str,
    datasource_store_with_single_datasource: DatasourceStore,
) -> None:
    assert len(datasource_store_with_single_datasource.list_keys()) == 1

    datasource_store_with_single_datasource.delete_by_name(
        datasource_name=datasource_name
    )

    assert len(datasource_store_with_single_datasource.list_keys()) == 0


def test_datasource_store_update_by_name(
    datasource_name: str,
    datasource_config: DatasourceConfig,
    datasource_store_with_single_datasource: DatasourceStore,
) -> None:
    updated_base_directory: str = "foo/bar/baz"

    updated_datasource_config = copy.deepcopy(datasource_config)
    updated_datasource_config.data_connectors["tripdata_monthly_configured"][
        "base_directory"
    ] = updated_base_directory

    datasource_store_with_single_datasource.update_by_name(
        datasource_name=datasource_name, datasource_config=updated_datasource_config
    )

    key = DataContextVariableKey(
        resource_name=datasource_name,
    )
    actual_config = cast(
        DatasourceConfig, datasource_store_with_single_datasource.get(key=key)
    )

    assert actual_config.to_dict() == updated_datasource_config.to_dict()


def test_datasource_store_update_raises_error_if_datasource_doesnt_exist(
    datasource_name: str,
    empty_datasource_store: DatasourceStore,
) -> None:
    updated_datasource_config = DatasourceConfig()
    with pytest.raises(ValueError) as e:
        empty_datasource_store.update_by_name(
            datasource_name=datasource_name, datasource_config=updated_datasource_config
        )

    assert f"Unable to load datasource `{datasource_name}`" in str(e.value)
