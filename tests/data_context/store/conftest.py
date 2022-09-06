import copy
from typing import Callable

import pytest

from great_expectations.core.serializer import JsonConfigSerializer
from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudRESTResource,
)
from great_expectations.data_context.types.base import datasourceConfigSchema, DatasourceConfig
from tests.data_context.conftest import MockResponse


@pytest.fixture
def datasource_store_ge_cloud_backend(
    ge_cloud_base_url: str,
    ge_cloud_access_token: str,
    ge_cloud_organization_id: str,
    datasource_store_name: str,
):
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
        store_name=datasource_store_name,
        store_backend=ge_cloud_store_backend_config,
        serializer=JsonConfigSerializer(schema=datasourceConfigSchema),
    )
    return store


@pytest.fixture
def datasource_id() -> str:
    return "aaa7cfdd-4aa4-4f3d-a979-fe2ea5203cbf"


@pytest.fixture
def mocked_post_response(
    mock_response_factory: Callable, datasource_id: str
) -> Callable[[], MockResponse]:

    def _mocked_post_response(*args, **kwargs):
        return mock_response_factory(
            {
                "data": {
                    "id": datasource_id,
                }
            },
            201,
        )

    return _mocked_post_response


@pytest.fixture
def datasource_config_with_names_and_ids(datasource_config_with_names: DatasourceConfig, datasource_id: str) -> DatasourceConfig:
    updated_config = copy.deepcopy(datasource_config_with_names)
    updated_config["id"] = datasource_id
    return updated_config


@pytest.fixture
def mocked_get_response(
    mock_response_factory: Callable,
    datasource_config_with_names_and_ids: DatasourceConfig,
    datasource_id: str,
) -> Callable[[], MockResponse]:
    def _mocked_get_response(*args, **kwargs):
        created_by_id = "c06ac6a2-52e0-431e-b878-9df624edc8b8"
        organization_id = "046fe9bc-c85b-4e95-b1af-e4ce36ba5384"

        return mock_response_factory(
            {
                "data": {
                    "attributes": {
                        "datasource_config": datasource_config_with_names_and_ids,
                        "created_at": "2022-08-02T17:55:45.107550",
                        "created_by_id": created_by_id,
                        "deleted": False,
                        "deleted_at": None,
                        "desc": None,
                        "name": "my_datasource",
                        "organization_id": f"{organization_id}",
                        "updated_at": "2022-08-02T17:55:45.107550",
                    },
                    "id": datasource_id,
                    "links": {
                        "self": f"/organizations/{organization_id}/datasources/{datasource_id}"
                    },
                    "type": "datasource",
                },
            },
            200,
        )

    return _mocked_get_response
