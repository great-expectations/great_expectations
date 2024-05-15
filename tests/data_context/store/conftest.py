import pytest

from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import DatasourceStore
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)


@pytest.fixture
def datasource_store_ge_cloud_backend(
    ge_cloud_base_url: str,
    ge_cloud_access_token: str,
    ge_cloud_organization_id: str,
    datasource_store_name: str,
):
    ge_cloud_store_backend_config: dict = {
        "class_name": GXCloudStoreBackend.__name__,
        "ge_cloud_base_url": ge_cloud_base_url,
        "ge_cloud_resource_type": GXCloudRESTResource.DATASOURCE,
        "ge_cloud_credentials": {
            "access_token": ge_cloud_access_token,
            "organization_id": ge_cloud_organization_id,
        },
        "suppress_store_backend_id": True,
    }

    store = DatasourceStore(
        store_name=datasource_store_name,
        store_backend=ge_cloud_store_backend_config,
    )
    return store
