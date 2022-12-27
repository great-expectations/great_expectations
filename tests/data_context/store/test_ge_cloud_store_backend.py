import pytest

from great_expectations.data_context.cloud_constants import (
    CLOUD_DEFAULT_BASE_URL,
    GXCloudRESTResource,
)
from great_expectations.data_context.store.ge_cloud_store_backend import (
    GeCloudStoreBackend,
)
from great_expectations.data_context.store.gx_cloud_store_backend import (
    GXCloudStoreBackend,
)


@pytest.mark.cloud
@pytest.mark.unit
def test_ge_cloud_store_backend_is_alias_of_gx_cloud_store_backend(
    ge_cloud_access_token: str,
) -> None:
    ge_cloud_base_url = CLOUD_DEFAULT_BASE_URL
    ge_cloud_credentials = {
        "access_token": ge_cloud_access_token,
        "organization_id": "51379b8b-86d3-4fe7-84e9-e1a52f4a414c",
    }

    backend = GeCloudStoreBackend(
        ge_cloud_base_url=ge_cloud_base_url,
        ge_cloud_credentials=ge_cloud_credentials,
        ge_cloud_resource_type=GXCloudRESTResource.CHECKPOINT,
    )

    assert isinstance(backend, GXCloudStoreBackend)
