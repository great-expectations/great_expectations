import great_expectations as gx
import pytest
import os

from great_expectations.data_context import CloudDataContext
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.store import GXCloudStoreBackend
from great_expectations.data_context.store.gx_cloud_store_backend import EndpointVersion


class V1GetContextError(Exception):
    pass


@pytest.fixture()
def set_v1_get_context_endpoint():
    previous = GXCloudStoreBackend._ENDPOINT_VERSION_LOOKUP[GXCloudRESTResource.DATA_CONTEXT]
    if previous == EndpointVersion.V1:
        raise V1GetContextError("We no longer need the set_v1_get_context_endpoint fixture, please remove.")
    GXCloudStoreBackend._ENDPOINT_VERSION_LOOKUP[GXCloudRESTResource.DATA_CONTEXT] = EndpointVersion.V1
    yield
    GXCloudStoreBackend._ENDPOINT_VERSION_LOOKUP[GXCloudRESTResource.DATA_CONTEXT] = previous


@pytest.mark.cloud
def test_get_context(set_v1_get_context_endpoint: None):
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    # This assert is to ensure we are hitting the v1 and not the v0 endpoint.
    # We do this by asserting the config version is above anything that will appear in v0.
    # assert context.config_version >= 4.0
