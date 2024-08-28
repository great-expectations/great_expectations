import os

import pytest

import great_expectations as gx
from great_expectations.data_context import CloudDataContext


class V1GetContextError(Exception):
    pass


@pytest.mark.cloud
@pytest.mark.parametrize("analytics_enabled", [True, False])
def test_get_context(monkeypatch, analytics_enabled):
    from great_expectations.analytics.config import ENV_CONFIG

    monkeypatch.setattr(ENV_CONFIG, "gx_analytics_enabled", analytics_enabled)
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("GX_CLOUD_BASE_URL"),
        cloud_organization_id=os.environ.get("GX_CLOUD_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    # This assert is to ensure we are hitting the v1 and not the v0 endpoint.
    assert context.config.config_version == 4.0
