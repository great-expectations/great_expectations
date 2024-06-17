from typing import Dict

import pytest

from great_expectations.core.config_provider import _CloudConfigurationProvider
from great_expectations.data_context.cloud_constants import GXCloudEnvironmentVariable
from great_expectations.data_context.types.base import GXCloudConfig

# Randomly generated values but formatted to represent actual creds
# Globally scoped so they can be shared across pytest.params
base_url = "https://api.testing.greatexpectations.io/"
access_token = "1009d2fe-54b3-43b8-8297-ba2d517f9752"
organization_id = "0dcf5ce1-806f-4199-9e69-e24dfba5e62a"


@pytest.mark.cloud
@pytest.mark.parametrize(
    "cloud_config,expected_values",
    [
        pytest.param(
            GXCloudConfig(
                base_url=base_url,
                access_token=access_token,
                organization_id=organization_id,
            ),
            {
                GXCloudEnvironmentVariable.BASE_URL: base_url,
                GXCloudEnvironmentVariable.ACCESS_TOKEN: access_token,
                GXCloudEnvironmentVariable.ORGANIZATION_ID: organization_id,
            },
            id="include_org_id",
        ),
        pytest.param(
            GXCloudConfig(base_url=base_url, access_token=access_token),
            {
                GXCloudEnvironmentVariable.BASE_URL: base_url,
                GXCloudEnvironmentVariable.ACCESS_TOKEN: access_token,
            },
            id="omit_org_id",
        ),
    ],
)
def test_CloudConfigurationProvider_get_values(
    cloud_config: GXCloudConfig, expected_values: Dict[str, str]
):
    provider = _CloudConfigurationProvider(cloud_config)
    assert provider.get_values() == expected_values
