import pytest

from great_expectations.data_context.data_context.base_data_context import (
    BaseDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.types.base import GXCloudConfig
from great_expectations.util import _resolve_cloud_args

# Globally scoped so we can reuse across test parameterization
cloud_base_url = "my_cloud_url"
cloud_access_token = "my_cloud_access_token"
cloud_organization_id = "my_cloud_organization_id"
cloud_config = GXCloudConfig(
    base_url=cloud_base_url,
    access_token=cloud_access_token,
    organization_id=cloud_organization_id,
)
ge_cloud_base_url = "my_ge_cloud_url"
ge_cloud_access_token = "my_ge_cloud_access_token"
ge_cloud_organization_id = "my_ge_cloud_organization_id"
ge_cloud_config = GXCloudConfig(
    base_url=ge_cloud_base_url,
    access_token=ge_cloud_access_token,
    organization_id=ge_cloud_organization_id,
)


@pytest.mark.unit
@pytest.mark.cloud
@pytest.mark.parametrize(
    "cloud_args,expected_resolved_args",
    [
        pytest.param(
            {"cloud_mode": True, "cloud_config": cloud_config},
            (True, cloud_config),
            id="new_style_args",
        ),
        pytest.param(
            {"ge_cloud_mode": True, "ge_cloud_config": ge_cloud_config},
            (True, ge_cloud_config),
            id="deprecated_args",
        ),
        pytest.param(
            {
                "cloud_mode": True,
                "ge_cloud_mode": True,
                "cloud_config": cloud_config,
                "ge_cloud_config": ge_cloud_config,
            },
            (True, cloud_config),
            id="conflicting_args",
        ),
    ],
)
def test_BaseDataContext_resolve_cloud_args(
    cloud_args: dict, expected_resolved_args: tuple
):
    actual_resolved_args = BaseDataContext._resolve_cloud_args(**cloud_args)
    assert actual_resolved_args == expected_resolved_args


@pytest.mark.unit
@pytest.mark.cloud
@pytest.mark.parametrize(
    "cloud_args,expected_resolved_args",
    [
        pytest.param(
            {
                "cloud_mode": True,
                "cloud_base_url": cloud_base_url,
                "cloud_access_token": cloud_access_token,
                "cloud_organization_id": cloud_organization_id,
            },
            (cloud_base_url, cloud_access_token, cloud_organization_id, True),
            id="new_style_args",
        ),
        pytest.param(
            {
                "ge_cloud_mode": True,
                "ge_cloud_base_url": ge_cloud_base_url,
                "ge_cloud_access_token": ge_cloud_access_token,
                "ge_cloud_organization_id": ge_cloud_organization_id,
            },
            (ge_cloud_base_url, ge_cloud_access_token, ge_cloud_organization_id, True),
            id="deprecated_args",
        ),
        pytest.param(
            {
                "cloud_mode": True,
                "cloud_base_url": cloud_base_url,
                "cloud_access_token": cloud_access_token,
                "cloud_organization_id": cloud_organization_id,
                "ge_cloud_mode": True,
                "ge_cloud_base_url": ge_cloud_base_url,
                "ge_cloud_access_token": ge_cloud_access_token,
                "ge_cloud_organization_id": ge_cloud_organization_id,
            },
            (cloud_base_url, cloud_access_token, cloud_organization_id, True),
            id="conflicting_args",
        ),
    ],
)
def test_DataContext_resolve_cloud_args(
    cloud_args: dict, expected_resolved_args: tuple
):
    actual_resolved_args = DataContext._resolve_cloud_args(**cloud_args)
    assert actual_resolved_args == expected_resolved_args


@pytest.mark.unit
@pytest.mark.cloud
@pytest.mark.parametrize(
    "cloud_args,expected_resolved_args",
    [
        pytest.param(
            {
                "cloud_base_url": cloud_base_url,
                "cloud_access_token": cloud_access_token,
                "cloud_organization_id": cloud_organization_id,
            },
            (cloud_base_url, cloud_access_token, cloud_organization_id),
            id="new_style_args",
        ),
        pytest.param(
            {
                "ge_cloud_base_url": ge_cloud_base_url,
                "ge_cloud_access_token": ge_cloud_access_token,
                "ge_cloud_organization_id": ge_cloud_organization_id,
            },
            (ge_cloud_base_url, ge_cloud_access_token, ge_cloud_organization_id),
            id="deprecated_args",
        ),
        pytest.param(
            {
                "cloud_base_url": cloud_base_url,
                "cloud_access_token": cloud_access_token,
                "cloud_organization_id": cloud_organization_id,
                "ge_cloud_base_url": ge_cloud_base_url,
                "ge_cloud_access_token": ge_cloud_access_token,
                "ge_cloud_organization_id": ge_cloud_organization_id,
            },
            (cloud_base_url, cloud_access_token, cloud_organization_id),
            id="conflicting_args",
        ),
    ],
)
def test_CloudDataContext_resolve_cloud_args(
    cloud_args: dict, expected_resolved_args: tuple
):
    actual_resolved_args = CloudDataContext._resolve_cloud_args(**cloud_args)
    assert actual_resolved_args == expected_resolved_args


@pytest.mark.unit
@pytest.mark.cloud
@pytest.mark.parametrize(
    "cloud_args,expected_resolved_args",
    [
        pytest.param(
            {
                "cloud_mode": True,
                "cloud_base_url": cloud_base_url,
                "cloud_access_token": cloud_access_token,
                "cloud_organization_id": cloud_organization_id,
            },
            (cloud_base_url, cloud_access_token, cloud_organization_id, True),
            id="new_style_args",
        ),
        pytest.param(
            {
                "ge_cloud_mode": True,
                "ge_cloud_base_url": ge_cloud_base_url,
                "ge_cloud_access_token": ge_cloud_access_token,
                "ge_cloud_organization_id": ge_cloud_organization_id,
            },
            (ge_cloud_base_url, ge_cloud_access_token, ge_cloud_organization_id, True),
            id="deprecated_args",
        ),
        pytest.param(
            {
                "cloud_mode": True,
                "cloud_base_url": cloud_base_url,
                "cloud_access_token": cloud_access_token,
                "cloud_organization_id": cloud_organization_id,
                "ge_cloud_mode": True,
                "ge_cloud_base_url": ge_cloud_base_url,
                "ge_cloud_access_token": ge_cloud_access_token,
                "ge_cloud_organization_id": ge_cloud_organization_id,
            },
            (cloud_base_url, cloud_access_token, cloud_organization_id, True),
            id="conflicting_args",
        ),
        # The interaction between cloud_mode and ge_cloud_mode is very particular so we take extra care testing here:
        pytest.param(
            {
                "cloud_mode": True,
                "ge_cloud_mode": False,
            },
            (None, None, None, True),
            id="cloud_mode=True_overrides_ge_cloud_mode",
        ),
        pytest.param(
            {
                "cloud_mode": False,
                "ge_cloud_mode": True,
            },
            (None, None, None, False),
            id="cloud_mode=False_overrides_ge_cloud_mode",
        ),
        pytest.param(
            {
                "cloud_mode": None,
                "ge_cloud_mode": False,
            },
            (None, None, None, False),
            id="ge_cloud_mode=False_overrides_cloud_mode=None",
        ),
        pytest.param(
            {
                "cloud_mode": None,
                "ge_cloud_mode": True,
            },
            (None, None, None, True),
            id="ge_cloud_mode=True_overrides_cloud_mode=None",
        ),
    ],
)
def test_get_context_resolve_cloud_args(
    cloud_args: dict, expected_resolved_args: tuple
):
    actual_resolved_args = _resolve_cloud_args(**cloud_args)
    assert actual_resolved_args == expected_resolved_args
