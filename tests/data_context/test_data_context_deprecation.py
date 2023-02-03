from __future__ import annotations

from unittest import mock

import pytest

from great_expectations.data_context.data_context import (
    AbstractDataContext,
    CloudDataContext,
)
from great_expectations.data_context.data_context.base_data_context import (
    _resolve_cloud_args as base_data_context_resolver,
)
from great_expectations.data_context.data_context.data_context import (
    _resolve_cloud_args as data_context_resolver,
)
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from great_expectations.data_context.types.base import GXCloudConfig
from great_expectations.util import _resolve_cloud_args as get_context_resolver

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
    actual_resolved_args = base_data_context_resolver(**cloud_args)
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
    actual_resolved_args = data_context_resolver(**cloud_args)
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
    actual_resolved_args = get_context_resolver(**cloud_args)
    assert actual_resolved_args == expected_resolved_args


@pytest.mark.unit
@pytest.mark.cloud
@pytest.mark.parametrize(
    "id, ge_cloud_id, expected",
    [
        pytest.param("abc123", None, "abc123", id="only id"),
        pytest.param(None, "def456", "def456", id="only ge_cloud_id"),
        pytest.param(None, None, None, id="neither id nor ge_cloud_id"),
    ],
)
def test_data_context__resolve_id_and_ge_cloud_id_success(
    id: str | None, ge_cloud_id: str | None, expected: str | None
):
    resolved = AbstractDataContext._resolve_id_and_ge_cloud_id(
        id=id, ge_cloud_id=ge_cloud_id
    )
    assert resolved == expected


@pytest.mark.unit
@pytest.mark.cloud
def test_data_context__resolve_id_and_ge_cloud_id_failure():
    id = "abc123"
    ge_cloud_id = "def456"

    with pytest.raises(ValueError) as e:
        _ = AbstractDataContext._resolve_id_and_ge_cloud_id(
            id=id, ge_cloud_id=ge_cloud_id
        )

    assert "either id or ge_cloud_id (not both)" in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "method_name, required_args",
    [
        pytest.param(
            "save_expectation_suite", ["expectation_suite"], id="save_expectation_suite"
        ),
        pytest.param("save_datasource", ["datasource"], id="save_datasource"),
        pytest.param("save_profiler", ["profiler"], id="save_profiler"),
        pytest.param(
            "create_expectation_suite",
            ["expectation_suite_name"],
            id="create_expectation_suite",
        ),
    ],
)
def test_data_context_crud_api_deprecation(
    in_memory_runtime_context: EphemeralDataContext,
    method_name: str,
    required_args: list[str],
):
    context = in_memory_runtime_context

    method = getattr(context, method_name)
    args = {arg: mock.Mock() for arg in required_args}

    with pytest.deprecated_call():
        try:
            method(**args)
        # We only care about triggering the method to ensure deprecation
        # The actual underlying logic of each method is under test elsewhere in the test suite
        except Exception:
            pass
