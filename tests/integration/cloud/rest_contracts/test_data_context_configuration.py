from __future__ import annotations

import os
from typing import TYPE_CHECKING, Final

import pact
import pytest

import great_expectations as gx
from tests.integration.cloud.rest_contracts.conftest import (
    EXISTING_ORGANIZATION_ID,
)

if TYPE_CHECKING:
    import requests

GET_DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY: Final[dict] = {
    "anonymous_usage_statistics": pact.Like(
        {
            "data_context_id": pact.Format().uuid,
            "enabled": False,
        }
    ),
    "datasources": pact.Like({}),
    "include_rendered_content": {
        "globally": True,
        "expectation_validation_result": True,
        "expectation_suite": True,
    },
}


@pytest.fixture
def cloud_base_url() -> str:
    try:
        return os.environ["GX_CLOUD_BASE_URL"]
    except KeyError as e:
        raise OSError("GX_CLOUD_BASE_URL is not set in this environment.") from e


@pytest.fixture
def cloud_access_token() -> str:
    try:
        return os.environ["GX_CLOUD_ACCESS_TOKEN"]
    except KeyError as e:
        raise OSError("GX_CLOUD_ACCESS_TOKEN is not set in this environment.") from e


@pytest.mark.cloud
def test_data_context_configuration(
    gx_cloud_session: requests.Session,
    cloud_base_url: str,
    cloud_access_token: str,
    pact_test: pact.Pact,
) -> None:
    provider_state = "the Data Context exists"
    scenario = "a request for a Data Context"
    method = "GET"
    path = f"{cloud_base_url}/organizations/{EXISTING_ORGANIZATION_ID}/data-context-configuration"
    status = 200
    response_body = GET_DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            headers=dict(gx_cloud_session.headers),
            method=method,
            path=path,
        )
        .will_respond_with(
            status=status,
            body=response_body,
        )
    )

    with pact_test:
        _ = gx.get_context(
            mode="cloud",
            cloud_base_url=cloud_base_url,
            cloud_organization_id=EXISTING_ORGANIZATION_ID,
            cloud_access_token=cloud_access_token,
        )
