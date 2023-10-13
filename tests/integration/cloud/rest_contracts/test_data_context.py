from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest
from pact import Like, Pact

if TYPE_CHECKING:
    import requests

ORGANIZATION_ID = os.environ.get("GX_CLOUD_ORGANIZATION_ID")

DATASOURCE_ID = "15da041b-328e-44f7-892e-2bfd1a887ef8"


@pytest.mark.cloud
def test_get_data_context_given_exists(
    session: requests.Session,
    pact: Pact,
    pact_mock_mercury_url: str,
):
    path = f"/organizations/{ORGANIZATION_ID}/data-context-configuration"
    request_url = f"{pact_mock_mercury_url}{path}"

    expected_data_context = {
        "anonymous_usage_statistics": {
            "data_context_id": ORGANIZATION_ID,
            "enabled": False,
        },
        "config_version": 3,
        "datasources": {},
        "include_rendered_content": {
            "globally": True,
            "expectation_validation_result": True,
            "expectation_suite": True,
        },
        "stores": {},
    }

    (
        pact.given("a Data Context exists")
        .upon_receiving("a request for Data Context")
        .with_request(
            method="GET",
            path=path,
        )
        .will_respond_with(
            status=200,
            body=Like(expected_data_context),
        )
    )

    with pact:
        session.get(request_url)
