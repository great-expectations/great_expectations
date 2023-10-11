import os

import pytest
from pact import Like, Pact

from great_expectations.core.http import create_session

ORGANIZATION_ID = os.environ.get("MERCURY_ORGANIZATION_ID")
ACCESS_TOKEN = os.environ.get("MERCURY_ACCESS_TOKEN")

DATASOURCE_ID = "15da041b-328e-44f7-892e-2bfd1a887ef8"


@pytest.mark.mercury
def test_get_data_context_given_exists(pact: Pact, pact_mock_mercury_url: str):
    path = f"/organizations/{ORGANIZATION_ID}/data-context-configuration"
    request_url = f"{pact_mock_mercury_url}{path}"
    session = create_session(access_token=ACCESS_TOKEN)

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
            headers=dict(session.headers),
        )
        .will_respond_with(200, body=Like(expected_data_context))
    )

    with pact:
        session.get(request_url)
