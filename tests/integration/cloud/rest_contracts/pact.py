import pytest
from pact import Like

import great_expectations as gx

ORGANIZATION_ID = "0ccac18e-7631-4bdd-8a42-3c35cce574c6"
DATASOURCE_ID = "e7d11180-e9c0-4028-933c-ca86adb79ec2"


EXPECTED_DATA_CONTEXT = {
    "anonymous_usage_statistics": {
        "data_context_id": ORGANIZATION_ID,
        "enabled": False,
        "usage_statistics_url": "https://qa.stats.greatexpectations.io/great_expectations/v1/usage_statistics",
    },
    "config_version": 3,
    "datasources": {},
    "fluent_datasources": {},
    "include_rendered_content": {
        "globally": False,
        "expectation_validation_result": False,
        "expectation_suite": False,
    },
    "stores": {},
}

EXPECTED_EMPTY_DATASOURCES = {"data": []}

EXPECTED_DATASOURCE = {
    "data": {
        "id": DATASOURCE_ID,
        "attributes": {
            "datasource_config": {
                "id": DATASOURCE_ID,
                "name": "pandas_datasource",
                "type": "pandas",
                "assets": [],
            }
        },
    }
}


@pytest.mark.cloud
def test_full_workflow_with_pact_mocks(pact, pact_mock_mercury_url):
    datasource_name = "pandas_datasource"

    (
        pact.given("a Data Context exists")
        .upon_receiving("a request for Data Context")
        .with_request(
            "get",
            f"/organizations/{ORGANIZATION_ID}/data-context-configuration",
        )
        .will_respond_with(200, body=Like(EXPECTED_DATA_CONTEXT))
    )

    (
        pact.given("no datasources exist")
        .upon_receiving("a request for Datasources")
        .with_request(
            "get",
            f"/organizations/{ORGANIZATION_ID}/datasources",
        )
        .will_respond_with(200, body=Like(EXPECTED_EMPTY_DATASOURCES))
    )

    (
        pact.given("the Datasource does not exist")
        .upon_receiving("a request to add a Datasource")
        .with_request(
            "post",
            f"/organizations/{ORGANIZATION_ID}/datasources",
        )
        .will_respond_with(200, body=Like(EXPECTED_DATASOURCE))
    )

    (
        pact.given("the Datasource exists")
        .upon_receiving("a request to get a Datasource")
        .with_request(
            "get",
            f"/organizations/{ORGANIZATION_ID}/datasources/{DATASOURCE_ID}",
        )
        .will_respond_with(200, body=Like(EXPECTED_DATASOURCE))
    )

    (
        pact.given("the Datasource exists")
        .upon_receiving("a request to update a Datasource")
        .with_request(
            "put",
            f"/organizations/{ORGANIZATION_ID}/datasources/{DATASOURCE_ID}",
        )
        .will_respond_with(200, body=Like(EXPECTED_DATASOURCE))
    )

    with pact:
        context = gx.get_context(
            mode="cloud", cloud_base_url=pact_mock_mercury_url.geturl()
        )
        context.sources.add_pandas(name=datasource_name)
