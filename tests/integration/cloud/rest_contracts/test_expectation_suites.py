from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Callable, Final

import pact
import pytest

from great_expectations.data_context import CloudDataContext
from great_expectations.exceptions import DataContextError
from tests.integration.cloud.rest_contracts.conftest import (
    EXISTING_ORGANIZATION_ID,
    ContractInteraction,
)

if TYPE_CHECKING:
    from tests.integration.cloud.rest_contracts.conftest import PactBody


NON_EXISTENT_EXPECTATION_SUITE_ID: Final[str] = "6ed9a340-8469-4ee2-a300-ffbe5d09b49d"

EXISTING_EXPECTATION_SUITE_ID: Final[str] = "9390c24d-e8d6-4944-9411-4d0aaed14915"

EXISTING_EXPECTATION_SUITE_NAME: Final[str] = "david_expectation_suite"


POST_EXPECTATION_SUITE_MIN_REQUEST_BODY: Final[PactBody] = {
    "data": {
        "type": "expectation_suite",
        "attributes": {
            "suite": {
                "ge_cloud_id": None,
                "meta": {"great_expectations_version": "0.13.23"},
                "expectations": [
                    {
                        "kwargs": {"max_value": 3, "min_value": 1},
                        "meta": {},
                        "expectation_type": "expect_table_row_count_to_be_between",
                    },
                ],
                "expectation_suite_name": "brand new suite",
                "data_asset_type": "pandas",
            },
            "organization_id": EXISTING_ORGANIZATION_ID,
        },
    },
}

POST_EXPECTATION_SUITE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": {
        "attributes": {
            "created_by_id": pact.Format().uuid,
            "organization_id": pact.Format().uuid,
            "suite": {
                "expectation_suite_name": "brand new suite",
                "expectations": [
                    {
                        "expectation_type": "expect_table_row_count_to_be_between",
                        "ge_cloud_id": pact.Format().uuid,
                        "kwargs": {"max_value": 3, "min_value": 1},
                        "meta": {},
                    }
                ],
                "ge_cloud_id": pact.Format().uuid,
                "meta": {"great_expectations_version": "0.13.23"},
            },
        },
        "id": pact.Format().uuid,
        "type": "expectation_suite",
    }
}

GET_EXPECTATION_SUITE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": {
        "attributes": {
            "created_by_id": pact.Format().uuid,
            "organization_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
            "suite": {
                "expectation_suite_name": pact.Like("raw_health.critical_1a"),
                "expectations": pact.EachLike(
                    {
                        "expectation_type": "expect_table_row_count_to_be_between",
                        "ge_cloud_id": pact.Format().uuid,
                        "kwargs": {},
                        "meta": {},
                        "rendered_content": pact.EachLike(
                            {
                                "name": "atomic.prescriptive.summary",
                                "value": {},
                                "value_type": "StringValueType",
                            }
                        ),
                    },
                    minimum=1,
                ),
                "ge_cloud_id": "3705d38a-0eec-4bd8-9956-fdb34df924b6",
                "meta": {"great_expectations_version": pact.Like("0.13.23")},
            },
        },
        "id": "3705d38a-0eec-4bd8-9956-fdb34df924b6",
        "type": "expectation_suite",
    },
}

GET_EXPECTATION_SUITES_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.EachLike(
        {
            "attributes": {
                "created_by_id": pact.Format().uuid,
                "organization_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
                "suite": {
                    "expectation_suite_name": pact.Like("raw_health.critical_1a"),
                    "ge_cloud_id": pact.Format().uuid,
                    "meta": {"great_expectations_version": pact.Like("0.13.23")},
                },
            },
            "id": pact.Format().uuid,
            "type": "expectation_suite",
        },
        minimum=1,
    ),
}


@pytest.mark.cloud
def test_get_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
) -> None:
    provider_state = "the Expectation Suite does exist"
    scenario = "a request to get an Expectation Suite"
    method = "GET"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/expectation-suites/{EXISTING_EXPECTATION_SUITE_ID}"
    status = 200
    response_body = GET_EXPECTATION_SUITE_MIN_RESPONSE_BODY

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
        )
        .will_respond_with(
            status=status,
            body=response_body,
        )
    )

    with pact_test:
        cloud_data_context.get_expectation_suite(
            ge_cloud_id=EXISTING_EXPECTATION_SUITE_ID
        )


@pytest.mark.cloud
def test_get_non_existent_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
) -> None:
    provider_state = "the Expectation Suite does not exist"
    scenario = "a request to get an Expectation Suite"
    method = "GET"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/expectation-suites/{NON_EXISTENT_EXPECTATION_SUITE_ID}"
    status = 404

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
        )
        .will_respond_with(
            status=status,
        )
    )

    with pact_test:
        with pytest.raises(DataContextError):
            cloud_data_context.get_expectation_suite(
                ge_cloud_id=NON_EXISTENT_EXPECTATION_SUITE_ID
            )


@pytest.mark.cloud
def test_get_expectation_suites(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
) -> None:
    provider_state = "Expectation Suite exist"
    scenario = "a request to get Expectation Suites"
    method = "GET"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/expectation-suites"
    status = 200
    response_body = GET_EXPECTATION_SUITES_MIN_RESPONSE_BODY

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
        )
        .will_respond_with(
            status=status,
            body=response_body,
        )
    )

    with pact_test:
        cloud_data_context.list_expectation_suites()


@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="POST",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
            ),
            upon_receiving="a request to post an Expectation Suite",
            given="the Expectation Suite does not exist",
            request_body=POST_EXPECTATION_SUITE_MIN_REQUEST_BODY,
            response_status=201,
            response_body=POST_EXPECTATION_SUITE_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_post_expectation_suite_request(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
def test_put_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
) -> None:
    provider_state = "the Expectation Suite does exist"
    scenario = "a request to put an Expectation Suite"
    method = "PUT"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/expectation-suites/{EXISTING_EXPECTATION_SUITE_ID}"
    request_body = POST_EXPECTATION_SUITE_MIN_REQUEST_BODY
    status = 204

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
            body=request_body,
        )
        .will_respond_with(
            status=status,
        )
    )

    suite_dict = POST_EXPECTATION_SUITE_MIN_REQUEST_BODY["data"]["attributes"]["suite"]

    with pact_test:
        cloud_data_context.add_or_update_expectation_suite(**suite_dict)
    cloud_data_context.delete_expectation_suite(
        expectation_suite_name=suite_dict["expectation_suite_name"]
    )


@pytest.mark.cloud
def test_put_non_existent_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
) -> None:
    provider_state = "the Expectation Suite does not exist"
    scenario = "a request to put an Expectation Suite"
    method = "PUT"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/expectation-suites/{NON_EXISTENT_EXPECTATION_SUITE_ID}"
    request_body = POST_EXPECTATION_SUITE_MIN_REQUEST_BODY
    status = 404

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
            body=request_body,
        )
        .will_respond_with(
            status=status,
        )
    )

    suite_dict = POST_EXPECTATION_SUITE_MIN_REQUEST_BODY["data"]["attributes"]["suite"]

    with pact_test:
        cloud_data_context.add_or_update_expectation_suite(**suite_dict)
    cloud_data_context.delete_expectation_suite(
        expectation_suite_name=suite_dict["expectation_suite_name"]
    )


@pytest.mark.cloud
def test_delete_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
) -> None:
    provider_state = "the Expectation Suite does exist"
    scenario = "a request to delete an Expectation Suite"
    method = "DELETE"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/expectation-suites"
    query = {
        "name": "brand new suite",
    }
    status = 204

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
            query=query,
        )
        .will_respond_with(
            status=status,
        )
    )

    with pact_test:
        cloud_data_context.delete_expectation_suite(
            expectation_suite_name=query["name"]
        )


@pytest.mark.cloud
def test_delete_non_existent_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
) -> None:
    provider_state = "the Expectation Suite does not exist"
    scenario = "a request to delete an Expectation Suite"
    method = "DELETE"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/expectation-suites"
    query = {
        "name": "brand new suite",
    }
    status = 404

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
            query=query,
        )
        .will_respond_with(
            status=status,
        )
    )

    with pact_test:
        with pytest.raises(DataContextError):
            cloud_data_context.delete_expectation_suite(
                expectation_suite_name=query["name"]
            )
