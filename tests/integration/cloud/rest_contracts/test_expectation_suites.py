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
    from requests import Session

    from tests.integration.cloud.rest_contracts.conftest import PactBody


NON_EXISTENT_EXPECTATION_SUITE_ID: Final[str] = "6ed9a340-8469-4ee2-a300-ffbe5d09b49d"
GET_EXPECTATION_SUITE_ID: Final[str] = "c138767f-1d62-4312-bfff-1167891ab76f"
PUT_EXPECTATION_SUITE_ID: Final[str] = "9390c24d-e8d6-4944-9411-4d0aaed14915"

POST_EXPECTATION_SUITE_MIN_REQUEST_BODY: Final[PactBody] = {
    "data": {
        "id": None,
        "meta": {"great_expectations_version": "0.13.23"},
        "expectations": [
            {
                "kwargs": {"max_value": 3, "min_value": 1},
                "meta": {},
                "expectation_type": "expect_table_row_count_to_be_between",
            },
        ],
        "name": "brand new suite",
        "organization_id": EXISTING_ORGANIZATION_ID,
    },
}

POST_EXPECTATION_SUITE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": {
        "created_by_id": pact.Format().uuid,
        "organization_id": pact.Format().uuid,
        "name": "brand new suite",
        "expectations": [
            {
                "expectation_type": "expect_table_row_count_to_be_between",
                "id": pact.Format().uuid,
                "kwargs": {"max_value": 3, "min_value": 1},
                "meta": {},
            }
        ],
        "id": pact.Format().uuid,
        "meta": {"great_expectations_version": "0.13.23"},
    }
}

GET_EXPECTATION_SUITE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": {
        "created_by_id": pact.Format().uuid,
        "organization_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
        "name": pact.Like("no_checkpoint_suite"),
        "expectations": [
            {
                "type": "expect_column_values_to_be_between",
                "id": pact.Format().uuid,
                "kwargs": {
                    "column": "passenger_count",
                    "max_value": 5,
                    "min_value": 0,
                    "mostly": 0.97,
                },
                "meta": {},
            }
        ],
        "id": GET_EXPECTATION_SUITE_ID,
        "meta": {"great_expectations_version": "0.18.3"},
    },
}

GET_EXPECTATION_SUITES_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.EachLike(
        {
            "created_by_id": pact.Format().uuid,
            "organization_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
            "name": pact.Like("raw_health.critical_1a"),
            "id": pact.Format().uuid,
            "meta": {"great_expectations_version": pact.Like("0.13.23")},
            "expectations": pact.EachLike(
                {
                    "type": pact.Like("expect_column_values_to_be_between"),
                    "id": pact.Format().uuid,
                    "kwargs": pact.Like(
                        {
                            "column": "passenger_count",
                            "other_field": "another_value",
                        }
                    ),
                    "meta": pact.Like({}),
                }
            ),
        },
        minimum=1,
    ),
}


@pytest.mark.cloud
def test_get_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
    gx_cloud_session: Session,
) -> None:
    provider_state = "the Expectation Suite does exist"
    scenario = "a request to get an Expectation Suite"
    method = "GET"
    path = pathlib.Path(
        "/",
        "api",
        "v1",
        "organizations",
        EXISTING_ORGANIZATION_ID,
        "expectation-suites",
    )
    status = 200
    response_body = GET_EXPECTATION_SUITE_MIN_RESPONSE_BODY

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=str(path),
            headers=dict(gx_cloud_session.headers),
        )
        .will_respond_with(
            status=status,
            body=response_body,
        )
    )

    with pact_test:
        cloud_data_context.suites.get("no_checkpoint_suite")


@pytest.mark.cloud
def test_get_non_existent_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
    gx_cloud_session: Session,
) -> None:
    provider_state = "the Expectation Suite does not exist"
    scenario = "a request to get an Expectation Suite"
    method = "GET"
    path = pathlib.Path(
        "/",
        "api",
        "v1",
        "organizations",
        EXISTING_ORGANIZATION_ID,
        "expectation-suites",
    )

    status = 404

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=str(path),
            headers=dict(gx_cloud_session.headers),
        )
        .will_respond_with(
            status=status,
        )
    )

    with pact_test:
        with pytest.raises(DataContextError):
            cloud_data_context.suites.get(name="non_existent")


@pytest.mark.cloud
def test_get_expectation_suites(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
    gx_cloud_session: Session,
) -> None:
    provider_state = "Expectation Suite exist"
    scenario = "a request to get Expectation Suites"
    method = "GET"
    path = pathlib.Path(
        "/",
        "api",
        "v1",
        "organizations",
        EXISTING_ORGANIZATION_ID,
        "expectation-suites",
    )
    status = 200
    response_body = GET_EXPECTATION_SUITES_MIN_RESPONSE_BODY

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=str(path),
            headers=dict(gx_cloud_session.headers),
        )
        .will_respond_with(
            status=status,
            body=response_body,
        )
    )

    with pact_test:
        cloud_data_context.suites.all()


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="POST",
            request_path=pathlib.Path(
                "/",
                "api",
                "v1",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
            ),
            upon_receiving="a request to post an Expectation Suite",
            given="the Expectation Suite does not exist",
            request_body={
                "data": {
                    "meta": {"great_expectations_version": "0.13.23"},
                    "expectations": [
                        {
                            "kwargs": {"max_value": 3, "min_value": 1},
                            "meta": {},
                            "expectation_type": "expect_table_row_count_to_be_between",
                        },
                    ],
                    "name": "brand new suite",
                },
            },
            response_status=201,
            response_body=POST_EXPECTATION_SUITE_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_post_expectation_suite_request(
    contract_interaction: ContractInteraction,
    run_rest_api_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_rest_api_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="PUT",
            request_path=pathlib.Path(
                "/",
                "api",
                "v1" "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
                PUT_EXPECTATION_SUITE_ID,
            ),
            upon_receiving="a request to put an Expectation Suite",
            given="the Expectation Suite does exist",
            request_body={
                "data": {
                    "meta": {"great_expectations_version": "0.13.23"},
                    "expectations": [
                        {
                            "kwargs": {"max_value": 3, "min_value": 1},
                            "meta": {},
                            "expectation_type": "expect_table_row_count_to_be_between",
                        },
                    ],
                    "name": "renamed suite",
                },
            },
            response_status=200,
            response_body=None,
        ),
    ],
)
def test_put_expectation_suite_request(
    contract_interaction: ContractInteraction,
    run_rest_api_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_rest_api_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="PUT",
            request_path=pathlib.Path(
                "/",
                "api",
                "v1",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
                NON_EXISTENT_EXPECTATION_SUITE_ID,
            ),
            upon_receiving="a request to put an Expectation Suite",
            given="the Expectation Suite does not exist",
            request_body={
                "data": {
                    "meta": {"great_expectations_version": "0.13.23"},
                    "expectations": [
                        {
                            "kwargs": {"max_value": 3, "min_value": 1},
                            "meta": {},
                            "expectation_type": "expect_table_row_count_to_be_between",
                        },
                    ],
                    "name": "renamed suite",
                },
            },
            response_status=404,
            response_body=None,
        ),
    ],
)
def test_put_non_existent_expectation_suite(
    contract_interaction: ContractInteraction,
    run_rest_api_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_rest_api_pact_test(contract_interaction)
