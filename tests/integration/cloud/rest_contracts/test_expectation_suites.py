from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Callable, Final

import pact
import pytest

from tests.integration.cloud.rest_contracts.conftest import (
    EXISTING_ORGANIZATION_ID,
    ContractInteraction,
)

if TYPE_CHECKING:
    from tests.integration.cloud.rest_contracts.conftest import PactBody


NON_EXISTENT_EXPECTATION_SUITE_ID: Final[str] = "6ed9a340-8469-4ee2-a300-ffbe5d09b49d"

EXISTING_EXPECTATION_SUITE_ID: Final[str] = "3705d38a-0eec-4bd8-9956-fdb34df924b6"

POST_EXPECTATION_SUITE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": {
        "attributes": {
            "created_by_id": pact.Format().uuid,
            "organization_id": "0ccac18e-7631-4bdd-8a42-3c35cce574c6",
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
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="GET",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
                EXISTING_EXPECTATION_SUITE_ID,
            ),
            upon_receiving="a request to get an Expectation Suite",
            given="the Expectation Suite does exist",
            response_status=200,
            response_body=GET_EXPECTATION_SUITE_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_get_expectation_suite(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="GET",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
                NON_EXISTENT_EXPECTATION_SUITE_ID,
            ),
            upon_receiving="a request to get an Expectation Suite",
            given="the Expectation Suite does not exist",
            response_status=404,
            response_body=None,
        ),
    ],
)
def test_get_non_existent_expectation_suite(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="GET",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
            ),
            upon_receiving="a request to get Expectation Suites",
            given="Expectation Suite exist",
            response_status=200,
            response_body=GET_EXPECTATION_SUITES_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_get_expectation_suites(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
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
            request_body={
                "data": {
                    "type": "expectation_suite",
                    "attributes": {
                        "suite": {
                            "meta": {"great_expectations_version": "0.13.23"},
                            "expectations": [
                                {
                                    "kwargs": {"max_value": 3, "min_value": 1},
                                    "meta": {},
                                    "expectation_type": "expect_table_row_count_to_be_between",
                                },
                            ],
                            "expectation_suite_name": "brand new suite",
                        }
                    },
                },
            },
            response_status=201,
            response_body=POST_EXPECTATION_SUITE_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_post_expectation_suite(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
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
            given="an Expectation Suite with same name exists",
            request_body={
                "data": {
                    "type": "expectation_suite",
                    "attributes": {
                        "suite": {
                            "meta": {"great_expectations_version": "0.13.23"},
                            "expectations": [
                                {
                                    "kwargs": {"max_value": 3, "min_value": 1},
                                    "meta": {},
                                    "expectation_type": "expect_table_row_count_to_be_between",
                                },
                            ],
                            "expectation_suite_name": "raw_health.critical_1a",
                        }
                    },
                },
            },
            response_status=400,
            response_body={
                "errors": [
                    {
                        "detail": "Expectation Suite with name raw_health.critical_1a already exists."
                    }
                ]
            },
        ),
    ],
)
def test_post_expectation_suite_with_existing_name(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="PUT",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
                EXISTING_EXPECTATION_SUITE_ID,
            ),
            upon_receiving="a request to put an Expectation Suite",
            given="the Expectation Suite does exist",
            request_body={
                "data": {
                    "type": "expectation_suite",
                    "attributes": {
                        "suite": {
                            "meta": {"great_expectations_version": "0.13.23"},
                            "expectations": [
                                {
                                    "kwargs": {"max_value": 3, "min_value": 1},
                                    "meta": {},
                                    "expectation_type": "expect_table_row_count_to_be_between",
                                },
                            ],
                            "expectation_suite_name": "renamed suite",
                        }
                    },
                },
            },
            response_status=204,
            response_body=None,
        ),
    ],
)
def test_put_expectation_suite(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="PUT",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
                NON_EXISTENT_EXPECTATION_SUITE_ID,
            ),
            upon_receiving="a request to put an Expectation Suite",
            given="the Expectation Suite does not exist",
            request_body={
                "data": {
                    "type": "expectation_suite",
                    "attributes": {
                        "suite": {
                            "meta": {"great_expectations_version": "0.13.23"},
                            "expectations": [
                                {
                                    "kwargs": {"max_value": 3, "min_value": 1},
                                    "meta": {},
                                    "expectation_type": "expect_table_row_count_to_be_between",
                                },
                            ],
                            "expectation_suite_name": "renamed suite",
                        }
                    },
                },
            },
            response_status=404,
            response_body=None,
        ),
    ],
)
def test_put_non_existent_expectation_suite(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="DELETE",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
            ),
            request_params={
                "name": "brand new suite",
            },
            upon_receiving="a request to delete an Expectation Suite",
            given="the Expectation Suite does exist",
            response_status=204,
            response_body=None,
        ),
    ],
)
def test_delete_expectation_suite(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="DELETE",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "expectation-suites",
                NON_EXISTENT_EXPECTATION_SUITE_ID,
            ),
            request_params={
                "name": "brand new suite",
            },
            upon_receiving="a request to delete an Expectation Suite",
            given="the Expectation Suite does not exist",
            response_status=404,
            response_body=None,
        ),
    ],
)
def test_delete_non_existent_expectation_suite(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)
