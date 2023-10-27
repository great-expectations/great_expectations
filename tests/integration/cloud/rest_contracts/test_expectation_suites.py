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


NON_EXPECTATION_SUITE_ID: Final[str] = "6ed9a340-8469-4ee2-a300-ffbe5d09b49d"

EXISTING_EXPECTATION_SUITE_ID: Final[str] = "3705d38a-0eec-4bd8-9956-fdb34df924b6"


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
            given="the Expectation Suite exists",
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
