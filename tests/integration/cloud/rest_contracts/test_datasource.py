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


NON_EXISTENT_DATASOURCE_ID: Final[str] = "6ed9a340-8469-4ee2-a300-ffbe5d09b49d"

EXISTING_DATASOURCE_ID: Final[str] = "15da041b-328e-44f7-892e-2bfd1a887ef8"


POST_DATASOURCE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.Like(
        {
            "id": pact.Format().uuid,
            "attributes": {
                "datasource_config": {},
            },
        },
    )
}

GET_DATASOURCE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.Like(
        {
            "id": pact.Format().uuid,
            "type": "pandas",
            "attributes": {
                # pact doesn't test optional attributes like an empty "assets" list
                # this is the minimum response. https://docs.pact.io/faq#why-is-there-no-support-for-specifying-optional-attributes
                "datasource_config": pact.Like({}),
            },
        },
    )
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
                "datasources",
                EXISTING_DATASOURCE_ID,
            ),
            upon_receiving="a request to get a Data Source",
            given="the Data Source exists",
            response_status=200,
            response_body=GET_DATASOURCE_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_datasource(
    contract_interaction: ContractInteraction,
    run_rest_api_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_rest_api_pact_test(contract_interaction)
