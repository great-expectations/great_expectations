from __future__ import annotations

import pathlib
from typing import TYPE_CHECKING, Final

import pact
import pytest

from tests.integration.cloud.rest_contracts.conftest import (
    EXISTING_ORGANIZATION_ID,
    ContractInteraction,
)

if TYPE_CHECKING:
    from tests.integration.cloud.rest_contracts.conftest import PactBody

GET_DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY: Final[PactBody] = {
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
                "data-context-configuration",
            ),
            upon_receiving="a request for a Data Context",
            given="the Data Context exists",
            response_status=200,
            response_body=GET_DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_data_context_configuration(
    contract_interaction: ContractInteraction,
) -> None:
    contract_interaction.run()
