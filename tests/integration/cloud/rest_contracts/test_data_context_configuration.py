from __future__ import annotations

import pathlib
import uuid
from typing import Callable

import pytest
from pact import Like

from tests.integration.cloud.rest_contracts.conftest import (
    ContractInteraction,
)


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="GET",
            upon_receiving="a request for a Data Context",
            given="the Data Context exists",
            response_status=200,
            response_body={
                "anonymous_usage_statistics": Like(
                    {
                        "data_context_id": str(uuid.uuid4()),
                        "enabled": True,
                    }
                ),
                "datasources": Like({}),
                "include_rendered_content": {
                    "globally": True,
                    "expectation_validation_result": True,
                    "expectation_suite": True,
                },
            },
        ),
    ],
)
def test_data_context_configuration(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[pathlib.Path, ContractInteraction], None],
    existing_organization_id: str,
) -> None:
    # the path to the endpoint relative to the base url
    path = pathlib.Path(
        "/", "organizations", existing_organization_id, "data-context-configuration"
    )
    run_pact_test(path, contract_interaction)
