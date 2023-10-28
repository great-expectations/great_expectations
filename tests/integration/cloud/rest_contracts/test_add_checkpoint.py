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

NON_EXISTING_CHECKPOINT_ID: Final[str] = "68058949-f9c2-47b1-922a-a89c23ffad99"
EXISTING_CHECKPOINT_ID: Final[str] = "68058949-f9c2-47b1-922a-a89c23ffad99"
PUT_CHECKPOINT_MIN_RESPONSE_BODY: Final[PactBody] = pact.Like("204 string")
PUT_CHECKPOINTS_MIN_REQUEST_BODY = {
    "data": {
        "id": "123",
        "type": "checkpoint",
        "attributes": {
            "checkpoint_config": {},
        },
    },
}

GET_CHECKPOINTS_MIN_RESPONSE_BODY: Final[dict] = {
    "data": {
        "id": pact.Format().uuid,
        "type": "checkpoint",
        "attributes": {
            "checkpoint_config": {
                "validations": [
                    {
                        "id": pact.Format().uuid,
                    }
                ]
            },
        },
    },
}


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
                "checkpoints",
                NON_EXISTING_CHECKPOINT_ID,
            ),
            request_body=PUT_CHECKPOINTS_MIN_REQUEST_BODY,
            upon_receiving="a request to create a Checkpoint",
            given="the Checkpoint is created",
            response_status=204,
            response_body="",
        ),
        ContractInteraction(
            method="GET",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "checkpoints",
                EXISTING_CHECKPOINT_ID,
            ),
            upon_receiving="a request to get a Checkpoint",
            given="the Checkpoint exists",
            response_status=200,
            response_body=GET_CHECKPOINTS_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_put_checkpoint(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)
