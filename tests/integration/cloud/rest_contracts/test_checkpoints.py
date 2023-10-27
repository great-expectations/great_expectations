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


NON_EXISTENT_CHECKPOINT_ID: Final[str] = "6ed9a340-8469-4ee2-a300-ffbe5d09b49d"

EXISTING_CHECKPOINT_ID: Final[str] = "051a1f4d-6276-484a-81e5-3df4fadd5154"

GET_CHECKPOINT_MIN_CHECKPOINT_BODY: Final[PactBody] = {
    "id": pact.Format().uuid,
    "type": "checkpoint",
    "attributes": {
        "id": pact.Format().uuid,
        "name": pact.Like("test_checkpoint"),
        "config": {},
    },
}

GET_CHECKPOINT_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.Like(GET_CHECKPOINT_MIN_CHECKPOINT_BODY)
}

GET_CHECKPOINTS_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.EachLike(
        GET_CHECKPOINT_MIN_CHECKPOINT_BODY,
        minimum=1,  # Default but writing it out for clarity
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
                "checkpoints",
                EXISTING_CHECKPOINT_ID,
            ),
            upon_receiving="a request to get a Checkpoint",
            given="the Checkpoint exists",
            response_status=200,
            response_body=GET_CHECKPOINT_MIN_RESPONSE_BODY,
        ),
        ContractInteraction(
            method="GET",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "checkpoints",
                NON_EXISTENT_CHECKPOINT_ID,
            ),
            upon_receiving="a request to get a Checkpoint",
            given="the Checkpoint does not exist",
            response_status=404,
            response_body=GET_CHECKPOINT_MIN_RESPONSE_BODY,
        ),
        ContractInteraction(
            method="GET",
            request_path=pathlib.Path(
                "/",
                "organizations",
                EXISTING_ORGANIZATION_ID,
                "checkpoints",
            ),
            upon_receiving="a request to get all Checkpoints",
            given="the Checkpoints exist",
            response_status=200,
            response_body=GET_CHECKPOINTS_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_checkpoint(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_pact_test(contract_interaction)
