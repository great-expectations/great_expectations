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

EXISTING_CHECKPOINT_ID: Final[str] = "4d734009-2c50-4222-bd71-9660f5b05fff"

# Get Checkpoint
GET_CHECKPOINT_MIN_CHECKPOINT_BODY: Final[PactBody] = {
    "id": pact.Format().uuid,
    "type": "checkpoint",
    "attributes": {
        "id": pact.Format().uuid,
        "name": pact.Like("string checkpoint name"),
        "organization_id": pact.Format().uuid,
        "checkpoint_config": {},
    },
}

GET_CHECKPOINT_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.Like(GET_CHECKPOINT_MIN_CHECKPOINT_BODY)
}
GET_CHECKPOINT_NOT_FOUND_RESPONSE_BODY: Final[PactBody] = pact.Like("404 string")

# Get Checkpoints
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
            response_body=GET_CHECKPOINT_NOT_FOUND_RESPONSE_BODY,
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
    run_rest_api_pact_test: Callable[[ContractInteraction], None],
) -> None:
    run_rest_api_pact_test(contract_interaction)
