from unittest.mock import MagicMock

import pytest

from great_expectations.agent.actions.run_checkpoint import RunCheckpointAction
from great_expectations.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
)
from great_expectations.data_context import CloudDataContext

pytestmark = pytest.mark.unit


@pytest.fixture(scope="function")
def context():
    return MagicMock(autospec=CloudDataContext)


@pytest.fixture
def checkpoint_event():
    return RunCheckpointEvent(
        type="run_checkpoint_request",
        checkpoint_id="f5d32bbf-1392-4248-bc40-a3966fab2e0e",
    )


def test_run_checkpoint_action_returns_action_result(context, checkpoint_event):
    action = RunCheckpointAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    checkpoint_id = "5f3814d6-a2e2-40f9-ba75-87ddf485c3a8"
    checkpoint = context.run_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    checkpoint.run_results = {
        "GXCloudIdentifier::validation_result::78ebf58e-bdb5-4d79-88d5-79bae19bf7d0": {
            "actions_results": {
                "store_validation_result": {
                    "id": "78ebf58e-bdb5-4d79-88d5-79bae19bf7d0"
                }
            }
        }
    }
    action_result = action.run(event=checkpoint_event, id=id)

    assert action_result.type == checkpoint_event.type
    assert action_result.id == id
    assert action_result.created_resources == [
        CreatedResource(
            resource_id="78ebf58e-bdb5-4d79-88d5-79bae19bf7d0",
            type="SuiteValidationResult",
        ),
    ]
