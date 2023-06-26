from unittest.mock import MagicMock

import pytest

from great_expectations.agent.actions import (
    CreatedResource,
    RunOnboardingDataAssistantAction,
)
from great_expectations.agent.models import RunOnboardingDataAssistantEvent
from great_expectations.data_context import CloudDataContext
from great_expectations.datasource import LegacyDatasource
from great_expectations.datasource.fluent import Datasource
from great_expectations.exceptions import StoreBackendError

pytestmark = pytest.mark.unit


@pytest.fixture(scope="function")
def context():
    return MagicMock(autospec=CloudDataContext)


@pytest.fixture
def event():
    return RunOnboardingDataAssistantEvent(
        type="onboarding_data_assistant_request.received",
        datasource_name="test-datasource",
        data_asset_name="test-data-asset",
    )


def test_run_onboarding_data_assistant_event_raises_for_legacy_datasource(
    context, event
):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    context.get_checkpoint.side_effect = StoreBackendError("test-message")
    datasource = MagicMock(spec=LegacyDatasource)
    context.get_datasource.return_value = datasource

    with pytest.raises(ValueError, match=r"fluent-style datasource"):
        action.run(event=event, id=id)


def test_run_onboarding_data_assistant_event_creates_expectation_suite(context, event):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    context.get_checkpoint.side_effect = StoreBackendError("test-message")
    expectation_suite_id = "084a6e0f-c014-4e40-b6b7-b2f57cb9e176"
    checkpoint_id = "f5d32bbf-1392-4248-bc40-a3966fab2e0e"
    expectation_suite = context.assistants.onboarding.run().get_expectation_suite()
    expectation_suite.ge_cloud_id = expectation_suite_id
    checkpoint = context.add_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    datasource = MagicMock(spec=Datasource)
    context.get_datasource.return_value = datasource

    action.run(event=event, id=id)

    context.add_expectation_suite.assert_called_once_with(
        expectation_suite=expectation_suite
    )


def test_run_onboarding_data_assistant_event_creates_checkpoint(context, event):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    context.get_checkpoint.side_effect = StoreBackendError("test-message")
    expectation_suite_id = "084a6e0f-c014-4e40-b6b7-b2f57cb9e176"
    expectation_suite_name = f"{event.data_asset_name} onboarding assistant suite"
    checkpoint_name = f"{event.data_asset_name} onboarding assistant checkpoint"
    checkpoint_id = "f5d32bbf-1392-4248-bc40-a3966fab2e0e"
    expectation_suite = context.assistants.onboarding.run().get_expectation_suite()
    expectation_suite.ge_cloud_id = expectation_suite_id
    checkpoint = context.add_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    datasource = MagicMock(spec=Datasource)
    context.get_datasource.return_value = datasource

    action.run(event=event, id=id)

    expected_checkpoint_config = {
        "name": checkpoint_name,
        "validations": [
            {
                "expectation_suite_name": expectation_suite_name,
                "expectation_suite_ge_cloud_id": expectation_suite_id,
                "batch_request": {
                    "datasource_name": event.datasource_name,
                    "data_asset_name": event.data_asset_name,
                },
            }
        ],
        "config_version": 1,
        "class_name": "Checkpoint",
    }

    context.add_checkpoint.assert_called_once_with(**expected_checkpoint_config)


def test_run_onboarding_data_assistant_action_returns_action_result(context, event):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    context.get_checkpoint.side_effect = StoreBackendError("test-message")
    expectation_suite_id = "084a6e0f-c014-4e40-b6b7-b2f57cb9e176"
    checkpoint_id = "f5d32bbf-1392-4248-bc40-a3966fab2e0e"
    expectation_suite = context.assistants.onboarding.run().get_expectation_suite()
    expectation_suite.ge_cloud_id = expectation_suite_id
    checkpoint = context.add_checkpoint.return_value
    checkpoint.ge_cloud_id = checkpoint_id
    datasource = MagicMock(spec=Datasource)
    context.get_datasource.return_value = datasource

    action_result = action.run(event=event, id=id)

    assert action_result.type == event.type
    assert action_result.id == id
    assert action_result.created_resources == [
        CreatedResource(resource_id=expectation_suite_id, type="ExpectationSuite"),
        CreatedResource(resource_id=checkpoint_id, type="Checkpoint"),
    ]
