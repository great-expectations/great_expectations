from unittest.mock import MagicMock

import pytest

from great_expectations.agent.actions import RunOnboardingDataAssistantAction
from great_expectations.agent.models import RunOnboardingDataAssistantEvent
from great_expectations.data_context import CloudDataContext
from great_expectations.exceptions import StoreBackendError


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


def test_run_onboarding_data_assistant_event_requires_unique_expectation_suite(
    context, event
):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_checkpoint.side_effect = StoreBackendError("test-message")

    with pytest.raises(ValueError):
        action.run(event, id=id)


def test_run_onboarding_data_assistant_event_requires_unique_checkpoint(context, event):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")

    with pytest.raises(ValueError):
        action.run(event, id=id)


def test_run_onboarding_data_assistant_event_creates_expectation_suite(context, event):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    context.get_checkpoint.side_effect = StoreBackendError("test-message")

    action.run(event, id=id)

    context.add_expectation_suite.assert_called()


def test_run_onboarding_data_assistant_event_creates_checkpoint(context, event):
    action = RunOnboardingDataAssistantAction(context=context)
    id = "096ce840-7aa8-45d1-9e64-2833948f4ae8"
    context.get_expectation_suite.side_effect = StoreBackendError("test-message")
    context.get_checkpoint.side_effect = StoreBackendError("test-message")

    action.run(event, id=id)

    context.add_or_update_checkpoint.assert_called()
