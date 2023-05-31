from unittest.mock import MagicMock

import pytest

from great_expectations.agent.event_handler import EventHandler, UnknownEventError
from great_expectations.agent.models import (
    RunCheckpointEvent,
    RunOnboardingDataAssistantEvent,
    UnknownEvent,
)
from great_expectations.data_context import CloudDataContext

pytestmark = pytest.mark.unit


def test_event_handler_raises_for_unknown_event():
    event = UnknownEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    with pytest.raises(UnknownEventError):
        handler.handle_event(event=event, id=correlation_id)


def test_event_handler_handles_run_data_assistant_event(mocker):
    action = mocker.patch(
        "great_expectations.agent.event_handler.RunOnboardingDataAssistantAction"
    )
    event = RunOnboardingDataAssistantEvent(
        datasource_name="test-datasource", data_asset_name="test-data-asset"
    )
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    handler.handle_event(event=event, id=correlation_id)

    action.assert_called_with(context=context)
    action().run.assert_called_with(event=event, id=correlation_id)


def test_event_handler_handles_run_checkpoint_event():
    event = RunCheckpointEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    handler = EventHandler(context=context)

    with pytest.raises(NotImplementedError):
        handler.handle_event(event=event, id=correlation_id)
