from typing import Literal
from unittest.mock import MagicMock, Mock

import pytest

from great_expectations.agent.event_handler import EventHandler, UnknownEventError
from great_expectations.agent.message_service.subscriber import EventContext
from great_expectations.agent.models import (
    EventBase,
    RunCheckpointEvent,
    RunDataAssistantEvent,
)
from great_expectations.data_context import CloudDataContext


def test_event_handler_raises_for_unknown_event():
    class NotARealEvent(EventBase):
        type: Literal["not-a-real-event"] = "not-a-real-event"

    event = NotARealEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    processed_successfully = Mock()
    processed_with_failures = Mock()
    redeliver_message = Mock()
    event_context = EventContext(
        event=event,
        correlation_id=correlation_id,
        processed_successfully=processed_successfully,
        processed_with_failures=processed_with_failures,
        redeliver_message=redeliver_message,
    )
    handler = EventHandler(context=context)

    with pytest.raises(UnknownEventError):
        handler.handle_event(event_context)


def test_event_handler_handles_run_data_assistant_event():
    event = RunDataAssistantEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    processed_successfully = Mock()
    processed_with_failures = Mock()
    redeliver_message = Mock()
    event_context = EventContext(
        event=event,
        correlation_id=correlation_id,
        processed_successfully=processed_successfully,
        processed_with_failures=processed_with_failures,
        redeliver_message=redeliver_message,
    )
    handler = EventHandler(context=context)

    with pytest.raises(NotImplementedError):
        handler.handle_event(event_context)


def test_event_handler_handles_run_checkpoint_event():
    event = RunCheckpointEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)
    processed_successfully = Mock()
    processed_with_failures = Mock()
    redeliver_message = Mock()
    event_context = EventContext(
        event=event,
        correlation_id=correlation_id,
        processed_successfully=processed_successfully,
        processed_with_failures=processed_with_failures,
        redeliver_message=redeliver_message,
    )
    handler = EventHandler(context=context)

    with pytest.raises(NotImplementedError):
        handler.handle_event(event_context)
