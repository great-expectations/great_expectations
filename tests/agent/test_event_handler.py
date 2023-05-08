from typing import Literal
from unittest.mock import MagicMock

import pytest

from great_expectations.agent.event_handler import EventHandler, ShutdownRequest
from great_expectations.agent.models import (
    EventBase,
    RunDataAssistantEvent,
    ShutdownEvent,
)
from great_expectations.data_context import CloudDataContext


def test_event_handler_accepts_unknown_event():
    class NotARealEvent(EventBase):
        type: Literal["not-a-real-event"] = "not-a-real-event"

    event = NotARealEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)

    handler = EventHandler(context=context)

    handler.handle_event(event=event, correlation_id=correlation_id)

    context.assert_not_called()  # no op


def test_event_handler_handles_run_data_assistant_event():
    event = RunDataAssistantEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)

    handler = EventHandler(context=context)

    with pytest.raises(NotImplementedError):
        handler.handle_event(event=event, correlation_id=correlation_id)


def test_event_handler_handles_shutdown_event():
    event = ShutdownEvent()
    correlation_id = "74842258-803a-48ca-8921-eaf2802c14e2"
    context = MagicMock(autospec=CloudDataContext)

    handler = EventHandler(context=context)

    with pytest.raises(ShutdownRequest):
        handler.handle_event(event=event, correlation_id=correlation_id)
