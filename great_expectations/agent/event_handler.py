from typing import List

from pydantic import BaseModel

from great_expectations.agent.message_service.subscriber import EventContext
from great_expectations.agent.models import RunDataAssistantEvent
from great_expectations.data_context import CloudDataContext


class CreatedResource(BaseModel):
    type: str
    id: str


class EventHandlerResult(BaseModel):
    id: str
    type: str
    created_resources: List[CreatedResource]


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def handle_event(self, event_context: EventContext) -> EventHandlerResult:
        """Pass event to the correct handler."""
        if isinstance(event_context.event, RunDataAssistantEvent):
            return self._handle_run_data_assistant(event_context)
        else:
            # if pydantic parsing failed, event_context.event will be None
            raise UnknownEventError("Unknown message received - cannot process.")

    def _handle_run_data_assistant(
        self, event_context: EventContext
    ) -> EventHandlerResult:
        """Action that occurs when a RunDataAssistant event is received."""
        raise NotImplementedError


class UnknownEventError(Exception):
    ...
