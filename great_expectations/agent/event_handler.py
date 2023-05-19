from great_expectations.agent.message_service.subscriber import EventContext
from great_expectations.agent.models import RunDataAssistantEvent
from great_expectations.data_context import CloudDataContext




class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def handle_event(self, event_context: EventContext) -> None:
        """Pass event to the correct handler."""
        if isinstance(event_context.event, RunDataAssistantEvent):
            self._handle_run_data_assistant(event_context)
        else:
            raise ValueError(f'Unknown message received - cannot process.')

    def _handle_run_data_assistant(
        self, event_context: EventContext
    ) -> None:
        """Action that occurs when a RunDataAssistant event is received."""
        raise NotImplementedError
