from great_expectations.agent.models import Event, RunDataAssistantEvent, ShutdownEvent
from great_expectations.data_context import CloudDataContext


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def handle_event(self, event: Event, correlation_id: str) -> None:
        """Pass event to the correct handler."""
        if isinstance(event, RunDataAssistantEvent):
            self._handle_run_data_assistant(event, correlation_id)
        elif isinstance(event, ShutdownEvent):
            self._handle_shutdown(event, correlation_id)
        else:
            pass

    def _handle_run_data_assistant(
        self, event: RunDataAssistantEvent, correlation_id: str
    ) -> None:
        """Action that occurs when a RunDataAssistant event is received."""
        raise NotImplementedError

    def _handle_shutdown(self, event: ShutdownEvent, correlation_id: str) -> None:
        """Action that occurs when a ShutdownEvent is received."""
        raise ShutdownRequest


class ShutdownRequest(Exception):
    ...
