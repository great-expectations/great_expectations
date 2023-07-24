from great_expectations.agent.actions.agent_action import ActionResult, AgentAction
from great_expectations.agent.actions.data_assistants import (
    RunMissingnessDataAssistantAction,
    RunOnboardingDataAssistantAction,
)
from great_expectations.agent.models import (
    Event,
    RunCheckpointEvent,
    RunMissingnessDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
)
from great_expectations.data_context import CloudDataContext


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def get_event_action(self, event: Event) -> AgentAction:
        """Get the action that should be run for the given event."""
        if isinstance(event, RunOnboardingDataAssistantEvent):
            return RunOnboardingDataAssistantAction(context=self._context)

        if isinstance(event, RunMissingnessDataAssistantEvent):
            return RunMissingnessDataAssistantAction(context=self._context)

        if isinstance(event, RunCheckpointEvent):
            raise NotImplementedError

        # shouldn't get here
        raise UnknownEventError("Unknown message received - cannot process.")

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""
        action = self.get_event_action(event=event)
        action_result = action.run(event=event, id=id)
        return action_result


class UnknownEventError(Exception):
    ...
