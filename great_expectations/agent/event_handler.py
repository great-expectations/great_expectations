from great_expectations.agent.actions import RunOnboardingDataAssistantAction
from great_expectations.agent.actions.agent_action import ActionResult
from great_expectations.agent.agent_server_session import AgentServerSession
from great_expectations.agent.models import (
    Event,
    RunCheckpointEvent,
    RunOnboardingDataAssistantEvent,
)
from great_expectations.data_context import CloudDataContext


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(
        self, context: CloudDataContext, agent_server_session: AgentServerSession
    ) -> None:
        self._context = context
        self._agent_server_session = agent_server_session

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""

        if isinstance(event, RunOnboardingDataAssistantEvent):
            action = RunOnboardingDataAssistantAction(
                context=self._context, agent_server_session=self._agent_server_session
            )
        elif isinstance(event, RunCheckpointEvent):
            raise NotImplementedError
        else:
            # shouldn't get here
            raise UnknownEventError("Unknown message received - cannot process.")

        action_result = action.run(event=event, id=id)
        return action_result


class UnknownEventError(Exception):
    ...
