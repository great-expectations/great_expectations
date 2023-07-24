from great_expectations.agent.actions.agent_action import ActionResult, AgentAction
from great_expectations.agent.actions.data_assistants.utils import (
    build_action_result,
    build_batch_request,
)
from great_expectations.agent.models import RunOnboardingDataAssistantEvent


class RunOnboardingDataAssistantAction(AgentAction[RunOnboardingDataAssistantEvent]):
    def __init__(self, context):
        super().__init__(context=context)
        self._data_assistant = self._context.assistants.onboarding

    def run(
        self,
        event: RunOnboardingDataAssistantEvent,
        id: str,
    ) -> ActionResult:
        batch_request = build_batch_request(context=self._context, event=event)

        data_assistant_result = self._data_assistant.run(
            batch_request=batch_request,
        )
        return build_action_result(
            context=self._context,
            event=event,
            data_assistant_result=data_assistant_result,
            id=id,
        )
