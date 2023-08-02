from great_expectations.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.agent.models import (
    CreatedResource,
    RunCheckpointEvent,
)


class RunCheckpointAction(AgentAction[RunCheckpointEvent]):
    def run(self, event: RunCheckpointEvent, id: str) -> ActionResult:
        checkpoint_run_result = self._context.run_checkpoint(
            ge_cloud_id=event.checkpoint_id
        )
        validation_results = checkpoint_run_result.run_results
        created_resources = []
        for key in validation_results.keys():
            created_resource = CreatedResource(
                resource_id=validation_results[key]["actions_results"][
                    "store_validation_result"
                ]["id"],
                type="SuiteValidationResult",
            )
            created_resources.append(created_resource)

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=created_resources,
        )
