from datetime import datetime
from typing import TYPE_CHECKING, Union

from great_expectations.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.agent.models import (
    CreatedResource,
    RunDataAssistantEvent,
    RunOnboardingDataAssistantEvent,
)
from great_expectations.datasource.fluent import Datasource as FluentDatasource

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite


# TODO(https://greatexpectations.atlassian.net/browse/DX-652): Remove RunOnboardingDataAssistantEvent after fully deprecating it
class RunDataAssistantAction(
    AgentAction[Union[RunDataAssistantEvent, RunOnboardingDataAssistantEvent]]
):
    def run(
        self,
        event: Union[RunDataAssistantEvent, RunOnboardingDataAssistantEvent],
        id: str,
    ) -> ActionResult:
        # build tz aware timestamp
        tz = datetime.now().astimezone().tzinfo  # noqa: DTZ005
        timestamp = datetime.now(tz=tz)

        assistant_name = (
            "onboarding"
            if isinstance(event, RunOnboardingDataAssistantEvent)
            else event.assistant_name
        )
        # ensure we have unique names for created resources
        expectation_suite_name = (
            f"{event.data_asset_name} {assistant_name} assistant suite {timestamp}"
        )
        checkpoint_name = (
            f"{event.data_asset_name} {assistant_name} assistant checkpoint {timestamp}"
        )

        datasource = self._context.get_datasource(datasource_name=event.datasource_name)
        if not isinstance(datasource, FluentDatasource):
            raise ValueError(
                "The RunDataAssistant Action can only be used with a fluent-style datasource."
            )

        asset = datasource.get_asset(asset_name=event.data_asset_name)
        try:
            batch_request = asset.build_batch_request()
        except ValueError as e:
            raise ValueError(
                "The RunDataAssistant Action cannot be used with an in-memory dataframe asset."
            ) from e

        try:
            data_assistant = getattr(self._context.assistants, assistant_name)
        except AttributeError as e:
            raise ValueError(
                f'The RunDataAssistant Action cannot be used with an unknown assistant "{assistant_name}".'
            ) from e

        data_assistant_result = data_assistant.run(
            batch_request=batch_request,
        )
        expectation_suite: ExpectationSuite = (
            data_assistant_result.get_expectation_suite(
                expectation_suite_name=expectation_suite_name
            )
        )
        self._context.add_expectation_suite(expectation_suite=expectation_suite)

        checkpoint_config = {
            "name": checkpoint_name,
            "validations": [
                {
                    "expectation_suite_name": expectation_suite_name,
                    "expectation_suite_ge_cloud_id": expectation_suite.ge_cloud_id,
                    "batch_request": {
                        "datasource_name": event.datasource_name,
                        "data_asset_name": event.data_asset_name,
                    },
                }
            ],
            "config_version": 1,
            "class_name": "Checkpoint",
        }

        checkpoint = self._context.add_checkpoint(**checkpoint_config)  # type: ignore[arg-type]

        expectation_suite_id = expectation_suite.ge_cloud_id
        checkpoint_id = checkpoint.ge_cloud_id
        if expectation_suite_id is None or checkpoint_id is None:
            raise ValueError("Cloud backed resources must have an ID.")
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(
                    resource_id=expectation_suite_id, type="ExpectationSuite"
                ),
                CreatedResource(resource_id=checkpoint_id, type="Checkpoint"),
            ],
        )
