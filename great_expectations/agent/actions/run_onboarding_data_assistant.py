from datetime import datetime
from typing import TYPE_CHECKING

from great_expectations.agent.actions.agent_action import (
    ActionResult,
    AgentAction,
)
from great_expectations.agent.models import (
    CreatedResource,
    RunOnboardingDataAssistantEvent,
)
from great_expectations.datasource.fluent import Datasource as FluentDatasource
from great_expectations.exceptions import StoreBackendError

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite


class RunOnboardingDataAssistantAction(AgentAction[RunOnboardingDataAssistantEvent]):
    def run(self, event: RunOnboardingDataAssistantEvent, id: str) -> ActionResult:
        expectation_suite_name = f"{event.data_asset_name} onboarding assistant suite"
        checkpoint_name = f"{event.data_asset_name} onboarding assistant checkpoint"

        # build tz aware timestamp
        tz = datetime.now().astimezone().tzinfo  # noqa: DTZ005
        timestamp = datetime.now(tz=tz)

        # ensure we have unique names for created resources
        try:
            self._context.get_expectation_suite(
                expectation_suite_name=expectation_suite_name
            )
            # if that didn't error, this name exists, so we add the timestamp
            expectation_suite_name = f"{expectation_suite_name} {timestamp}"
        except StoreBackendError:
            # resource is unique
            pass

        try:
            self._context.get_checkpoint(name=checkpoint_name)
            # if that didn't error, this name exists, so we add the timestamp
            checkpoint_name = f"{checkpoint_name} {timestamp}"
        except StoreBackendError:
            # resource is unique
            pass

        datasource = self._context.get_datasource(datasource_name=event.datasource_name)
        if not isinstance(datasource, FluentDatasource):
            raise ValueError(
                "The RunOnboardingDataAssistant Action can only be used with a fluent-style datasource."
            )
        asset = datasource.get_asset(asset_name=event.data_asset_name)
        try:
            batch_request = asset.build_batch_request()
        except ValueError as e:
            raise ValueError(
                "The RunOnboardingDataAssistant Action cannot be used with an in-memory dataframe asset."
            ) from e

        data_assistant_result = self._context.assistants.onboarding.run(
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
