from typing import TYPE_CHECKING

from great_expectations.agent.actions.action import (
    ActionResult,
    AgentAction,
    CreatedResource,
)
from great_expectations.agent.models import RunOnboardingDataAssistantEvent
from great_expectations.exceptions import StoreBackendError

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite


class RunOnboardingDataAssistantAction(AgentAction):
    def run(self, event: RunOnboardingDataAssistantEvent, id: str) -> ActionResult:
        expectation_suite_name = f"{event.data_asset_name} onboarding assistant suite"
        checkpoint_name = f"{event.data_asset_name} onboarding assistant checkpoint"

        # ensure resources we create don't already exist before we run the Data Assistant.
        try:
            self._context.get_expectation_suite(
                expectation_suite_name=expectation_suite_name
            )
            raise ValueError(
                f"Onboarding Assistant Expectation Suite `{expectation_suite_name}` already exists. "
                + "Please rename or delete suite and try again"
            )
        except StoreBackendError:
            # resource is unique
            pass

        try:
            self._context.get_checkpoint(name=checkpoint_name)
            raise ValueError(
                f"Onboarding Assistant Checkpoint `{checkpoint_name}` already exists. "
                + "Please rename or delete Checkpoint and try again"
            )
        except StoreBackendError:
            # resource is unique
            pass

        datasource = self._context.get_datasource(datasource_name=event.datasource_name)
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

        checkpoint = self._context.add_or_update_checkpoint(**checkpoint_config)

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(
                    id=expectation_suite.ge_cloud_id, type="ExpectationSuite"
                ),
                CreatedResource(id=checkpoint.ge_cloud_id, type="Checkpoint"),
            ],
        )
