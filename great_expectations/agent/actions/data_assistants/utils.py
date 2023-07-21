from datetime import datetime
from typing import TYPE_CHECKING

from great_expectations.agent.actions.agent_action import (
    ActionResult,
)
from great_expectations.agent.models import (
    CreatedResource,
    RunDataAssistantEvent,
)
from great_expectations.datasource.fluent import Datasource as FluentDatasource

if TYPE_CHECKING:
    from great_expectations.core import ExpectationSuite
    from great_expectations.core.batch import BatchRequest
    from great_expectations.rule_based_profiler.data_assistant_result.data_assistant_result import (
        DataAssistantResult,
    )
    from great_expectations.data_context.data_context import CloudDataContext


def build_batch_request(
    context: CloudDataContext,
    event: RunDataAssistantEvent,
) -> BatchRequest:
    datasource = context.get_datasource(datasource_name=event.datasource_name)
    if not isinstance(datasource, FluentDatasource):
        raise ValueError(
            "The RunDataAssistant Action can only be used with a fluent-style datasource."
        )

    asset = datasource.get_asset(asset_name=event.data_asset_name)
    try:
        batch_request = asset.build_batch_request()
    except ValueError as e:
        raise ValueError(
            f"The RunDataAssistant Action for data assistant cannot be used with an in-memory dataframe asset."
        ) from e

    return batch_request


def build_action_result(
    context: CloudDataContext,
    event: RunDataAssistantEvent,
    data_assistant_result: DataAssistantResult,
) -> ActionResult:
    # build tz aware timestamp
    tz = datetime.now().astimezone().tzinfo  # noqa: DTZ005
    timestamp = datetime.now(tz=tz)

    # ensure we have unique names for created resources
    expectation_suite_name = f"{event.type} {event.data_asset_name} suite {timestamp}"
    checkpoint_name = f"{event.type} {event.data_asset_name} checkpoint {timestamp}"

    expectation_suite: ExpectationSuite = data_assistant_result.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    context.add_expectation_suite(expectation_suite=expectation_suite)

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
            CreatedResource(resource_id=expectation_suite_id, type="ExpectationSuite"),
            CreatedResource(resource_id=checkpoint_id, type="Checkpoint"),
        ],
    )
