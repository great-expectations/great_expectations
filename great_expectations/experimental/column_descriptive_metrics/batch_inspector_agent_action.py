from typing import Literal

from great_expectations.agent.actions import ActionResult, AgentAction
from great_expectations.agent.models import CreatedResource, EventBase
from great_expectations.experimental.column_descriptive_metrics.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.column_descriptive_metrics.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.column_descriptive_metrics.column_descriptive_metrics_repository import (
    ColumnDescriptiveMetricsRepository,
)


class RunBatchInspectorEvent(EventBase):
    type: Literal[
        "batch_inspector_request.received"
    ] = "batch_inspector_request.received"
    datasource_name: str
    data_asset_name: str


class RunBatchInspectorAction(AgentAction[RunBatchInspectorEvent]):
    def run(self, event: RunBatchInspectorEvent, id: str) -> ActionResult:
        batch_inspector = BatchInspector(organization_id=self._context.organization_id)

        datasource_from_action = self._context.get_datasource(event.datasource_name)
        data_asset_from_action = datasource_from_action.get_asset(event.data_asset_name)
        batch_request_from_action = data_asset_from_action.build_batch_request()
        batch_from_action = data_asset_from_action.get_batch_list_from_batch_request(
            batch_request=batch_request_from_action
        )[0]
        # TODO: Emit warning if more than one batch found that we are only using the first one.

        metrics = batch_inspector.get_column_descriptive_metrics(
            batch=batch_from_action
        )

        cloud_data_store = CloudDataStore(
            context=self._context
        )  # context to connect to cloud
        column_descriptive_metrics_repository = ColumnDescriptiveMetricsRepository(
            data_store=cloud_data_store
        )
        column_descriptive_metrics_repository.create(metrics)

        # TODO: Reconcile this with storing multiple metrics (eg metrics.id):
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(resource_id=metrics.id, type="Metrics"),
            ],
        )
