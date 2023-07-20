from great_expectations.agent.actions import ActionResult, AgentAction
from great_expectations.agent.models import (
    CreatedResource,
    RunBatchInspectorEvent,
)
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)


class RunBatchInspectorAction(AgentAction[RunBatchInspectorEvent]):
    def run(self, event: RunBatchInspectorEvent, id: str) -> ActionResult:
        datasource_from_action = self._context.get_datasource(event.datasource_name)
        data_asset_from_action = datasource_from_action.get_asset(event.data_asset_name)
        batch_request_from_action = data_asset_from_action.build_batch_request()
        validator_from_action = self._context.get_validator(
            batch_request=batch_request_from_action
        )

        batch_inspector = BatchInspector()
        metrics = batch_inspector.get_column_descriptive_metrics(validator_from_action)

        cloud_data_store = CloudDataStore(context=self._context)
        column_descriptive_metrics_repository = MetricRepository(
            data_store=cloud_data_store
        )
        column_descriptive_metrics_repository.create(metrics)

        # TODO: Reconcile this with storing multiple metrics (eg metrics.id):
        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(
                    resource_id="TODO_SOME_ID_probably_run_id", type="Metrics"
                ),
            ],
        )
