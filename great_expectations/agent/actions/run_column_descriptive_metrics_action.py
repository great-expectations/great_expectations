from great_expectations.agent.actions import ActionResult, AgentAction
from great_expectations.agent.models import (
    CreatedResource,
    RunColumnDescriptiveMetricsEvent,
)
from great_expectations.data_context import CloudDataContext
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)


class ColumnDescriptiveMetricsAction(AgentAction[RunColumnDescriptiveMetricsEvent]):
    def __init__(
        self,
        context: CloudDataContext,
        metric_repository: MetricRepository,
        batch_inspector: BatchInspector,
    ):
        self._context = context
        self._metric_repository = metric_repository
        self._batch_inspector = batch_inspector

    def run(self, event: RunColumnDescriptiveMetricsEvent, id: str) -> ActionResult:
        datasource = self._context.get_datasource(event.datasource_name)
        data_asset = datasource.get_asset(event.data_asset_name)
        batch_request = data_asset.build_batch_request()

        metrics = self._batch_inspector.get_column_descriptive_metrics(batch_request)

        self._metric_repository.add(metrics)

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(resource_id=str(metrics.id), type="MetricRun"),
            ],
        )
