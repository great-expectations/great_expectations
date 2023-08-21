from great_expectations.agent.actions import ActionResult, AgentAction
from great_expectations.agent.models import (
    CreatedResource,
    RunColumnDescriptiveMetricsEvent,
)
from great_expectations.compatibility.typing_extensions import override
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
        super().__init__(context=context)
        self._metric_repository = metric_repository
        self._batch_inspector = batch_inspector

    @override
    def run(self, event: RunColumnDescriptiveMetricsEvent, id: str) -> ActionResult:
        datasource = self._context.get_datasource(event.datasource_name)
        data_asset = datasource.get_asset(event.data_asset_name)  # type: ignore[union-attr] # Non fluent datasources BaseDatasource, LegacyDatasource does not have get_asset
        batch_request = data_asset.build_batch_request()

        metric_run = self._batch_inspector.compute_metric_run(batch_request)

        self._metric_repository.add(metric_run)

        return ActionResult(
            id=id,
            type=event.type,
            created_resources=[
                CreatedResource(resource_id=str(metric_run.id), type="MetricRun"),
            ],
        )
