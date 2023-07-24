from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.agent.actions import (
    ColumnDescriptiveMetricsAction,
    RunOnboardingDataAssistantAction,
)
from great_expectations.agent.models import (
    Event,
    RunCheckpointEvent,
    RunColumnDescriptiveMetricsEvent,
    RunOnboardingDataAssistantEvent,
)
from great_expectations.experimental.metric_repository.batch_inspector import (
    BatchInspector,
)
from great_expectations.experimental.metric_repository.cloud_data_store import (
    CloudDataStore,
)
from great_expectations.experimental.metric_repository.column_descriptive_metrics_metric_retriever import (
    ColumnDescriptiveMetricsMetricRetriever,
)
from great_expectations.experimental.metric_repository.metric_repository import (
    MetricRepository,
)

if TYPE_CHECKING:
    from great_expectations.agent.actions.agent_action import ActionResult
    from great_expectations.data_context import CloudDataContext
    from great_expectations.experimental.metric_repository.metric_retriever import (
        MetricRetriever,
    )


class EventHandler:
    """
    Core business logic mapping events to actions.
    """

    def __init__(self, context: CloudDataContext) -> None:
        self._context = context

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""

        if isinstance(event, RunOnboardingDataAssistantEvent):
            action = RunOnboardingDataAssistantAction(context=self._context)
        elif isinstance(event, RunColumnDescriptiveMetricsEvent):
            metric_retrievers: list[MetricRetriever] = [
                ColumnDescriptiveMetricsMetricRetriever(self._context)
            ]
            batch_inspector = BatchInspector(self._context, metric_retrievers)
            cloud_data_store = CloudDataStore(self._context)
            column_descriptive_metrics_repository = MetricRepository(
                data_store=cloud_data_store
            )
            action = ColumnDescriptiveMetricsAction(
                context=self._context,
                batch_inspector=batch_inspector,
                metric_repository=column_descriptive_metrics_repository,
            )
        elif isinstance(event, RunCheckpointEvent):
            raise NotImplementedError
        else:
            # shouldn't get here
            raise UnknownEventError("Unknown message received - cannot process.")

        action_result = action.run(event=event, id=id)
        return action_result


class UnknownEventError(Exception):
    ...
