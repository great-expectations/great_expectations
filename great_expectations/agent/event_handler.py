from __future__ import annotations

from typing import TYPE_CHECKING

from great_expectations.agent.actions import ColumnDescriptiveMetricsAction
from great_expectations.agent.actions.data_assistants import (
    RunMissingnessDataAssistantAction,
    RunOnboardingDataAssistantAction,
)
from great_expectations.agent.actions.draft_datasource_config_action import (
    DraftDatasourceConfigAction,
)
from great_expectations.agent.models import (
    DraftDatasourceConfigEvent,
    Event,
    RunCheckpointEvent,
    RunColumnDescriptiveMetricsEvent,
    RunMissingnessDataAssistantEvent,
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
    from great_expectations.agent.actions.agent_action import ActionResult, AgentAction
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

    def get_event_action(self, event: Event) -> AgentAction:
        """Get the action that should be run for the given event."""
        if isinstance(event, RunOnboardingDataAssistantEvent):
            return RunOnboardingDataAssistantAction(context=self._context)

        if isinstance(event, RunMissingnessDataAssistantEvent):
            return RunMissingnessDataAssistantAction(context=self._context)

        if isinstance(event, RunCheckpointEvent):
            raise NotImplementedError

        if isinstance(event, RunColumnDescriptiveMetricsEvent):
            metric_retrievers: list[MetricRetriever] = [
                ColumnDescriptiveMetricsMetricRetriever(self._context)
            ]
            batch_inspector = BatchInspector(self._context, metric_retrievers)
            cloud_data_store = CloudDataStore(self._context)
            column_descriptive_metrics_repository = MetricRepository(
                data_store=cloud_data_store
            )
            return ColumnDescriptiveMetricsAction(
                context=self._context,
                batch_inspector=batch_inspector,
                metric_repository=column_descriptive_metrics_repository,
            )

        if isinstance(event, DraftDatasourceConfigEvent):
            return DraftDatasourceConfigAction(context=self._context)

        # shouldn't get here
        raise UnknownEventError("Unknown message received - cannot process.")

    def handle_event(self, event: Event, id: str) -> ActionResult:
        """Transform an Event into an ActionResult."""
        action = self.get_event_action(event=event)
        action_result = action.run(event=event, id=id)
        return action_result


class UnknownEventError(Exception):
    ...
