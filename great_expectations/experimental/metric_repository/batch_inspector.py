from __future__ import annotations

import abc
import uuid
from typing import TYPE_CHECKING

from great_expectations.experimental.metric_repository.metrics import (
    Metric,
    MetricException,
    MetricRun,
    NumericTableMetric,
    StringListTableMetric,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations import DataContext
    from great_expectations.datasource.fluent.interfaces import BatchRequest


class BatchInspector:
    def __init__(self, context: DataContext, metric_retrievers: list[MetricRetriever]):
        self._context = context
        self._metric_retrievers = metric_retrievers

    def compute_metric_run(self, batch_request: BatchRequest) -> MetricRun:
        run_id = self._generate_run_id()

        metrics = []
        for metric_retriever in self._metric_retrievers:
            metrics.extend(metric_retriever.get_metrics(batch_request=batch_request))

        return MetricRun(id=run_id, metrics=metrics)

    def _generate_run_id(self) -> uuid.UUID:
        return uuid.uuid4()


class MetricRetriever(abc.ABC):
    def __init__(self, context: DataContext):
        self._context = context

    @abc.abstractmethod
    def get_metrics(self, batch_request: BatchRequest) -> list[Metric]:
        raise NotImplementedError

    def _generate_metric_id(self) -> uuid.UUID:
        return uuid.uuid4()


class ColumnDescriptiveMetricsMetricRetriever(MetricRetriever):
    def get_metrics(self, batch_request: BatchRequest) -> list[Metric]:
        table_metrics_list = self._get_table_metrics(batch_request)

        return table_metrics_list

    def _get_table_metrics(self, batch_request: BatchRequest) -> list[Metric]:
        table_metric_names = ["table.row_count", "table.columns"]
        table_metric_configs = [
            MetricConfiguration(
                metric_name=metric_name, metric_domain_kwargs={}, metric_value_kwargs={}
            )
            for metric_name in table_metric_names
        ]
        validator = self._context.get_validator(batch_request=batch_request)
        computed_metrics = validator.compute_metrics(
            metric_configurations=table_metric_configs,
            runtime_configuration={"catch_exceptions": True},  # TODO: Is this needed?
        )

        # Convert computed_metrics
        metrics = []
        metric_name = "table.row_count"
        NumericTableMetric.update_forward_refs()
        StringListTableMetric.update_forward_refs()
        metrics.append(
            NumericTableMetric(
                id=self._generate_metric_id(),
                batch=validator.active_batch,
                metric_name=metric_name,
                value=computed_metrics[(metric_name, tuple(), tuple())],
                exception=MetricException(),  # TODO: Pass through
            )
        )
        metric_name = "table.columns"
        metrics.append(
            StringListTableMetric(
                id=self._generate_metric_id(),
                batch=validator.active_batch,
                metric_name=metric_name,
                value=computed_metrics[(metric_name, tuple(), tuple())],
                exception=MetricException(),  # TODO: Pass through
            )
        )
        return metrics
