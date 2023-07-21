from __future__ import annotations

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
    def __init__(self, context: DataContext):
        self._context = context

    def _generate_run_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def get_column_descriptive_metrics(self, batch_request: BatchRequest) -> MetricRun:
        run_id = self._generate_run_id()

        table_metrics_list = self._get_table_metrics(batch_request, run_id)

        metrics = MetricRun(id=run_id, metrics=table_metrics_list)

        return metrics

    def _get_table_metrics(
        self, batch_request: BatchRequest, run_id: uuid.UUID
    ) -> list[Metric]:
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
        metrics.append(
            NumericTableMetric(
                id=self._generate_metric_id(),
                run_id=run_id,
                # TODO: reimplement batch param
                # batch=batch,
                metric_name=metric_name,
                value=computed_metrics[(metric_name, tuple(), tuple())],
                exception=MetricException(),  # TODO: Pass through
            )
        )
        metric_name = "table.columns"
        metrics.append(
            StringListTableMetric(
                id=self._generate_metric_id(),
                run_id=run_id,
                # TODO: reimplement batch param
                # batch=batch,
                metric_name=metric_name,
                value=computed_metrics[(metric_name, tuple(), tuple())],
                exception=MetricException(),  # TODO: Pass through
            )
        )
        return metrics

    def _generate_metric_id(self) -> uuid.UUID:
        return uuid.uuid4()
