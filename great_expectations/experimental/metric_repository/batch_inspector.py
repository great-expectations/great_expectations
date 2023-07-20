from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from great_expectations.experimental.metric_repository.metrics import (
    Metric,
    MetricException,
    Metrics,
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

    def get_column_descriptive_metrics(self, batch_request: BatchRequest) -> Metrics:
        run_id = self._generate_run_id()

        table_row_count = self._get_table_row_count_metric(
            run_id=run_id, batch_request=batch_request
        )
        column_names = self._get_column_names_metric(
            run_id=run_id, batch_request=batch_request
        )
        metrics_list = [table_row_count, column_names]
        metrics = Metrics(id=run_id, metrics=metrics_list)
        return metrics

    def _get_table_row_count_metric(
        self, run_id: uuid.UUID, batch_request: BatchRequest
    ) -> Metric:
        metric_name = "table.row_count"
        validator = self._context.get_validator(batch_request=batch_request)
        metric_config = MetricConfiguration(
            metric_name=metric_name,
            metric_domain_kwargs={},
            metric_value_kwargs={},
        )
        raw_metric = validator.get_metric(metric_config)

        return NumericTableMetric(
            id=self._generate_metric_id(),
            run_id=run_id,
            # TODO: reimplement batch param
            # batch=batch,
            metric_name=metric_name,
            value=raw_metric,
            exception=MetricException(),  # TODO: Pass through
        )

    def _get_column_names_metric(
        self, run_id: uuid.UUID, batch_request: BatchRequest
    ) -> Metric:
        metric_name = "table.columns"
        validator = self._context.get_validator(batch_request=batch_request)
        metric_config = MetricConfiguration(
            metric_name=metric_name,
            metric_domain_kwargs={},
            metric_value_kwargs={},
        )
        raw_metric = validator.get_metric(metric_config)

        return StringListTableMetric(
            id=self._generate_metric_id(),
            run_id=run_id,
            # TODO: reimplement batch param
            # batch=batch,
            metric_name=metric_name,
            value=raw_metric,
            exception=MetricException(),  # TODO: Pass through
        )

    def _generate_metric_id(self) -> uuid.UUID:
        return uuid.uuid4()
