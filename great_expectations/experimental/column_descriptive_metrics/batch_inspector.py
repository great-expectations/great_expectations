from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from great_expectations.experimental.column_descriptive_metrics.metric_converter import (
    MetricConverter,
)
from great_expectations.experimental.column_descriptive_metrics.metrics import (
    Metric,
    Metrics,
)
from great_expectations.validator.metric_configuration import MetricConfiguration
from great_expectations.validator.metrics_calculator import MetricsCalculator

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.interfaces import Batch


class BatchInspector:
    def __init__(self, organization_id: uuid.UUID):
        self._metric_converter = MetricConverter(organization_id=organization_id)

    def _generate_run_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def get_column_descriptive_metrics(self, batch: Batch) -> Metrics:
        run_id = self._generate_run_id()
        table_row_count = self._get_table_row_count_metric(batch=batch, run_id=run_id)
        column_names = self._get_column_names_metric(batch=batch, run_id=run_id)
        metrics = Metrics(metrics=[table_row_count, column_names])
        return metrics

    def _get_metric(self, metric_name: str, batch: Batch, run_id: uuid.UUID) -> Metric:
        metrics_calculator = MetricsCalculator(
            execution_engine=batch.datasource.get_execution_engine()
        )

        # TODO: Thu - do we need the MetricConfiguration or can we just pass in the metric name?
        #  E.g. metrics_calculator.get_table_metric(metric_name)
        metric_config = MetricConfiguration(
            metric_name=metric_name,
            metric_domain_kwargs={},
            metric_value_kwargs={},
        )

        raw_metric = metrics_calculator.get_metric(metric_config)

        metric = self._metric_converter.convert_raw_metric_to_metric_object(
            raw_metric=raw_metric,
            metric_config=metric_config,
            run_id=run_id,
            batch=batch,
        )

        return metric

    def _get_table_row_count_metric(self, batch: Batch, run_id: uuid.UUID) -> Metric:
        return self._get_metric(
            metric_name="table.row_count", batch=batch, run_id=run_id
        )

    def _get_column_names_metric(self, batch: Batch, run_id: uuid.UUID) -> Metric:
        return self._get_metric(metric_name="table.columns", batch=batch, run_id=run_id)
