from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from great_expectations.experimental.column_descriptive_metrics.metrics import (
    Metric,
    Metrics,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.experimental.column_descriptive_metrics.metric_converter import (
        MetricConverter,
    )
    from great_expectations.validator.validator import Validator


class BatchInspector:
    def __init__(self, metric_converter: MetricConverter):
        self._metric_converter = metric_converter

    def _generate_run_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def get_column_descriptive_metrics(self, validator: Validator) -> Metrics:
        run_id = self._generate_run_id()
        table_row_count = self._get_table_row_count_metric(
            run_id=run_id, validator=validator
        )
        column_names = self._get_column_names_metric(run_id=run_id, validator=validator)
        metrics = Metrics(metrics=[table_row_count, column_names])
        return metrics

    def _get_metric(
        self, metric_name: str, run_id: uuid.UUID, validator: Validator
    ) -> Metric:
        # TODO: Thu - do we need the MetricConfiguration or can we just pass in the metric name?
        #  E.g. metrics_calculator.get_table_metric(metric_name)
        metric_config = MetricConfiguration(
            metric_name=metric_name,
            metric_domain_kwargs={},
            metric_value_kwargs={},
        )

        # raw_metric = metrics_calculator.get_metric(metric_config)
        raw_metric = validator.get_metric(metric_config)

        metric = self._metric_converter.convert_raw_metric_to_metric_object(
            raw_metric=raw_metric,
            metric_config=metric_config,
            run_id=run_id,
            batch=validator.active_batch,
        )

        return metric

    def _get_table_row_count_metric(self, run_id: uuid.UUID, validator) -> Metric:
        return self._get_metric(
            metric_name="table.row_count",
            run_id=run_id,
            validator=validator,
        )

    def _get_column_names_metric(self, run_id: uuid.UUID, validator) -> Metric:
        return self._get_metric(
            metric_name="table.columns", run_id=run_id, validator=validator
        )
