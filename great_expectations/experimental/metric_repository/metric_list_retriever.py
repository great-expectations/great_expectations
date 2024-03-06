from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Sequence

from great_expectations.compatibility.typing_extensions import override
from great_expectations.experimental.metric_repository.metric_retriever import (
    MetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    Metric,
    MetricTypes,
    TableMetric,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.batch_request import BatchRequest
    from great_expectations.validator.validator import Validator


class MetricListMetricRetriever(MetricRetriever):
    def __init__(self, context: AbstractDataContext):
        super().__init__(context=context)
        self._validator: Validator | None = None

    def get_validator(self, batch_request: BatchRequest) -> Validator:
        if self._validator is None:
            self._validator = self._context.get_validator(batch_request=batch_request)
        return self._validator

    @override
    def get_metrics(
        self,
        batch_request: BatchRequest,
        metric_list: Optional[List[MetricTypes]] = None,
    ) -> Sequence[Metric]:
        metrics: List[Metric] = []
        if not metric_list:
            raise ValueError("metric_list cannot be empty")
        # first check that these are all valid metric types
        self._check_valid_metric_types(metric_list)
        for metric in metric_list:
            if metric == MetricTypes.TABLE_ROW_COUNT:
                metrics.append(self._get_table_row_count(batch_request=batch_request))
            elif metric == MetricTypes.TABLE_COLUMNS:
                metrics.append(self._get_table_columns(batch_request=batch_request))
            # elif metric == MetricTypes.COLUMN_MIN:
            #     metrics.append(self._get_column_min(batch_request=batch_request))
            # elif metric == MetricTypes.COLUMN_MAX:
            #     metrics.append(self._get_column_max(batch_request=batch_request))
            # elif metric == MetricTypes.COLUMN_MEDIAN:
            #     metrics.append(self._get_column_median(batch_request=batch_request))
            # elif metric == MetricTypes.COLUMN_MEAN:
            #     metrics.append(self._get_column_mean(batch_request=batch_request))
            # elif metric == MetricTypes.COLUMN_NULL_COUNT:
            #     metrics.append(self._get_column_null_count(batch_request=batch_request))
        return metrics

    def _get_table_row_count(self, batch_request: BatchRequest):
        table_metric_configs = self._generate_table_metric_configurations(
            table_metric_names=[MetricTypes.TABLE_ROW_COUNT]
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, table_metric_configs
        )
        metric_name = MetricTypes.TABLE_ROW_COUNT
        value, exception = self._get_metric_from_computed_metrics(
            metric_name=metric_name,
            computed_metrics=computed_metrics,
            aborted_metrics=aborted_metrics,
        )
        return TableMetric[int](
            batch_id=batch_id,
            metric_name=metric_name,
            value=value,
            exception=exception,
        )

    def _get_table_columns(self, batch_request: BatchRequest) -> Metric:
        table_metric_configs = self._generate_table_metric_configurations(
            table_metric_names=[MetricTypes.TABLE_COLUMNS]
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, table_metric_configs
        )
        metric_name = MetricTypes.TABLE_COLUMNS
        value, exception = self._get_metric_from_computed_metrics(
            metric_name=metric_name,
            computed_metrics=computed_metrics,
            aborted_metrics=aborted_metrics,
        )
        return TableMetric[List[str]](
            batch_id=batch_id,
            metric_name=metric_name,
            value=value,
            exception=exception,
        )

    def _check_valid_metric_types(self, metric_list: List[MetricTypes]) -> bool:
        for metric in metric_list:
            if metric not in MetricTypes:
                return False
        return True
