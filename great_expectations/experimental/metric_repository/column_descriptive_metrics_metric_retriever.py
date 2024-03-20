from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, List, Optional, Sequence

from great_expectations.compatibility.typing_extensions import override
from great_expectations.experimental.metric_repository.metric_retriever import (
    MetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    Metric,
    MetricTypes,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.validator.metrics_calculator import (
        _MetricKey,
    )


class ColumnDescriptiveMetricsMetricRetriever(MetricRetriever):
    """Compute and retrieve Column Descriptive Metrics for a batch of data."""

    def __init__(self, context: AbstractDataContext):
        super().__init__(context=context)

    @override
    def get_metrics(
        self,
        batch_request: BatchRequest,
        metric_list: Optional[List[MetricTypes]] = None,
    ) -> Sequence[Metric]:
        # Note: Signature includes metric_list for compatibility with the MetricRetriever interface.
        # It is not used by ColumnDescriptiveMetricsMetricRetriever.

        table_metrics = self._calculate_table_metrics(batch_request)

        # We need to skip columns that do not report a type, because the metric computation
        # to determine semantic type will fail.
        table_column_types = list(
            filter(lambda m: m.metric_name == "table.column_types", table_metrics)
        )[0]
        exclude_column_names = self._get_columns_to_exclude(table_column_types)

        numeric_column_names = self._get_numeric_column_names(
            batch_request=batch_request, exclude_column_names=exclude_column_names
        )
        timestamp_column_names = self._get_timestamp_column_names(
            batch_request=batch_request, exclude_column_names=exclude_column_names
        )

        numeric_column_metrics = self._get_numeric_column_metrics(
            batch_request=batch_request, column_list=numeric_column_names
        )
        timestamp_column_metrics = self._get_timestamp_column_metrics(
            batch_request=batch_request, column_list=timestamp_column_names
        )
        all_column_names: List[str] = self._get_all_column_names(table_metrics)
        non_numeric_column_metrics = self._get_non_numeric_column_metrics(
            batch_request=batch_request, column_list=all_column_names
        )

        bundled_list = list(
            chain(
                table_metrics,
                numeric_column_metrics,
                timestamp_column_metrics,
                non_numeric_column_metrics,
            )
        )
        return bundled_list

    def _calculate_table_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        metrics = [
            self._get_table_row_count(batch_request),
            self._get_table_columns(batch_request),
            self._get_table_column_types(batch_request),
        ]
        return metrics

    def _get_numeric_column_metrics(
        self, batch_request: BatchRequest, column_list: List[str]
    ) -> Sequence[Metric]:
        column_metric_names = [
            "column.min",
            "column.max",
            "column.mean",
            "column.median",
        ]
        return self._get_column_metrics(
            batch_request=batch_request,
            column_list=column_list,
            column_metric_names=column_metric_names,
            column_metric_type=ColumnMetric[float],
        )

    def _get_timestamp_column_metrics(
        self, batch_request: BatchRequest, column_list: List[str]
    ) -> Sequence[Metric]:
        column_metric_names = [
            "column.min",
            "column.max",
            # "column.mean",  # Currently not supported for timestamp in Snowflake
            # "column.median",  # Currently not supported for timestamp in Snowflake
        ]
        # Note: Timestamps are returned as strings for Snowflake, this may need to be adjusted
        # when we support other datasources. For example in Pandas, timestamps can be returned as Timestamp().
        return self._get_column_metrics(
            batch_request=batch_request,
            column_list=column_list,
            column_metric_names=column_metric_names,
            column_metric_type=ColumnMetric[str],
        )

    def _get_non_numeric_column_metrics(
        self, batch_request: BatchRequest, column_list: List[str]
    ) -> Sequence[Metric]:
        column_metric_names = [
            "column_values.null.count",
        ]

        column_metric_configs = self._generate_column_metric_configurations(
            column_list, column_metric_names
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, column_metric_configs
        )

        # Convert computed_metrics
        metrics: list[Metric] = []
        ColumnMetric.update_forward_refs()
        metric_lookup_key: _MetricKey

        for metric_name in column_metric_names:
            for column in column_list:
                metric_lookup_key = (metric_name, f"column={column}", tuple())
                value, exception = self._get_metric_from_computed_metrics(
                    metric_name=metric_name,
                    metric_lookup_key=metric_lookup_key,
                    computed_metrics=computed_metrics,
                    aborted_metrics=aborted_metrics,
                )
                metrics.append(
                    ColumnMetric[int](
                        batch_id=batch_id,
                        metric_name=metric_name,
                        column=column,
                        value=value,
                        exception=exception,
                    )
                )

        return metrics
