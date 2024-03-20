from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.batch_request import BatchRequest
    from great_expectations.validator.validator import (
        Validator,
    )


class MetricListMetricRetriever(MetricRetriever):
    def __init__(self, context: AbstractDataContext):
        super().__init__(context=context)
        self._validator: Validator | None = None

    @override
    def get_metrics(
        self,
        batch_request: BatchRequest,
        metric_list: Optional[List[MetricTypes]] = None,
    ) -> Sequence[Metric]:
        metrics_result: List[Metric] = []

        if not metric_list:
            raise ValueError("metric_list cannot be empty")

        self._check_valid_metric_types(metric_list)

        table_metrics = self._calculate_table_metrics(
            batch_request=batch_request, metric_list=metric_list
        )
        metrics_result.extend(table_metrics)

        if not self._column_metrics_in_metric_list(metric_list):
            # if no column metrics are present in the metric list, we can return the table metrics
            return metrics_result

        if MetricTypes.TABLE_COLUMN_TYPES not in metric_list:
            logger.warning(
                "TABLE_COLUMN_TYPES metric is required to compute column metrics. Skipping column metrics."
            )
            return metrics_result

        table_column_types = list(
            filter(
                lambda m: m.metric_name == MetricTypes.TABLE_COLUMN_TYPES, table_metrics
            )
        )[0]

        # We need to skip columns that do not report a type, because the metric computation
        # to determine semantic type will fail.
        exclude_column_names = self._get_columns_to_exclude(table_column_types)

        numeric_column_names = self._get_numeric_column_names(
            batch_request=batch_request, exclude_column_names=exclude_column_names
        )
        timestamp_column_names = self._get_timestamp_column_names(
            batch_request=batch_request, exclude_column_names=exclude_column_names
        )
        numeric_column_metrics = self._get_numeric_column_metrics(
            metric_list, batch_request, numeric_column_names
        )
        timestamp_column_metrics = self._get_timestamp_column_metrics(
            metric_list, batch_request, timestamp_column_names
        )
        all_column_names: List[str] = self._get_all_column_names(table_metrics)
        non_numeric_column_metrics = self._get_non_numeric_column_metrics(
            metric_list, batch_request, all_column_names
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

    def _get_non_numeric_column_metrics(
        self,
        metrics_list: List[MetricTypes],
        batch_request: BatchRequest,
        column_list: List[str],
    ) -> Sequence[Metric]:
        """Calculate column metrics for non-numeric columns.

        Args:
            metrics_list (List[MetricTypes]): list of metrics sent from Agent.
            batch_request (BatchRequest): for current batch.
            column_list (List[str]): list of non-numeric columns.

        Returns:
            Sequence[Metric]: List of metrics for non-numeric columns.
        """
        # currently only the null-count is supported. If more metrics are added, this set will need to be updated.
        column_metric_names = {MetricTypes.COLUMN_NULL_COUNT}
        metrics: list[Metric] = []
        metrics_list_as_set = set(metrics_list)
        metrics_to_calculate = sorted(
            column_metric_names.intersection(metrics_list_as_set)
        )

        if not metrics_to_calculate:
            return metrics
        else:
            return self._get_column_metrics(
                batch_request=batch_request,
                column_list=column_list,
                column_metric_names=list(metrics_to_calculate),
                column_metric_type=ColumnMetric[int],
            )

    def _get_numeric_column_metrics(
        self,
        metrics_list: List[MetricTypes],
        batch_request: BatchRequest,
        column_list: List[str],
    ) -> Sequence[Metric]:
        """Calculate column metrics for numeric columns.

        Args:
            metrics_list (List[MetricTypes]): list of metrics sent from Agent.
            batch_request (BatchRequest): for current batch.
            column_list (List[str]): list of numeric columns.

        Returns:
            Sequence[Metric]: List of metrics for numeric columns.
        """
        metrics: list[Metric] = []
        column_metric_names = {
            MetricTypes.COLUMN_MIN,
            MetricTypes.COLUMN_MAX,
            MetricTypes.COLUMN_MEAN,
            MetricTypes.COLUMN_MEDIAN,
        }
        metrics_list_as_set = set(metrics_list)
        metrics_to_calculate = sorted(
            column_metric_names.intersection(metrics_list_as_set)
        )
        if not metrics_to_calculate:
            return metrics

        return self._get_column_metrics(
            batch_request=batch_request,
            column_list=column_list,
            column_metric_names=list(metrics_to_calculate),
            column_metric_type=ColumnMetric[float],
        )

    def _get_timestamp_column_metrics(
        self,
        metrics_list: List[MetricTypes],
        batch_request: BatchRequest,
        column_list: List[str],
    ) -> Sequence[Metric]:
        """Calculate column metrics for timestamp columns.

        Args:
            metrics_list (List[MetricTypes]): list of metrics sent from Agent.
            batch_request (BatchRequest): for current batch.
            column_list (List[str]): list of timestamp columns.

        Returns:
            Sequence[Metric]: List of metrics for timestamp columns.
        """
        metrics: list[Metric] = []
        column_metric_names = {
            MetricTypes.COLUMN_MIN,
            MetricTypes.COLUMN_MAX,
            # MetricTypes.COLUMN_MEAN,  # Currently not supported for timestamp in Snowflake
            #  MetricTypes.COLUMN_MEDIAN,  # Currently not supported for timestamp in Snowflake
        }
        metrics_list_as_set = set(metrics_list)
        metrics_to_calculate = sorted(
            column_metric_names.intersection(metrics_list_as_set)
        )
        if not metrics_to_calculate:
            return metrics

        # Note: Timestamps are returned as strings for Snowflake, this may need to be adjusted
        # when we support other datasources. For example in Pandas, timestamps can be returned as Timestamp().
        return self._get_column_metrics(
            batch_request=batch_request,
            column_list=column_list,
            column_metric_names=list(metrics_to_calculate),
            column_metric_type=ColumnMetric[str],
        )

    def _calculate_table_metrics(
        self, batch_request: BatchRequest, metric_list: List[MetricTypes]
    ) -> List[Metric]:
        """Calculate table metrics, which include row_count, column names and types.

        Args:
            metrics_list (List[MetricTypes]): list of metrics sent from Agent.
            batch_request (BatchRequest): for current batch.

        Returns:
            Sequence[Metric]: List of table metrics.
        """
        metrics: List[Metric] = []
        if MetricTypes.TABLE_ROW_COUNT in metric_list:
            metrics.append(self._get_table_row_count(batch_request=batch_request))
        if MetricTypes.TABLE_COLUMNS in metric_list:
            metrics.append(self._get_table_columns(batch_request=batch_request))
        if MetricTypes.TABLE_COLUMN_TYPES in metric_list:
            metrics.append(self._get_table_column_types(batch_request=batch_request))
        return metrics

    def _check_valid_metric_types(self, metric_list: List[MetricTypes]) -> bool:
        """Check whether all the metric types in the list are valid.

        Args:
            metric_list (List[MetricTypes]): list of MetricTypes that are passed in to MetricListMetricRetriever.

        Returns:
            bool: True if all the metric types in the list are valid, False otherwise.
        """
        for metric in metric_list:
            if metric not in MetricTypes:
                return False
        return True

    def _column_metrics_in_metric_list(self, metric_list: List[MetricTypes]) -> bool:
        """Helper method to check whether any column metrics are present in the metric list.

        Args:
            metric_list (List[MetricTypes]): list of MetricTypes that are passed in to MetricListMetricRetriever.

        Returns:
            bool: True if any column metrics are present in the metric list, False otherwise.
        """
        column_metrics: List[MetricTypes] = [
            MetricTypes.COLUMN_MIN,
            MetricTypes.COLUMN_MAX,
            MetricTypes.COLUMN_MEDIAN,
            MetricTypes.COLUMN_MEAN,
            MetricTypes.COLUMN_NULL_COUNT,
        ]
        for metric in column_metrics:
            if metric in metric_list:
                return True
        return False
