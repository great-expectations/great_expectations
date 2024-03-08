from __future__ import annotations

import logging
from collections.abc import Hashable
from itertools import chain
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Tuple, Union

from great_expectations.compatibility.typing_extensions import override
from great_expectations.experimental.metric_repository.metric_retriever import (
    MetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    Metric,
    MetricTypes,
    TableMetric,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent.batch_request import BatchRequest
    from great_expectations.validator.validator import Validator

logger = logging.getLogger(__name__)
logging.captureWarnings(True)

_MetricKey: TypeAlias = Union[Tuple[str, Hashable, Hashable], Tuple[str, str, str]]


class MetricListMetricRetriever(MetricRetriever):
    def __init__(self, context: AbstractDataContext):
        super().__init__(context=context)
        self._validator: Validator | None = None

    def _column_metrics_in_metric_list(self, metric_list: List[MetricTypes]) -> bool:
        # all the column metrics we are considering
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

    @override
    def get_metrics(
        self,
        batch_request: BatchRequest,
        metric_list: Optional[List[MetricTypes]] = None,
    ) -> Sequence[Metric]:
        metrics_result: List[Metric] = []
        if not metric_list:
            raise ValueError("metric_list cannot be empty")
        # first check that these are all valid metric types
        self._check_valid_metric_types(metric_list)
        table_metrics = self._get_table_metrics(
            batch_request=batch_request, metric_list=metric_list
        )
        # TODO: check if this is efficient
        # make sure to take care of this case
        for metric in table_metrics:
            metrics_result.append(metric)

        # exit early if only Table Metrics exist
        if not self._column_metrics_in_metric_list(metric_list):
            return metrics_result

        # keep this in for now. We will always guarantee that TABLE_COLUMN_TYPES is present
        # We need to skip columns that do not report a type, because the metric computation
        # to determine semantic type will fail.
        table_column_types = list(
            filter(
                lambda m: m.metric_name == MetricTypes.TABLE_COLUMN_TYPES, table_metrics
            )
        )[0]

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
        column_metric_names = {MetricTypes.COLUMN_NULL_COUNT}
        metrics_list_as_set = set(metrics_list)
        metrics_to_calculate = column_metric_names.intersection(metrics_list_as_set)

        column_metric_configs = self._generate_column_metric_configurations(
            column_list, list(metrics_to_calculate)
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, column_metric_configs
        )

        # Convert computed_metrics
        metrics: list[Metric] = []
        ColumnMetric.update_forward_refs()
        metric_lookup_key: _MetricKey

        for metric_name in metrics_to_calculate:
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

    def _get_numeric_column_metrics(
        self,
        metrics_list: List[MetricTypes],
        batch_request: BatchRequest,
        column_list: List[str],
    ) -> Sequence[Metric]:
        column_metric_names = {
            MetricTypes.COLUMN_MIN,
            MetricTypes.COLUMN_MAX,
            MetricTypes.COLUMN_MEAN,
            MetricTypes.COLUMN_MEDIAN,
        }
        metrics_list_as_set = set(metrics_list)
        metrics_to_calculate = column_metric_names.intersection(metrics_list_as_set)

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
        column_metric_names = {
            "column.min",
            "column.max",
            # "column.mean",  # Currently not supported for timestamp in Snowflake
            # "column.median",  # Currently not supported for timestamp in Snowflake
        }
        metrics_list_as_set = set(metrics_list)
        metrics_to_calculate = column_metric_names.intersection(metrics_list_as_set)

        # Note: Timestamps are returned as strings for Snowflake, this may need to be adjusted
        # when we support other datasources. For example in Pandas, timestamps can be returned as Timestamp().
        return self._get_column_metrics(
            batch_request=batch_request,
            column_list=column_list,
            column_metric_names=list(metrics_to_calculate),
            column_metric_type=ColumnMetric[str],
        )

    def _get_table_metrics(
        self, batch_request: BatchRequest, metric_list: List[MetricTypes]
    ) -> List[Metric]:
        metrics: List[Metric] = []
        # in is typically a o(1) operation.
        if MetricTypes.TABLE_ROW_COUNT in metric_list:
            metrics.append(self._get_table_row_count(batch_request=batch_request))
        if MetricTypes.TABLE_COLUMNS in metric_list:
            metrics.append(self._get_table_columns(batch_request=batch_request))
        if MetricTypes.TABLE_COLUMN_TYPES in metric_list:
            metrics.append(self._get_table_column_types(batch_request=batch_request))
        return metrics

    def _get_table_row_count(self, batch_request: BatchRequest) -> Metric:
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

    def _get_table_column_types(self, batch_request: BatchRequest) -> Metric:
        table_metric_configs = self._generate_table_metric_configurations(
            table_metric_names=[MetricTypes.TABLE_COLUMN_TYPES]
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, table_metric_configs
        )
        metric_name = MetricTypes.TABLE_COLUMN_TYPES
        metric_lookup_key: _MetricKey = (metric_name, tuple(), "include_nested=True")
        value, exception = self._get_metric_from_computed_metrics(
            metric_name=metric_name,
            metric_lookup_key=metric_lookup_key,
            computed_metrics=computed_metrics,
            aborted_metrics=aborted_metrics,
        )
        raw_column_types: list[dict[str, Any]] = value
        # If type is not found, don't add empty type field. This can happen if our db introspection fails.
        column_types_converted_to_str: list[dict[str, str]] = []
        for raw_column_type in raw_column_types:
            if raw_column_type.get("type"):
                column_types_converted_to_str.append(
                    {
                        "name": raw_column_type["name"],
                        "type": str(raw_column_type["type"]),
                    }
                )
            else:
                column_types_converted_to_str.append({"name": raw_column_type["name"]})

        return TableMetric[List[str]](
            batch_id=batch_id,
            metric_name=metric_name,
            value=column_types_converted_to_str,  # type: ignore
            exception=exception,
        )

    def _check_valid_metric_types(self, metric_list: List[MetricTypes]) -> bool:
        for metric in metric_list:
            if metric not in MetricTypes:
                return False
        return True

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
