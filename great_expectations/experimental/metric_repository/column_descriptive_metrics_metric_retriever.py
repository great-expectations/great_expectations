from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, Any, List, Sequence, Type

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.metric_retriever import (
    MetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    Metric,
    MetricException,
    TableMetric,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.validator.metrics_calculator import _MetricKey


class ColumnDescriptiveMetricsMetricRetriever(MetricRetriever):
    """Compute and retrieve Column Descriptive Metrics for a batch of data."""

    @override
    def get_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metrics_list = self._get_table_metrics(batch_request)
        column_list: List[str] = self._get_column_list(table_metrics_list)
        column_metrics_list = self._get_column_metrics(
            batch_request=batch_request, column_list=column_list
        )
        bundled_list = list(chain(table_metrics_list, column_metrics_list))
        return bundled_list

    def _get_column_list(self, metrics: Sequence[Metric]) -> List[str]:
        column_list: List[str] = []
        for metric in metrics:
            if metric.metric_name == "table.columns":
                column_list = metric.value
        return column_list

    def _get_table_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metric_names = ["table.row_count", "table.columns", "table.column_types"]
        computed_metrics, batch_id = self._compute_table_metrics(
            table_metric_names, batch_request
        )

        # Convert computed_metrics
        metrics: list[Metric] = []
        TableMetric.update_forward_refs()

        metric_name = "table.row_count"
        value, exception = self._get_table_metric_from_computed_metrics(
            metric_name=metric_name,
            metric_lookup_key=None,
            computed_metrics=computed_metrics,
        )

        metrics.append(
            TableMetric[int](
                batch_id=batch_id,
                metric_name=metric_name,
                value=value,
                exception=exception,
            )
        )

        metric_name = "table.columns"
        metric_lookup_key: _MetricKey = (metric_name, tuple(), tuple())
        metrics.append(
            TableMetric[List[str]](
                batch_id=batch_id,
                metric_name=metric_name,
                value=computed_metrics[metric_lookup_key],
                exception=None,  # TODO: Pass through a MetricException() if an exception is thrown
            )
        )

        metric_name = "table.column_types"
        metric_lookup_key = (metric_name, tuple(), "include_nested=True")

        raw_column_types: list[dict[str, Any]] = computed_metrics[metric_lookup_key]  # type: ignore[assignment] # Metric results from computed_metrics are not typed

        column_types_converted_to_str: list[dict[str, str]] = [
            {"name": raw_column_type["name"], "type": str(raw_column_type["type"])}
            for raw_column_type in raw_column_types
        ]
        metrics.append(
            TableMetric[List[str]](
                batch_id=batch_id,
                metric_name=metric_name,
                value=column_types_converted_to_str,
                exception=None,  # TODO: Pass through a MetricException() if an exception is thrown
            )
        )
        return metrics

    def _compute_table_metrics(
        self, table_metric_names: list[str], batch_request: BatchRequest
    ) -> tuple[dict[_MetricKey, Any], str | tuple]:
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

        assert isinstance(validator.active_batch, Batch)
        if not isinstance(validator.active_batch, Batch):
            raise TypeError(
                f"validator.active_batch is type {type(validator.active_batch).__name__} instead of type {Batch.__name__}"
            )
        batch_id = validator.active_batch.id
        return computed_metrics, batch_id

    def _get_table_metric_from_computed_metrics(
        self,
        metric_name: str,
        metric_lookup_key: _MetricKey | None,
        computed_metrics: dict[_MetricKey, Any],
    ):
        if metric_lookup_key is None:
            metric_lookup_key: _MetricKey = (
                metric_name,
                tuple(),
                tuple(),
            )
        value = None
        exception = None
        if metric_lookup_key in computed_metrics:
            value = computed_metrics[metric_lookup_key]
        else:
            exception = MetricException(
                type="TBD", message="TBD"
            )  # TODO: Fill in type and message if an exception is thrown

        return value, exception

    def _get_column_metrics(
        self, batch_request: BatchRequest, column_list: List[str]
    ) -> Sequence[Metric]:
        column_metric_names = [
            "column.min",
            "column.max",
            "column.mean",
            "column.median",
            "column_values.null.count",
        ]

        column_metric_configs: List[MetricConfiguration] = list()

        # TODO: nested for-loop can be better
        for metric_name in column_metric_names:
            for column in column_list:
                column_metric_configs.append(
                    MetricConfiguration(
                        metric_name=metric_name,
                        metric_domain_kwargs={"column": column},
                        metric_value_kwargs={},
                    )
                )
        validator = self._context.get_validator(batch_request=batch_request)
        computed_metrics = validator.compute_metrics(
            metric_configurations=column_metric_configs,
            runtime_configuration={"catch_exceptions": True},  # TODO: Is this needed?
        )
        # Convert computed_metrics
        metrics: list[Metric] = []
        ColumnMetric.update_forward_refs()

        assert isinstance(validator.active_batch, Batch)
        if not isinstance(validator.active_batch, Batch):
            raise TypeError(
                f"validator.active_batch is type {type(validator.active_batch).__name__} instead of type {Batch.__name__}"
            )
        metric_lookup_key: _MetricKey

        # TODO: nested for-loop can be better
        value_type: Type[int] | Type[float] = float
        for metric_name in column_metric_names:
            if metric_name == "column_values.null.count":
                value_type = int
            for column in column_list:
                metric_lookup_key = (metric_name, f"column={column}", tuple())
                metrics.append(
                    ColumnMetric[value_type](  # type: ignore[valid-type]  # Will be refactored in upcoming PR
                        batch_id=validator.active_batch.id,
                        metric_name=metric_name,
                        column=column,
                        value=computed_metrics[metric_lookup_key],
                        exception=None,  # TODO: Pass through a MetricException() if an exception is thrown
                    )
                )

        return metrics
