from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, List, Sequence

from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.metric_retriever import (
    MetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    Metric,
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
        return list(chain(table_metrics_list, column_metrics_list))

    def _get_column_list(self, metrics: Sequence[Metric]) -> List[str]:
        column_list: List[str] = []
        for metric in metrics:
            if metric.metric_name == "table.columns":
                column_list = metric.value
        return column_list

    def _get_table_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metric_names = [
            "table.row_count",
            "table.columns",
        ]  # , "table.column_types"]
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
        metrics: list[Metric] = []
        metric_name = "table.row_count"
        TableMetric.update_forward_refs()

        assert isinstance(validator.active_batch, Batch)
        if not isinstance(validator.active_batch, Batch):
            raise TypeError(
                f"validator.active_batch is type {type(validator.active_batch).__name__} instead of type {Batch.__name__}"
            )

        metric_lookup_key: _MetricKey = (
            metric_name,
            tuple(),
            tuple(),
        )

        metrics.append(
            TableMetric[int](
                batch_id=validator.active_batch.id,
                metric_name=metric_name,
                value=computed_metrics[metric_lookup_key],  # type: ignore[arg-type] # Pydantic verifies the value type
                exception=None,  # TODO: Pass through a MetricException() if an exception is thrown
            )
        )

        metric_name = "table.columns"
        metric_lookup_key = (metric_name, tuple(), tuple())
        metrics.append(
            TableMetric[List[str]](
                batch_id=validator.active_batch.id,
                metric_name=metric_name,
                value=computed_metrics[metric_lookup_key],  # type: ignore[arg-type] # Pydantic verifies the value type
                exception=None,  # TODO: Pass through a MetricException() if an exception is thrown
            )
        )
        return metrics

    def _get_column_metrics(
        self, batch_request: BatchRequest, column_list: List[str]
    ) -> Sequence[Metric]:
        column_metric_names = ["column.min", "column.max"]

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
        for metric_name in column_metric_names:
            for column in column_list:
                metric_lookup_key = (metric_name, f"column={column}", tuple())
                metrics.append(
                    ColumnMetric[float](
                        batch_id=validator.active_batch.id,
                        metric_name=metric_name,
                        column=column,
                        value=computed_metrics[metric_lookup_key],  # type: ignore[arg-type] # Pydantic verifies the value type
                        exception=None,  # TODO: Pass through a MetricException() if an exception is thrown
                    )
                )

        return metrics
