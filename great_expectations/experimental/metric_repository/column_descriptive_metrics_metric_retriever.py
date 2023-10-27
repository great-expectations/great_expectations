from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, Any, List, Sequence

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.domain import SemanticDomainTypes
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
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.validator.metrics_calculator import (
        _AbortedMetricsInfoDict,
        _MetricKey,
        _MetricsDict,
    )
    from great_expectations.validator.validator import Validator


class ColumnDescriptiveMetricsMetricRetriever(MetricRetriever):
    """Compute and retrieve Column Descriptive Metrics for a batch of data."""

    def __init__(self, context: AbstractDataContext):
        super().__init__(context=context)
        self._validator: Validator | None = None

    def get_validator(self, batch_request: BatchRequest) -> Validator:
        if self._validator is None:
            self._validator = self._context.get_validator(batch_request=batch_request)
        return self._validator

    @override
    def get_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metrics = self._get_table_metrics(batch_request)

        # We need to skip columns that do not report a type, because the metric computation
        # to determine semantic type will fail.
        table_column_types = list(
            filter(lambda m: m.metric_name == "table.column_types", table_metrics)
        )[0]
        exclude_column_names = self._get_columns_to_exclude(table_column_types)

        numeric_column_names = self._get_numeric_column_names(
            batch_request=batch_request, exclude_column_names=exclude_column_names
        )
        numeric_column_metrics = self._get_numeric_column_metrics(
            batch_request=batch_request, column_list=numeric_column_names
        )

        all_column_names: List[str] = self._get_all_column_names(table_metrics)
        non_numeric_column_metrics = self._get_non_numeric_column_metrics(
            batch_request=batch_request, column_list=all_column_names
        )

        bundled_list = list(
            chain(
                table_metrics,
                numeric_column_metrics,
                non_numeric_column_metrics,
            )
        )
        return bundled_list

    def _get_table_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metric_names = ["table.row_count", "table.columns", "table.column_types"]
        table_metric_configs = self._generate_table_metric_configurations(
            table_metric_names
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, table_metric_configs
        )

        metrics = [
            self._get_table_row_count(batch_id, computed_metrics, aborted_metrics),
            self._get_table_columns(batch_id, computed_metrics, aborted_metrics),
            self._get_table_column_types(batch_id, computed_metrics, aborted_metrics),
        ]

        return metrics

    def _get_table_row_count(
        self,
        batch_id: str,
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
    ) -> Metric:
        metric_name = "table.row_count"
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

    def _get_table_columns(
        self,
        batch_id: str,
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
    ) -> Metric:
        metric_name = "table.columns"
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

    def _get_table_column_types(
        self,
        batch_id: str,
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
    ) -> Metric:
        metric_name = "table.column_types"
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
            value=column_types_converted_to_str,
            exception=exception,
        )

    def _get_columns_to_exclude(self, table_column_types: Metric) -> List[str]:
        columns_to_skip: List[str] = []
        for column_type in table_column_types.value:
            if not column_type.get("type"):
                columns_to_skip.append(column_type["name"])
        return columns_to_skip

    def _get_numeric_column_metrics(
        self, batch_request: BatchRequest, column_list: List[str]
    ) -> Sequence[Metric]:
        column_metric_names = [
            "column.min",
            "column.max",
            "column.mean",
            "column.median",
        ]

        column_metric_configs = self._generate_column_metric_configurations(
            column_list, column_metric_names
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, column_metric_configs
        )

        # Convert computed_metrics
        metrics: list[Metric] = []
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
                    ColumnMetric[float](
                        batch_id=batch_id,
                        metric_name=metric_name,
                        column=column,
                        value=value,
                        exception=exception,
                    )
                )

        return metrics

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

    def _get_all_column_names(self, metrics: Sequence[Metric]) -> List[str]:
        column_list: List[str] = []
        for metric in metrics:
            if metric.metric_name == "table.columns":
                column_list = metric.value
        return column_list

    def _get_numeric_column_names(
        self,
        batch_request: BatchRequest,
        exclude_column_names: List[str],
    ) -> list[str]:
        """Get the names of all numeric columns in the batch."""
        validator = self.get_validator(batch_request=batch_request)
        domain_builder = ColumnDomainBuilder(
            include_semantic_types=[SemanticDomainTypes.NUMERIC],
            exclude_column_names=exclude_column_names,
        )
        assert isinstance(
            validator.active_batch, Batch
        ), f"validator.active_batch is type {type(validator.active_batch).__name__} instead of type {Batch.__name__}"
        batch_id = validator.active_batch.id
        numeric_column_names = domain_builder.get_effective_column_names(
            validator=validator,
            batch_ids=[batch_id],
        )
        return numeric_column_names

    def _generate_table_metric_configurations(
        self, table_metric_names: list[str]
    ) -> list[MetricConfiguration]:
        table_metric_configs = [
            MetricConfiguration(
                metric_name=metric_name, metric_domain_kwargs={}, metric_value_kwargs={}
            )
            for metric_name in table_metric_names
        ]
        return table_metric_configs

    def _generate_column_metric_configurations(
        self, column_list: list[str], column_metric_names: list[str]
    ) -> list[MetricConfiguration]:
        column_metric_configs: List[MetricConfiguration] = list()
        for metric_name in column_metric_names:
            for column in column_list:
                column_metric_configs.append(
                    MetricConfiguration(
                        metric_name=metric_name,
                        metric_domain_kwargs={"column": column},
                        metric_value_kwargs={},
                    )
                )
        return column_metric_configs

    def _compute_metrics(
        self, batch_request: BatchRequest, metric_configs: list[MetricConfiguration]
    ) -> tuple[str, _MetricsDict, _AbortedMetricsInfoDict]:
        validator = self.get_validator(batch_request=batch_request)
        # The runtime configuration catch_exceptions is explicitly set to True to catch exceptions
        # that are thrown when computing metrics. This is so we can capture the error for later
        # surfacing, and not have the entire metric run fail so that other metrics will still be
        # computed.
        (
            computed_metrics,
            aborted_metrics,
        ) = validator.compute_metrics(
            metric_configurations=metric_configs,
            runtime_configuration={"catch_exceptions": True},
        )
        assert isinstance(
            validator.active_batch, Batch
        ), f"validator.active_batch is type {type(validator.active_batch).__name__} instead of type {Batch.__name__}"
        batch_id = validator.active_batch.id
        return batch_id, computed_metrics, aborted_metrics

    def _get_metric_from_computed_metrics(
        self,
        metric_name: str,
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
        metric_lookup_key: _MetricKey | None = None,
    ) -> tuple[Any, MetricException | None]:
        if metric_lookup_key is None:
            metric_lookup_key = (
                metric_name,
                tuple(),
                tuple(),
            )
        value = None
        metric_exception = None
        if metric_lookup_key in computed_metrics:
            value = computed_metrics[metric_lookup_key]
        elif metric_lookup_key in aborted_metrics:
            exception = aborted_metrics[metric_lookup_key]
            # Note: Only retrieving the first exception if there are multiple exceptions.
            exception_info = exception["exception_info"]
            assert isinstance(exception_info, set)  # Type narrowing for mypy
            first_exception = list(exception_info)[0]
            exception_type = "Unknown"  # Note: we currently only capture the message and traceback, not the type
            exception_message = first_exception.exception_message
            metric_exception = MetricException(
                type=exception_type, message=exception_message
            )
        else:
            metric_exception = MetricException(
                type="Not found",
                message="Metric was not successfully computed but exception was not found.",
            )

        return value, metric_exception
