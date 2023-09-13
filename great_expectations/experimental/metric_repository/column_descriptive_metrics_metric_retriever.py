from __future__ import annotations

from itertools import chain
from typing import TYPE_CHECKING, Any, List, Sequence, Type

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
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.validator.metrics_calculator import _MetricKey


class ColumnDescriptiveMetricsMetricRetriever(MetricRetriever):
    """Compute and retrieve Column Descriptive Metrics for a batch of data."""

    @override
    def get_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metrics = self._get_table_metrics(batch_request)

        numeric_column_names = self._get_numeric_column_names(
            batch_request=batch_request
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
        computed_metrics, batch_id = self._compute_table_metrics(
            table_metric_names, batch_request
        )

        # Convert computed_metrics
        metrics: list[Metric] = []

        metrics.append(self._get_table_row_count(batch_id, computed_metrics))
        metrics.append(self._get_table_columns(batch_id, computed_metrics))
        metrics.append(self._get_table_column_types(batch_id, computed_metrics))
        return metrics

    def _get_table_row_count(self, batch_id, computed_metrics) -> Metric:
        metric_name = "table.row_count"
        value, exception = self._get_metric_from_computed_metrics(
            metric_name=metric_name,
            computed_metrics=computed_metrics,
        )
        return TableMetric[int](
            batch_id=batch_id,
            metric_name=metric_name,
            value=value,
            exception=exception,
        )

    def _get_table_columns(self, batch_id, computed_metrics) -> Metric:
        metric_name = "table.columns"
        value, exception = self._get_metric_from_computed_metrics(
            metric_name=metric_name,
            computed_metrics=computed_metrics,
        )
        return TableMetric[List[str]](
            batch_id=batch_id,
            metric_name=metric_name,
            value=value,
            exception=exception,
        )

    def _get_table_column_types(self, batch_id, computed_metrics) -> Metric:
        metric_name = "table.column_types"
        metric_lookup_key: _MetricKey = (metric_name, tuple(), "include_nested=True")
        value, exception = self._get_metric_from_computed_metrics(
            metric_name=metric_name,
            metric_lookup_key=metric_lookup_key,
            computed_metrics=computed_metrics,
        )
        raw_column_types: list[
            dict[str, Any]
        ] = value  # type: ignore[assignment] # Metric results from computed_metrics are not typed
        column_types_converted_to_str: list[dict[str, str]] = [
            {"name": raw_column_type["name"], "type": str(raw_column_type["type"])}
            for raw_column_type in raw_column_types
        ]
        return TableMetric[List[str]](
            batch_id=batch_id,
            metric_name=metric_name,
            value=column_types_converted_to_str,
            exception=exception,
        )

    def _compute_table_metrics(
        self, table_metric_names: list[str], batch_request: BatchRequest
    ) -> tuple[dict[_MetricKey, Any], str | tuple]:
        table_metric_configs = self._generate_table_metric_configurations(
            table_metric_names
        )
        validator = self._context.get_validator(batch_request=batch_request)
        # The runtime configuration catch_exceptions is explicitly set to True to catch exceptions
        # that are thrown when computing metrics. This is so we can capture the error for later
        # surfacing, and not have the entire metric run fail so that other metrics will still be
        # computed.
        # TODO: Get aborted_metrics in addition to computed metrics so they can be plumbed into the MetricException.
        computed_metrics = validator.compute_metrics(
            metric_configurations=table_metric_configs,
            runtime_configuration={"catch_exceptions": True},
        )

        assert isinstance(validator.active_batch, Batch)
        if not isinstance(validator.active_batch, Batch):
            raise TypeError(
                f"validator.active_batch is type {type(validator.active_batch).__name__} instead of type {Batch.__name__}"
            )
        batch_id = validator.active_batch.id
        return computed_metrics, batch_id

    def _get_numeric_column_metrics(
        self, batch_request: BatchRequest, column_list: List[str]
    ) -> Sequence[Metric]:
        column_metric_names = [
            "column.min",
            "column.max",
            "column.mean",
            "column.median",
        ]

        # TODO: nested for-loop can be better
        column_metric_configs = self._generate_column_metric_configurations(
            column_list, column_metric_names
        )
        validator = self._context.get_validator(batch_request=batch_request)
        # The runtime configuration catch_exceptions is explicitly set to True to catch exceptions
        # that are thrown when computing metrics. This is so we can capture the error for later
        # surfacing, and not have the entire metric run fail so that other metrics will still be
        # computed.
        # TODO: Get aborted_metrics in addition to computed metrics so they can be plumbed into the MetricException.
        computed_metrics = validator.compute_metrics(
            metric_configurations=column_metric_configs,
            runtime_configuration={"catch_exceptions": True},
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

        for metric_name in column_metric_names:
            for column in column_list:
                metric_lookup_key = (metric_name, f"column={column}", tuple())
                value, exception = self._get_metric_from_computed_metrics(
                    metric_name=metric_name,
                    metric_lookup_key=metric_lookup_key,
                    computed_metrics=computed_metrics,
                )
                metrics.append(
                    ColumnMetric[float](
                        batch_id=validator.active_batch.id,
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
        validator = self._context.get_validator(batch_request=batch_request)
        # The runtime configuration catch_exceptions is explicitly set to True to catch exceptions
        # that are thrown when computing metrics. This is so we can capture the error for later
        # surfacing, and not have the entire metric run fail so that other metrics will still be
        # computed.
        # TODO: Get aborted_metrics in addition to computed metrics so they can be plumbed into the MetricException.
        computed_metrics = validator.compute_metrics(
            metric_configurations=column_metric_configs,
            runtime_configuration={"catch_exceptions": True},
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
                value, exception = self._get_metric_from_computed_metrics(
                    metric_name=metric_name,
                    metric_lookup_key=metric_lookup_key,
                    computed_metrics=computed_metrics,
                )
                metrics.append(
                    ColumnMetric[value_type](  # type: ignore[valid-type]  # Will be refactored in upcoming PR
                        batch_id=validator.active_batch.id,
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

    def _get_numeric_column_names(self, batch_request: BatchRequest) -> list[str]:
        """Get the names of all numeric columns in the batch."""
        validator = self._context.get_validator(batch_request=batch_request)
        domain_builder = ColumnDomainBuilder(
            include_semantic_types=[SemanticDomainTypes.NUMERIC],
        )
        numeric_column_names = domain_builder.get_effective_column_names(
            validator=validator,
            batch_ids=[validator.active_batch.id],
        )
        return numeric_column_names

    def _generate_table_metric_configurations(self, table_metric_names):
        table_metric_configs = [
            MetricConfiguration(
                metric_name=metric_name, metric_domain_kwargs={}, metric_value_kwargs={}
            )
            for metric_name in table_metric_names
        ]
        return table_metric_configs

    def _generate_column_metric_configurations(
        self, column_list, column_metric_names
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

    def _get_metric_from_computed_metrics(
        self,
        metric_name: str,
        computed_metrics: dict[_MetricKey, Any],
        metric_lookup_key: _MetricKey | None = None,
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
