from __future__ import annotations

import abc
import uuid
from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Optional,
    Sequence,
)

from great_expectations.core.domain import SemanticDomainTypes
from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.metrics import (
    ColumnMetric,
    MetricException,
    MetricTypes,
    TableMetric,
)
from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.experimental.metric_repository.metrics import Metric
    from great_expectations.validator.metrics_calculator import (
        _AbortedMetricsInfoDict,
        _MetricKey,
        _MetricsDict,
    )
    from great_expectations.validator.validator import (
        Validator,
    )


class MetricRetriever(abc.ABC):
    """A MetricRetriever is responsible for retrieving metrics for a batch of data. It is an ABC that contains base logic and
    methods share by both the ColumnDescriptiveMetricsMetricReceiver and MetricListMetricRetriver.
    """

    def __init__(self, context: AbstractDataContext):
        self._context = context
        self._validator: Validator | None = None

    def get_validator(self, batch_request: BatchRequest) -> Validator:
        if self._validator is None:
            self._validator = self._context.get_validator(batch_request=batch_request)
        return self._validator

    @abc.abstractmethod
    def get_metrics(
        self,
        batch_request: BatchRequest,
        metric_list: Optional[List[MetricTypes]] = None,
    ) -> Sequence[Metric]:
        raise NotImplementedError

    def _generate_metric_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def _get_metric_from_computed_metrics(
        self,
        metric_name: str | MetricTypes,
        computed_metrics: _MetricsDict,
        aborted_metrics: _AbortedMetricsInfoDict,
        metric_lookup_key: _MetricKey | None = None,
    ) -> tuple[Any, MetricException | None]:
        # look up is done by string
        # TODO: update to be MetricTypes once MetricListMetricRetriever implementation is complete.
        if isinstance(metric_name, MetricTypes):
            metric_name = metric_name.value

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
            exception_info = exception["exception_info"]
            exception_type = "Unknown"  # Note: we currently only capture the message and traceback, not the type
            if isinstance(exception_info, ExceptionInfo):
                exception_message = exception_info.exception_message
                metric_exception = MetricException(
                    type=exception_type, message=exception_message
                )
        else:
            metric_exception = MetricException(
                type="Not found",
                message="Metric was not successfully computed but exception was not found.",
            )

        return value, metric_exception

    def _generate_table_metric_configurations(
        self, table_metric_names: list[str | MetricTypes]
    ) -> list[MetricConfiguration]:
        table_metric_configs = [
            MetricConfiguration(
                metric_name=metric_name, metric_domain_kwargs={}, metric_value_kwargs={}
            )
            for metric_name in table_metric_names
        ]
        return table_metric_configs

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

    def _get_columns_to_exclude(self, table_column_types: Metric) -> List[str]:
        columns_to_skip: List[str] = []
        for column_type in table_column_types.value:
            if not column_type.get("type"):
                columns_to_skip.append(column_type["name"])
        return columns_to_skip

    def _get_numeric_column_names(
        self,
        batch_request: BatchRequest,
        exclude_column_names: List[str],
    ) -> list[str]:
        """Get the names of all numeric columns in the batch."""
        return self._get_column_names_for_semantic_types(
            batch_request=batch_request,
            include_semantic_types=[SemanticDomainTypes.NUMERIC],
            exclude_column_names=exclude_column_names,
        )

    def _get_timestamp_column_names(
        self,
        batch_request: BatchRequest,
        exclude_column_names: List[str],
    ) -> list[str]:
        """Get the names of all timestamp columns in the batch."""
        return self._get_column_names_for_semantic_types(
            batch_request=batch_request,
            include_semantic_types=[SemanticDomainTypes.DATETIME],
            exclude_column_names=exclude_column_names,
        )

    def _get_column_names_for_semantic_types(
        self,
        batch_request: BatchRequest,
        include_semantic_types: List[SemanticDomainTypes],
        exclude_column_names: List[str],
    ) -> list[str]:
        """Get the names of all columns matching semantic types in the batch."""
        validator = self.get_validator(batch_request=batch_request)
        domain_builder = ColumnDomainBuilder(
            include_semantic_types=include_semantic_types,  # type: ignore[arg-type]  # ColumnDomainBuilder supports other ways of specifying semantic types
            exclude_column_names=exclude_column_names,
        )
        assert isinstance(
            validator.active_batch, Batch
        ), f"validator.active_batch is type {type(validator.active_batch).__name__} instead of type {Batch.__name__}"
        batch_id = validator.active_batch.id
        column_names = domain_builder.get_effective_column_names(
            validator=validator,
            batch_ids=[batch_id],
        )
        return column_names

    def _get_table_metrics(
        self,
        batch_request: BatchRequest,
        metric_name: MetricTypes | str,
        metric_type: type[Metric],
    ) -> Metric:
        metric_configs = self._generate_table_metric_configurations([metric_name])
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, metric_configs
        )
        value, exception = self._get_metric_from_computed_metrics(
            metric_name=metric_name,
            computed_metrics=computed_metrics,
            aborted_metrics=aborted_metrics,
        )
        return metric_type(
            batch_id=batch_id, metric_name=metric_name, value=value, exception=exception
        )

    def _get_column_metrics(
        self,
        batch_request: BatchRequest,
        column_list: List[str],
        column_metric_names: List[MetricTypes | str],
        column_metric_type: type[ColumnMetric[Any]],
    ) -> Sequence[Metric]:
        column_metric_configs = self._generate_column_metric_configurations(
            column_list, column_metric_names
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, column_metric_configs
        )

        # Convert computed_metrics
        ColumnMetric.update_forward_refs()
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
                    column_metric_type(
                        batch_id=batch_id,
                        metric_name=metric_name,
                        column=column,
                        value=value,
                        exception=exception,
                    )
                )
        return metrics

    def _generate_column_metric_configurations(
        self, column_list: list[str], column_metric_names: list[str | MetricTypes]
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

    def _get_all_column_names(self, metrics: Sequence[Metric]) -> List[str]:
        column_list: List[str] = []
        for metric in metrics:
            if metric.metric_name == MetricTypes.TABLE_COLUMNS:
                column_list = metric.value
        return column_list

    def _get_table_row_count(self, batch_request: BatchRequest) -> Metric:
        return self._get_table_metrics(
            batch_request=batch_request,
            metric_name=MetricTypes.TABLE_ROW_COUNT,
            metric_type=TableMetric[int],
        )

    def _get_table_columns(self, batch_request: BatchRequest) -> Metric:
        return self._get_table_metrics(
            batch_request=batch_request,
            metric_name=MetricTypes.TABLE_COLUMNS,
            metric_type=TableMetric[List[str]],
        )

    def _get_table_column_types(self, batch_request: BatchRequest) -> Metric:
        metric_name = MetricTypes.TABLE_COLUMN_TYPES
        metric_lookup_key: _MetricKey = (metric_name, tuple(), "include_nested=True")
        table_metric_configs = self._generate_table_metric_configurations(
            table_metric_names=[metric_name]
        )
        batch_id, computed_metrics, aborted_metrics = self._compute_metrics(
            batch_request, table_metric_configs
        )
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
