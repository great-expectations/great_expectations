from __future__ import annotations

import abc
import uuid
from typing import TYPE_CHECKING, Any, List, Optional, Sequence

from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.metrics import MetricException
from great_expectations.validator.exception_info import ExceptionInfo
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.experimental.metric_repository.metrics import (
        Metric,
        MetricTypes,
    )
    from great_expectations.validator.metrics_calculator import (
        _AbortedMetricsInfoDict,
        _MetricKey,
        _MetricsDict,
    )
    from great_expectations.validator.validator import Validator


class MetricRetriever(abc.ABC):
    """A MetricRetriever is responsible for retrieving metrics for a batch of data."""

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
        metrics_list: Optional[List[MetricTypes]] = None,
    ) -> Sequence[Metric]:
        raise NotImplementedError

    def _generate_metric_id(self) -> uuid.UUID:
        return uuid.uuid4()

    def _generate_table_metric_configurations(
        self, table_metric_names: List[str]
    ) -> List[MetricConfiguration]:
        table_metric_configs = [
            MetricConfiguration(
                metric_name=metric_name, metric_domain_kwargs={}, metric_value_kwargs={}
            )
            for metric_name in table_metric_names
        ]
        return table_metric_configs

    def _generate_column_metric_configurations(
        self, column_list: List[str], column_metric_names: List[str]
    ) -> List[MetricConfiguration]:
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
