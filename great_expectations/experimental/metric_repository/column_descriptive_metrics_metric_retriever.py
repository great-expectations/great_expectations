from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from typing_extensions import override

from great_expectations.datasource.fluent.interfaces import Batch
from great_expectations.experimental.metric_repository.metric_retriever import (
    MetricRetriever,
)
from great_expectations.experimental.metric_repository.metrics import (
    Metric,
    MetricException,
    NumericTableMetric,
    StringListTableMetric,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.datasource.fluent import BatchRequest
    from great_expectations.validator.metrics_calculator import _MetricKey


class ColumnDescriptiveMetricsMetricRetriever(MetricRetriever):
    # TODO: Docstrings
    @override
    def get_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metrics_list = self._get_table_metrics(batch_request)

        return table_metrics_list

    def _get_table_metrics(self, batch_request: BatchRequest) -> Sequence[Metric]:
        table_metric_names = ["table.row_count", "table.columns"]
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
        NumericTableMetric.update_forward_refs()
        StringListTableMetric.update_forward_refs()

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
            NumericTableMetric(
                id=self._generate_metric_id(),
                batch=validator.active_batch,
                metric_name=metric_name,
                value=computed_metrics[metric_lookup_key],  # type: ignore[arg-type] # Pydantic verifies the value type
                exception=MetricException(),  # TODO: Pass through
            )
        )

        metric_name = "table.columns"
        metric_lookup_key = (metric_name, tuple(), tuple())
        metrics.append(
            StringListTableMetric(
                id=self._generate_metric_id(),
                batch=validator.active_batch,
                metric_name=metric_name,
                value=computed_metrics[metric_lookup_key],  # type: ignore[arg-type] # Pydantic verifies the value type
                exception=MetricException(),  # TODO: Pass through
            )
        )
        return metrics
