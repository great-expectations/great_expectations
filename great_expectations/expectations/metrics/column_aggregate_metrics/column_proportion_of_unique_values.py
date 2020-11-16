from typing import Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.metric_provider import metric_value
from great_expectations.validator.validation_graph import MetricConfiguration


def unique_proportion(_metrics):
    total_values = _metrics.get("table.row_count")
    unique_values = _metrics.get("column.distinct_values.count")
    null_count = _metrics.get("column_values.nonnull.unexpected_count")

    if total_values > 0:
        return unique_values / (total_values - null_count)
    else:
        return 0


class ColumnUniqueProportion(ColumnMetricProvider):
    metric_name = "column.unique_proportion"

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(*args, metrics, **kwargs):
        return unique_proportion(metrics)

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(*args, metrics, **kwargs):
        return unique_proportion(metrics)

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(*args, metrics, **kwargs):
        return unique_proportion(metrics)

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        table_domain_kwargs = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        return {
            "column.distinct_values.count": MetricConfiguration(
                "column.distinct_values.count", metric.metric_domain_kwargs
            ),
            "table.row_count": MetricConfiguration(
                "table.row_count", table_domain_kwargs
            ),
            "column_values.nonnull.unexpected_count": MetricConfiguration(
                "column_values.nonnull.unexpected_count", metric.metric_domain_kwargs
            ),
        }
