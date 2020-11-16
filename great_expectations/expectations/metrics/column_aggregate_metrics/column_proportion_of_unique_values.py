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
from great_expectations.validator.validation_graph import MetricConfiguration


def unique_proportion(_metrics):
    total_values = _metrics.get("table.row_count")
    unique_values = _metrics.get("column.unique_value_count")
    null_count = _metrics.get("column_values.nonnull.unexpected_count")

    if total_values > 0:
        return unique_values / (total_values - null_count)
    else:
        return 0


class ColumnUniqueProportion(ColumnMetricProvider):
    metric_name = "column.unique_proportion"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, _metrics, **kwargs):
        return unique_proportion(_metrics)

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _metrics, **kwargs):
        return unique_proportion(_metrics)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _metrics, **kwargs):
        return unique_proportion(_metrics)

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """This should return a dictionary:

        {
          "dependency_name": MetricConfiguration,
          ...
        }
        """
        table_domain_kwargs = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        return {
            "column.unique_value_count": MetricConfiguration(
                "column.unique_value_count", metric.metric_domain_kwargs
            ),
            "table.row_count": MetricConfiguration(
                "table.row_count", table_domain_kwargs
            ),
            "column_values.nonnull.unexpected_count": MetricConfiguration(
                "column_values.nonnull.unexpected_count", metric.metric_domain_kwargs
            )
        }
