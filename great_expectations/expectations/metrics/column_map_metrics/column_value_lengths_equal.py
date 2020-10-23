from typing import Optional

import sqlalchemy as sa

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetric,
    column_map_condition,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnValuesValueLengthsEqual(ColumnMapMetric):
    condition_metric_name = "column_values.value_lengths_equal"
    condition_value_keys = ("value",)

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, value, _metrics, **kwargs):
        column_lengths = _metrics.get("column_values.value_lengths")

        return column_lengths == value

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value, _metrics, **kwargs):
        column_lengths = _metrics.get("column_values.value_lengths")

        return sa.func.length(sa.column(column)) == value

    @classmethod
    def get_evaluation_dependencies(
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
        return {
            "column_values.value_lengths": MetricConfiguration(
                "column_values.values_lengths", metric.metric_domain_kwargs
            )
        }
