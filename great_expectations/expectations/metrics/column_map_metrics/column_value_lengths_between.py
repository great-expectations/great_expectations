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


class ColumnValuesValueLengthsBetween(ColumnMapMetric):
    condition_metric_name = "column_values.value_lengths_between"
    condition_value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        column,
        _metrics,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs
    ):
        column_lengths = _metrics.get("column_values.value_lengths")

        metric_series = None
        if min_value is not None and max_value is not None:
            if strict_min and strict_max:
                metric_series = column_lengths.between(
                    min_value, max_value, inclusive=False
                )
            elif strict_min and not strict_max:
                metric_series = (column_lengths > min_value) & (
                    column_lengths <= max_value
                )
            elif not strict_min and strict_max:
                metric_series = (column_lengths >= min_value) & (
                    column_lengths < max_value
                )
            elif not strict_min and not strict_max:
                metric_series = column_lengths.between(
                    min_value, max_value, inclusive=True
                )
        elif min_value is None and max_value is not None:
            if strict_max:
                metric_series = column_lengths < max_value
            else:
                metric_series = column_lengths <= max_value
        elif min_value is not None and max_value is None:
            if strict_min:
                metric_series = column_lengths > min_value
            else:
                metric_series = column_lengths >= min_value

        else:
            raise ValueError("Invalid configuration")

        return metric_series

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column,
        _metrics,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs
    ):
        column_lengths = _metrics.get("column_values.value_lengths")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        # Assert that min_value and max_value are integers
        try:
            if min_value is not None and not float(min_value).is_integer():
                raise ValueError("min_value and max_value must be integers")

            if max_value is not None and not float(max_value).is_integer():
                raise ValueError("min_value and max_value must be integers")

        except ValueError:
            raise ValueError("min_value and max_value must be integers")

        if min_value is not None and max_value is not None:
            return sa.and_(
                sa.func.length(column) >= min_value,
                sa.func.length(column) <= max_value,
            )

        elif min_value is None and max_value is not None:
            return sa.func.length(column) <= max_value

        elif min_value is not None and max_value is None:
            return sa.func.length(column) >= min_value

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
                "column_values.value_lengths", metric.metric_domain_kwargs
            )
        }
