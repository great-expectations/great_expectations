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
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetricProvider,
    column_map_condition,
    column_map_function,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnValuesValueLengthEquals(ColumnMapMetricProvider):
    condition_metric_name = "column_values.value_length.equals"
    condition_value_keys = ("value",)

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, value, _metrics, **kwargs):
        column_lengths = _metrics.get("column_values.value_length.map_fn")
        return column_lengths == value

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value, _metrics, **kwargs):
        column_lengths, _ = _metrics.get("column_values.value_length.map_fn")
        return column_lengths == value

    @column_map_condition(engine=SparkDFExecutionEngine)
    def _spark(cls, column, value, _metrics, **kwargs):
        column_lengths, _ = _metrics.get("column_values.value_length.map_fn")
        return column_lengths == value

    @classmethod
    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "column_values.value_length.map_fn": MetricConfiguration(
                "column_values.value_length.map_fn", metric.metric_domain_kwargs
            )
        }


class ColumnValuesValueLength(ColumnMapMetricProvider):
    condition_metric_name = "column_values.value_length.between"
    function_metric_name = "column_values.value_length.map_fn"

    condition_value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @column_map_function(engine=PandasExecutionEngine)
    def _pandas_function(cls, column, **kwargs):
        return column.astype(str).str.len()

    @column_map_function(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_function(cls, column, **kwargs):
        return sa.func.length(column)

    @column_map_function(engine=SparkDFExecutionEngine)
    def _spark_function(cls, column, **kwargs):
        return F.length(column)

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
        column_lengths = _metrics.get("column_values.value_length.map_fn")

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
        column_lengths, _ = _metrics.get("column_values.value_length.map_fn")

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
            return sa.and_(column_lengths >= min_value, column_lengths <= max_value,)

        elif min_value is None and max_value is not None:
            return column_lengths <= max_value

        elif min_value is not None and max_value is None:
            return column_lengths >= min_value

    @column_map_condition(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column,
        _metrics,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs
    ):
        column_lengths, _ = _metrics.get("column_values.value_length.map_fn")

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
            return F.col((column_lengths >= min_value) & (column_lengths <= max_value))

        elif min_value is None and max_value is not None:
            return column_lengths <= max_value

        elif min_value is not None and max_value is None:
            return column_lengths >= min_value

    @classmethod
    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        return {
            "column_values.value_length.map_fn": MetricConfiguration(
                "column_values.value_length.map_fn", metric.metric_domain_kwargs
            )
        }
