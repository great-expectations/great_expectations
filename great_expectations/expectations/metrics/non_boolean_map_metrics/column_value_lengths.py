import sqlalchemy as sa

from great_expectations.core.metric import Metric
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetricProvider,
    column_map_condition,
    column_map_function,
)


class ColumnValuesValueLengths(ColumnMapMetricProvider):
    function_metric_name = "column_values.value_lengths"

    @column_map_function(engine=PandasExecutionEngine)
    def _pandas_function(cls, column, **kwargs):
        return column.astype(str).str.len()

    @column_map_function(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_function(cls, column, **kwargs):
        return sa.func.length(column)
