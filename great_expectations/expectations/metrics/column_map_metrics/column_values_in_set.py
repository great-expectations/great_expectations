from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetric,
    column_map_condition,
)


class ColumnValuesInSet(ColumnMapMetric):
    condition_metric_name = "column_values.in_set"
    condition_value_keys = ("value_set",)

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, value_set, **kwargs):
        return column.isin(value_set)

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value_set, **kwargs):
        return column.in_(value_set)

    @column_map_condition(engine=SparkDFExecutionEngine)
    def _spark(cls, column, value_set, **kwargs):
        return column.isin(value_set)
