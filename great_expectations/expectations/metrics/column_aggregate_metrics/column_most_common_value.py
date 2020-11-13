from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_partial, column_aggregate_value,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa


class ColumnMostCommonValue(ColumnMetricProvider):
    metric_name = "column.most_common_value"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        mode_list = list(column.mode().values)
        return mode_list
