from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnAggregateMetric,
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa


class ColumnMin(ColumnAggregateMetric):
    metric_name = "column.aggregate.min"

    @column_aggregate_metric(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.min()

    @column_aggregate_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return sa.func.min(column)

    @column_aggregate_metric(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        result = column.agg({column: "min"}).collect()
        if not result or not result[0]:
            return None
        return result[0][0]
