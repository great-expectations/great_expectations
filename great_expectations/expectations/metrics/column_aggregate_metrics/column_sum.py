from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnAggregateMetricProvider,
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa


class ColumnSum(ColumnAggregateMetricProvider):
    metric_name = "column.aggregate.sum"

    @column_aggregate_metric(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.sum()

    @column_aggregate_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return sa.func.sum(column)
