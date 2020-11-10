from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.import_manager import sa


class ColumnUniqueValueCount(ColumnMetricProvider):
    metric_name = "column.aggregate.unique_value_count"

    @column_aggregate_metric(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.value_counts().shape[0]

    @column_aggregate_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return sa.func.count(sa.func.distinct(column)).scalar()
