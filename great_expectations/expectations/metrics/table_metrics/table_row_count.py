from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.table_metric import (
    AggregateMetricProvider,
    aggregate_metric,
)


class TableRowCount(AggregateMetricProvider):
    metric_name = "table.row_count"

    @aggregate_metric(engine=PandasExecutionEngine)
    def _pandas(cls, table, **kwargs):
        return table.shape[0]

    @aggregate_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, table, **kwargs):
        return sa.func.count()

    @aggregate_metric(engine=SparkDFExecutionEngine)
    def _spark(cls, table, **kwargs):
        return table.count()
