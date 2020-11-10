from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider, column_aggregate_value,
)
from great_expectations.expectations.metrics.column_aggregate_metric import F as F
from great_expectations.expectations.metrics.column_aggregate_metric import (
    column_aggregate_partial,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa


class ColumnMean(ColumnMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.mean"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Mean Implementation"""
        return column.mean()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        """SqlAlchemy Mean Implementation"""
        return sa.func.avg(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        """Spark Mean Implementation"""
        return F.mean(column)
