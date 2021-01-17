from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.expectations.metrics.import_manager import F, sa


class ColumnInterquartileRange(ColumnMetricProvider):
    """MetricProvider Class for Aggregate Interquartile Range MetricProvider"""

    metric_name = "column.interquartile_range"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Interquartile Range Implementation"""
        return column.quantile(0.75) - column.quantile(0.25)

    # @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, **kwargs):
    #     """SqlAlchemy Mean Implementation"""
    #     # column * 1.0 needed for correct calculation of avg in MSSQL
    #     return sa.func.avg(column * 1.0)
    #
    # @column_aggregate_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, _table, _column_name, **kwargs):
    #     """Spark Mean Implementation"""
    #     types = dict(_table.dtypes)
    #     if types[_column_name] not in ("int", "float", "double", "bigint"):
    #         raise TypeError("Expected numeric column type for function mean()")
    #     return F.mean(column)
