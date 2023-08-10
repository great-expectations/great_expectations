from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.pyspark import types
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.util import convert_pandas_series_decimal_to_float_dtype


class ColumnMean(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.mean"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Mean Implementation"""
        convert_pandas_series_decimal_to_float_dtype(data=column, inplace=True)
        return column.mean()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        """SqlAlchemy Mean Implementation"""
        # column * 1.0 needed for correct calculation of avg in MSSQL
        return sa.func.avg(1.0 * column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table, _column_name, **kwargs):
        """Spark Mean Implementation"""
        column_data_type = _table.schema[_column_name].dataType
        if type(column_data_type) not in (
            types.DecimalType,
            types.IntegerType,
            types.DoubleType,
            types.FloatType,
            types.LongType,
        ):
            raise TypeError(
                f"Expected numeric column type for function mean(). Recieved type: {column_data_type}"
            )
        return F.mean(column)
