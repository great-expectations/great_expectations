from __future__ import annotations

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.not_imported import is_version_greater_or_equal
from great_expectations.compatibility.pyspark import functions as F
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


class ColumnSum(ColumnAggregateMetricProvider):
    metric_name = "column.sum"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        column = convert_pandas_series_decimal_to_float_dtype(data=column)
        return column.sum()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return sa.func.sum(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table=None, _column_name=None, **kwargs):
        # Summing an integral column accumulates in a LongType. Spark 4 evaluates
        # arithmetic under ANSI semantics by default and raises ARITHMETIC_OVERFLOW when
        # that Long accumulator overflows, whereas Spark 3 silently wraps around. To keep
        # the aggregate from raising on large-magnitude/high-cardinality integral data
        # under Spark 4, widen integral inputs to DoubleType before summing there. On
        # Spark 3 we leave the input untouched so the observed value stays an integer,
        # byte-identical to prior behavior.
        integral_types = (
            pyspark.types.ByteType,
            pyspark.types.ShortType,
            pyspark.types.IntegerType,
            pyspark.types.LongType,
        )
        if (
            pyspark.pyspark
            and is_version_greater_or_equal(pyspark.pyspark.__version__, "4.0.0")
            and _table is not None
            and _column_name is not None
            and isinstance(_table.schema[_column_name].dataType, integral_types)
        ):
            column = column.cast(pyspark.types.DoubleType())
        return F.sum(column)
