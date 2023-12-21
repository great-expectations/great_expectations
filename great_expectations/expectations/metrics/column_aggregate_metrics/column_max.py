from __future__ import annotations

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


class ColumnMax(ColumnAggregateMetricProvider):
    metric_name = "column.max"
    value_keys = ()

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.max()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return sa.func.max(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return F.max(column)
