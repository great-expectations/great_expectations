import pandas as pd

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
from great_expectations.expectations.metrics.import_manager import F, sa


class ColumnValuesLengthMin(ColumnAggregateMetricProvider):
    metric_name = "column_values.length.min"

    @column_aggregate_value(engine=PandasExecutionEngine, filter_column_isnull=True)
    def _pandas(cls, column: pd.Series, **kwargs: dict) -> int:
        return column.map(len).min()

    @column_aggregate_partial(
        engine=SqlAlchemyExecutionEngine, filter_column_isnull=True
    )
    def _sqlalchemy(cls, column, **kwargs: dict):  # type: ignore[no-untyped-def]
        return sa.func.min(sa.func.length(column))

    @column_aggregate_partial(engine=SparkDFExecutionEngine, filter_column_isnull=True)
    def _spark(cls, column, **kwargs: dict):  # type: ignore[no-untyped-def]
        return F.min(F.length(column))
