import pandas as pd

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    MapMetricProvider,
    column_map_condition,
)


class ColumnValuesDecreasing(MapMetricProvider):
    condition_metric_name = "column_values.decreasing"
    condition_value_keys = ("strictly",)

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, strictly=None, **kwargs):
        series_diff = column.diff()
        # The first element is null, so it gets a bye and is always treated as True
        series_diff[series_diff.isnull()] = -1

        if strictly:
            return series_diff < 0
        else:
            return series_diff <= 0
