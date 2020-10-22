import numpy as np
import pandas as pd

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetric,
    column_map_condition,
)
from great_expectations.expectations.metrics.util import parse_value_set


class ColumnValuesNotInSet(ColumnMapMetric):
    condition_metric_name = "column_values.not_in_set"
    condition_value_keys = ("value_set", "parse_strings_as_datetimes")

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, value_set, **kwargs):
        if value_set is None:
            # Vacuously true
            return np.ones(len(column), dtype=np.bool_)
        if pd.api.types.is_datetime64_any_dtype(column):
            parsed_value_set = parse_value_set(value_set=value_set)
        else:
            parsed_value_set = value_set

        return pd.DataFrame(
            {"column_values.not_in_set": ~column.isin(parsed_value_set)}
        )

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value_set, parse_strings_as_datetimes, **kwargs):
        if parse_strings_as_datetimes:
            parsed_value_set = parse_value_set(value_set)
        else:
            parsed_value_set = value_set
        return column.notin_(tuple(parsed_value_set))
