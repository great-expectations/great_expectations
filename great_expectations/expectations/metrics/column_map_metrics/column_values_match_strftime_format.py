from datetime import datetime

import pandas as pd

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    MapMetricProvider,
    column_map_condition,
)


class ColumnValuesMatchStrftimeFormat(MapMetricProvider):
    condition_metric_name = "column_values.match_strftime_format"
    condition_value_keys = "strftime_format"

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, strftime_format, **kwargs):
        def is_parseable_by_format(val):
            try:
                datetime.strptime(val, strftime_format)
                return True
            except TypeError:
                raise TypeError(
                    "Values passed to expect_column_values_to_match_strftime_format must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format."
                )
            except ValueError:
                return False

        return column.map(is_parseable_by_format)
