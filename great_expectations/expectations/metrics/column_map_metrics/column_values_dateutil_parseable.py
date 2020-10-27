from dateutil.parser import parse

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    MapMetricProvider,
    column_map_condition,
)


class ColumnValuesDateutilParseable(MapMetricProvider):
    condition_metric_name = "column_values.dateutil_parseable"

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def is_parseable(val):
            try:
                if type(val) != str:
                    raise TypeError(
                        "Values passed to expect_column_values_to_be_dateutil_parseable must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format."
                    )

                parse(val)
                return True

            except (ValueError, OverflowError):
                return False

        return column.map(is_parseable)
