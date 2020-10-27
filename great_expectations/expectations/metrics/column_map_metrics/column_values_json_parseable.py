import json

from dateutil.parser import parse

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetricProvider,
    column_map_condition,
)


class ColumnValuesJsonParseable(ColumnMapMetricProvider):
    condition_metric_name = "column_values.json_parseable"

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def is_json(val):
            try:
                json.loads(val)
                return True
            except:
                return False

        return column.map(is_json)
