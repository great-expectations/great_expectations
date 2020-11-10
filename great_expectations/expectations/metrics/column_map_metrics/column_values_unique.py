import uuid
from typing import Any, Dict, Optional, Tuple

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetricProvider,
    column_map_condition,
    column_map_function,
)
from great_expectations.expectations.metrics.column_map_metric import sa as sa
from great_expectations.expectations.metrics.import_manager import F, Window
from great_expectations.expectations.metrics.metric_provider import metric
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnValuesUnique(ColumnMapMetricProvider):
    condition_metric_name = "column_values.unique"

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return ~column.duplicated(keep=False)

    @column_map_condition(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _table, **kwargs):
        dup_query = (
            sa.select([column])
            .select_from(_table)
            .group_by(column)
            .having(sa.func.count(column) > 1)
        )

        return column.notin_(dup_query)

    @column_map_condition(
        engine=SparkDFExecutionEngine, metric_fn_type="window_condition_fn"
    )
    def _spark(cls, column, **kwargs):
        return F.count(F.lit(1)).over(Window.partitionBy(column)) <= 1
