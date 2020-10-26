from typing import Optional

from sqlalchemy.engine import reflection

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.metric import Metric
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric import (
    ColumnAggregateMetricProvider,
    column_aggregate_metric,
)
from great_expectations.expectations.metrics.column_aggregate_metric import sa as sa
from great_expectations.expectations.metrics.table_metric import (
    TableMetricProvider,
    table_metric,
)
from great_expectations.expectations.metrics.util import column_reflection_fallback
from great_expectations.validator.validation_graph import MetricConfiguration


class TableColumns(TableMetricProvider):
    metric_name = "table.columns"

    @table_metric(engine=PandasExecutionEngine)
    def _pandas(cls, table, **kwargs):
        cols = table.columns
        return cols.tolist()

    @table_metric(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, table, _dialect, _sqlalchemy_engine, **kwargs):
        try:
            insp = reflection.Inspector.from_engine(_sqlalchemy_engine)
            columns = insp.get_columns(table)
        except KeyError:
            # we will get a KeyError for temporary tables, since
            # reflection will not find the temporary schema
            columns = column_reflection_fallback()

        # Use fallback because for mssql reflection doesn't throw an error but returns an empty list
        if len(columns) == 0:
            columns = column_reflection_fallback(table, _dialect, _sqlalchemy_engine)

        return columns
