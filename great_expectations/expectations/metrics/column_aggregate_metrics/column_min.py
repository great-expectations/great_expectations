from dateutil.parser import parse

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sparkdf_execution_engine import (
    apply_dateutil_parse,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.warnings import warn_deprecated_parse_strings_as_datetimes


class ColumnMin(ColumnAggregateMetricProvider):
    metric_name = "column.min"
    value_keys = ("parse_strings_as_datetimes",)

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column
            return temp_column.min()
        else:
            return column.min()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

        return sa.func.min(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

            try:
                column = apply_dateutil_parse(column=column)
            except TypeError:
                pass

        return F.min(column)
