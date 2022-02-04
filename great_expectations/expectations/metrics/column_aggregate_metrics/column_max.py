import warnings

from dateutil.parser import parse

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
from great_expectations.expectations.metrics.import_manager import F, sa


class ColumnMax(ColumnAggregateMetricProvider):
    metric_name = "column.max"
    value_keys = ("parse_strings_as_datetimes",)

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.
""",
                DeprecationWarning,
            )

            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column
            return temp_column.max()
        else:
            return column.max()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                f"""The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.  Moreover, in "{cls.__name__}._sqlalchemy()", it is not used.
""",
                DeprecationWarning,
            )

        return sa.func.max(column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is no longer supported and will be deprecated in a \
future release.  Please update code accordingly.
""",
                DeprecationWarning,
            )

            try:
                column = apply_dateutil_parse(column=column)
            except TypeError:
                pass

        return F.max(column)
