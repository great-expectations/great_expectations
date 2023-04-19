from collections.abc import Sequence

import numpy as np

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.warnings import warn_deprecated_parse_strings_as_datetimes

try:
    import sqlalchemy as sa  # noqa: TID251
except ImportError:
    sa = None


class ColumnValuesInSet(ColumnMapMetricProvider):
    condition_metric_name = "column_values.in_set"
    condition_value_keys = ("value_set", "parse_strings_as_datetimes")

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        column,
        value_set,
        **kwargs,
    ):
        # no need to parse as datetime; just compare the strings as is
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

        if value_set is None:
            # Vacuously true
            return np.ones(len(column), dtype=np.bool_)

        return column.isin(value_set)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, value_set, **kwargs):
        return cls._sqlalchemy_impl(column, value_set, **kwargs)

    @staticmethod
    def _sqlalchemy_impl(column, value_set, **kwargs):
        # no need to parse as datetime; just compare the strings as is
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

        if value_set is None:
            # vacuously true
            return True

        if len(value_set) == 0:
            return False

        # This "if" block is a workaround for:
        # https://github.com/googleapis/python-bigquery-sqlalchemy/issues/489#issuecomment-1253731826
        # `in_` doesn't work for boolean columns in bigquery so we unroll expressions like
        # `column in (TRUE, FALSE)` into `column == TRUE or column == FALSE`
        if (
            sa
            and "_dialect" in kwargs
            and hasattr(kwargs["_dialect"], "__name__")
            and kwargs["_dialect"].__name__ == "sqlalchemy_bigquery"
            and "_metrics" in kwargs
            and "table.column_types" in kwargs["_metrics"]
            and isinstance(kwargs["_metrics"]["table.column_types"], Sequence)
        ):
            for column_info in kwargs["_metrics"]["table.column_types"]:
                if (
                    "name" in column_info
                    and column_info["name"] == column.name
                    and "type" in column_info
                    and type(column_info["type"]) == sa.Boolean
                ):
                    return sa.or_(*[column == value for value in value_set])
        return column.in_(value_set)

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column,
        value_set,
        **kwargs,
    ):
        # no need to parse as datetime; just compare the strings as is
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

        if value_set is None:
            # vacuously true
            return F.lit(True)

        return column.isin(value_set)
