import warnings

import numpy as np

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


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
            # deprecated-v0.13.41
            warnings.warn(
                f"""The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 and will be removed in \
            v0.16.  Please update code accordingly. Moreover, in "{cls.__name__}._pandas()", it is not used.
            """,
                DeprecationWarning,
            )

        if value_set is None:
            # Vacuously true
            return np.ones(len(column), dtype=np.bool_)

        return column.isin(value_set)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
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
            # deprecated-v0.13.41
            warnings.warn(
                f"""The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 and will be removed in \
            v0.16.  Please update code accordingly. Moreover, in "{cls.__name__}._sqlalchemy()", it is not used.
            """,
                DeprecationWarning,
            )

        if value_set is None:
            # vacuously true
            return True

        if len(value_set) == 0:
            return False

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
            # deprecated-v0.13.41
            warnings.warn(
                f"""The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 and will be removed in \
            v0.16.  Please update code accordingly. Moreover, in "{cls.__name__}._spark()", it is not used.
            """,
                DeprecationWarning,
            )

        if value_set is None:
            # vacuously true
            return F.lit(True)

        return column.isin(value_set)
