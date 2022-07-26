import datetime
import warnings

from dateutil.parser import parse

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ATOL, RTOL
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.util import isclose


class ColumnValuesBetween(ColumnMapMetricProvider):
    condition_metric_name = "column_values.between"
    condition_value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "parse_strings_as_datetimes",
        "allow_cross_type_comparisons",
    )

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        parse_strings_as_datetimes: bool = False,
        allow_cross_type_comparisons=None,
        **kwargs
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        if parse_strings_as_datetimes:
            # deprecated-v0.13.41
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in \
v0.16. As part of the V3 API transition, we've moved away from input transformation. For more information, \
please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/
""",
                DeprecationWarning,
            )

            if min_value is not None:
                try:
                    min_value = parse(min_value)
                except TypeError:
                    pass

            if max_value is not None:
                try:
                    max_value = parse(max_value)
                except TypeError:
                    pass

            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column

        else:
            temp_column = column

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        def is_between(val):
            # TODO: <Alex>Make "rtol" and "atol" customizable at "ExpectationConfiguration" level.</Alex>
            # TODO Might be worth explicitly defining comparisons between types (for example, between strings and ints).
            # Ensure types can be compared since some types in Python 3 cannot be logically compared.
            # print type(val), type(min_value), type(max_value), val, min_value, max_value

            if type(val) is None:
                return False

            if min_value is not None and max_value is not None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_min and strict_max:
                            return (min_value < val) and (val < max_value)
                        elif strict_min:
                            return (min_value < val) and (val <= max_value)
                        elif strict_max:
                            return (min_value <= val) and (val < max_value)
                        else:
                            return (min_value <= val) and (val <= max_value)
                    except TypeError:
                        return False

                else:
                    if (isinstance(val, str) != isinstance(min_value, str)) or (
                        isinstance(val, str) != isinstance(max_value, str)
                    ):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_min and strict_max:
                        return (min_value < val) and (val < max_value)
                    elif strict_min:
                        return (min_value < val) and (
                            isclose(
                                operand_a=val,
                                operand_b=max_value,
                                rtol=RTOL,
                                atol=ATOL,
                            )
                            or (val <= max_value)
                        )
                    elif strict_max:
                        # noinspection PyTypeChecker
                        return (
                            isclose(
                                operand_a=val,
                                operand_b=min_value,
                                rtol=RTOL,
                                atol=ATOL,
                            )
                            or (min_value <= val)
                        ) and (val < max_value)
                    else:
                        # noinspection PyTypeChecker
                        return (
                            isclose(
                                operand_a=val,
                                operand_b=min_value,
                                rtol=RTOL,
                                atol=ATOL,
                            )
                            or (min_value <= val)
                        ) and (
                            isclose(
                                operand_a=val,
                                operand_b=max_value,
                                rtol=RTOL,
                                atol=ATOL,
                            )
                            or (val <= max_value)
                        )

            elif min_value is None and max_value is not None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_max:
                            return val < max_value
                        else:
                            return val <= max_value
                    except TypeError:
                        return False

                else:
                    if isinstance(val, str) != isinstance(max_value, str):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_max:
                        return val < max_value
                    else:
                        return isclose(
                            operand_a=val,
                            operand_b=max_value,
                            rtol=RTOL,
                            atol=ATOL,
                        ) or (val <= max_value)

            elif min_value is not None and max_value is None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_min:
                            return min_value < val
                        else:
                            return min_value <= val
                    except TypeError:
                        return False

                else:
                    if isinstance(val, str) != isinstance(min_value, str):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_min:
                        return min_value < val
                    else:
                        # noinspection PyTypeChecker
                        return isclose(
                            operand_a=val,
                            operand_b=min_value,
                            rtol=RTOL,
                            atol=ATOL,
                        ) or (min_value <= val)

            else:
                return False

        return temp_column.map(is_between)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        parse_strings_as_datetimes: bool = False,
        **kwargs
    ):
        if parse_strings_as_datetimes:
            # deprecated-v0.13.41
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in \
v0.16. As part of the V3 API transition, we've moved away from input transformation. For more information, \
please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/
""",
                DeprecationWarning,
            )

            if min_value is not None:
                try:
                    min_value = parse(min_value)
                except TypeError:
                    pass

            if max_value is not None:
                try:
                    max_value = parse(max_value)
                except TypeError:
                    pass

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        # TODO: <Alex>Make "rtol" and "atol" customizable at "ExpectationConfiguration" level.</Alex>
        if min_value is None:
            if strict_max:
                return column < sa.literal(max_value)

            if isinstance(max_value, (str, datetime.datetime)):
                return column <= sa.literal(max_value)

            return sa.or_(
                sa.func.abs(column - sa.literal(max_value))
                <= sa.literal(ATOL + RTOL * abs(max_value)),
                column <= sa.literal(max_value),
            )

        elif max_value is None:
            if strict_min:
                return sa.literal(min_value) < column

            if isinstance(min_value, (str, datetime.datetime)):
                return column >= sa.literal(min_value)

            return sa.or_(
                sa.func.abs(sa.literal(min_value) - column)
                <= ATOL + RTOL * sa.func.abs(column),
                column >= sa.literal(min_value),
            )

        else:
            if strict_min and strict_max:
                return sa.and_(
                    sa.literal(min_value) < column, column < sa.literal(max_value)
                )

            if strict_min:
                if isinstance(max_value, (str, datetime.datetime)):
                    return sa.and_(
                        column <= sa.literal(max_value),
                        sa.literal(min_value) < column,
                    )

                return sa.and_(
                    sa.literal(min_value) < column,
                    sa.or_(
                        column <= sa.literal(max_value),
                        sa.func.abs(column - sa.literal(max_value))
                        <= sa.literal(ATOL + RTOL * abs(max_value)),
                    ),
                )

            if strict_max:
                if isinstance(min_value, (str, datetime.datetime)):
                    return sa.and_(
                        column >= sa.literal(min_value),
                        column < sa.literal(max_value),
                    )

                return sa.and_(
                    column < sa.literal(max_value),
                    sa.or_(
                        column >= sa.literal(min_value),
                        sa.func.abs(sa.literal(min_value) - column)
                        <= ATOL + RTOL * sa.func.abs(column),
                    ),
                )

            if (isinstance(max_value, str) and isinstance(min_value, str)) or (
                isinstance(max_value, datetime.datetime)
                and isinstance(min_value, datetime.datetime)
            ):
                return sa.and_(
                    column >= sa.literal(min_value),
                    column <= sa.literal(max_value),
                )

            if isinstance(max_value, (str, datetime.datetime)):
                return sa.and_(
                    column <= sa.literal(max_value),
                    sa.or_(
                        column >= sa.literal(min_value),
                        sa.func.abs(sa.literal(min_value) - column)
                        <= ATOL + RTOL * sa.func.abs(column),
                    ),
                )

            if isinstance(min_value, (str, datetime.datetime)):
                return sa.and_(
                    column >= sa.literal(min_value),
                    sa.or_(
                        column <= sa.literal(max_value),
                        sa.func.abs(column - sa.literal(max_value))
                        <= sa.literal(ATOL + RTOL * abs(max_value)),
                    ),
                )

            return sa.and_(
                sa.or_(
                    column >= sa.literal(min_value),
                    sa.func.abs(sa.literal(min_value) - column)
                    <= ATOL + RTOL * sa.func.abs(column),
                ),
                sa.or_(
                    column <= sa.literal(max_value),
                    sa.func.abs(column - sa.literal(max_value))
                    <= sa.literal(ATOL + RTOL * abs(max_value)),
                ),
            )

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        parse_strings_as_datetimes: bool = False,
        **kwargs
    ):
        if parse_strings_as_datetimes:
            # deprecated-v0.13.41
            warnings.warn(
                """The parameter "parse_strings_as_datetimes" is deprecated as of v0.13.41 in \
v0.16. As part of the V3 API transition, we've moved away from input transformation. For more information, \
please see: https://greatexpectations.io/blog/why_we_dont_do_transformations_for_expectations/
""",
                DeprecationWarning,
            )

            if min_value is not None:
                try:
                    min_value = parse(min_value)
                except TypeError:
                    pass

            if max_value is not None:
                try:
                    max_value = parse(max_value)
                except TypeError:
                    pass

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")

        # TODO: <Alex>Make "rtol" and "atol" customizable at "ExpectationConfiguration" level.</Alex>
        if min_value is None:
            if strict_max:
                return column < F.lit(max_value)

            if isinstance(max_value, (str, datetime.datetime)):
                return column <= F.lit(max_value)

            return (
                F.abs(column - F.lit(max_value)) <= F.lit(ATOL + RTOL * abs(max_value))
            ) | (column <= F.lit(max_value))

        elif max_value is None:
            if strict_min:
                return F.lit(min_value) < column

            if isinstance(min_value, (str, datetime.datetime)):
                return F.lit(min_value) <= column

            return (F.abs(F.lit(min_value) - column) <= ATOL + RTOL * F.abs(column)) | (
                F.lit(min_value) <= column
            )

        else:
            if strict_min and strict_max:
                return (F.lit(min_value) < column) & (column < F.lit(max_value))

            if strict_min:
                if isinstance(max_value, (str, datetime.datetime)):
                    return (F.lit(min_value) < column) & (column <= F.lit(max_value))

                return (F.lit(min_value) < column) & (
                    (
                        F.abs(column - F.lit(max_value))
                        <= F.lit(ATOL + RTOL * abs(max_value))
                    )
                    | (column <= F.lit(max_value))
                )

            if strict_max:
                if isinstance(min_value, (str, datetime.datetime)):
                    return (F.lit(min_value) <= column) & (column < F.lit(max_value))

                return (
                    (F.abs(F.lit(min_value) - column) <= ATOL + RTOL * F.abs(column))
                    | (F.lit(min_value) <= column)
                ) & (column < F.lit(max_value))

            if (isinstance(max_value, str) and isinstance(min_value, str)) or (
                isinstance(max_value, datetime.datetime)
                and isinstance(min_value, datetime.datetime)
            ):
                return (F.lit(min_value) <= column) & (column <= F.lit(max_value))

            if isinstance(max_value, (str, datetime.datetime)):
                return (
                    (F.abs(F.lit(min_value) - column) <= ATOL + RTOL * F.abs(column))
                    | (F.lit(min_value) <= column)
                ) & (column <= F.lit(max_value))

            if isinstance(min_value, (str, datetime.datetime)):
                return (F.lit(min_value) <= column) & (
                    (
                        F.abs(column - F.lit(max_value))
                        <= F.lit(ATOL + RTOL * abs(max_value))
                    )
                    | (column <= F.lit(max_value))
                )

            return (
                (F.abs(F.lit(min_value) - column) <= ATOL + RTOL * F.abs(column))
                | (F.lit(min_value) <= column)
            ) & (
                (
                    F.abs(column - F.lit(max_value))
                    <= F.lit(ATOL + RTOL * abs(max_value))
                )
                | (column <= F.lit(max_value))
            )
