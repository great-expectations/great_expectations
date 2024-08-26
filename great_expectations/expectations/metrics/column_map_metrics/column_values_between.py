from __future__ import annotations

import datetime
from typing import Optional, Union

import pandas as pd
from dateutil.parser import parse

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


class ColumnValuesBetween(ColumnMapMetricProvider):
    condition_metric_name = "column_values.between"
    condition_value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(  # noqa: C901
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs,
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003

        temp_column = column

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")  # noqa: TRY003

        # Use a vectorized approach for native numpy dtypes
        if column.dtype in [int, float]:
            return cls._pandas_vectorized(temp_column, min_value, max_value, strict_min, strict_max)
        elif isinstance(column.dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_ns_dtype(
            column.dtype
        ):
            if min_value is not None and isinstance(min_value, str):
                min_value = parse(min_value)

            if max_value is not None and isinstance(max_value, str):
                max_value = parse(max_value)

            return cls._pandas_vectorized(temp_column, min_value, max_value, strict_min, strict_max)

        def is_between(val):  # noqa: C901, PLR0911, PLR0912
            # TODO Might be worth explicitly defining comparisons between types (for example, between strings and ints).  # noqa: E501
            # Ensure types can be compared since some types in Python 3 cannot be logically compared.  # noqa: E501
            # print type(val), type(min_value), type(max_value), val, min_value, max_value

            if type(val) is None:
                return False

            if min_value is not None and max_value is not None:
                # Type of column values is either string or specific rich type (or "None").  In all cases, type of  # noqa: E501
                # column must match type of constant being compared to column value (otherwise, error is raised).  # noqa: E501
                if (isinstance(val, str) != isinstance(min_value, str)) or (
                    isinstance(val, str) != isinstance(max_value, str)
                ):
                    raise TypeError(  # noqa: TRY003
                        "Column values, min_value, and max_value must either be None or of the same type."  # noqa: E501
                    )

                if strict_min and strict_max:
                    return (val > min_value) and (val < max_value)

                if strict_min:
                    return (val > min_value) and (val <= max_value)

                if strict_max:
                    return (val >= min_value) and (val < max_value)

                return (val >= min_value) and (val <= max_value)

            elif min_value is None and max_value is not None:
                # Type of column values is either string or specific rich type (or "None").  In all cases, type of  # noqa: E501
                # column must match type of constant being compared to column value (otherwise, error is raised).  # noqa: E501
                if isinstance(val, str) != isinstance(max_value, str):
                    raise TypeError(  # noqa: TRY003
                        "Column values, min_value, and max_value must either be None or of the same type."  # noqa: E501
                    )

                if strict_max:
                    return val < max_value

                return val <= max_value

            elif min_value is not None and max_value is None:
                # Type of column values is either string or specific rich type (or "None").  In all cases, type of  # noqa: E501
                # column must match type of constant being compared to column value (otherwise, error is raised).  # noqa: E501
                if isinstance(val, str) != isinstance(min_value, str):
                    raise TypeError(  # noqa: TRY003
                        "Column values, min_value, and max_value must either be None or of the same type."  # noqa: E501
                    )

                if strict_min:
                    return val > min_value

                return val >= min_value

            else:
                return False

        return temp_column.map(is_between)

    @classmethod
    def _pandas_vectorized(  # noqa: C901, PLR0911
        cls,
        column: pd.Series,
        min_value: Optional[Union[int, float, datetime.datetime]],
        max_value: Optional[Union[int, float, datetime.datetime]],
        strict_min: bool,
        strict_max: bool,
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003

        if min_value is None:
            if strict_max:
                return column < max_value
            else:
                return column <= max_value

        if max_value is None:
            if strict_min:
                return min_value < column
            else:
                return min_value <= column

        if strict_min and strict_max:
            return (min_value < column) & (column < max_value)
        elif strict_min:
            return (min_value < column) & (column <= max_value)
        elif strict_max:
            return (min_value <= column) & (column < max_value)
        else:
            return (min_value <= column) & (column <= max_value)

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(  # noqa: C901, PLR0911
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs,
    ):
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")  # noqa: TRY003

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003

        if min_value is None:
            if strict_max:
                return column < sa.literal(max_value)

            return column <= sa.literal(max_value)

        elif max_value is None:
            if strict_min:
                return column > sa.literal(min_value)

            return column >= sa.literal(min_value)

        else:
            if strict_min and strict_max:
                return sa.and_(
                    column > sa.literal(min_value),
                    column < sa.literal(max_value),
                )

            if strict_min:
                return sa.and_(
                    column > sa.literal(min_value),
                    column <= sa.literal(max_value),
                )

            if strict_max:
                return sa.and_(
                    column >= sa.literal(min_value),
                    column < sa.literal(max_value),
                )

            return sa.and_(
                column >= sa.literal(min_value),
                column <= sa.literal(max_value),
            )

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(  # noqa: C901, PLR0911
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs,
    ):
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")  # noqa: TRY003

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003

        if min_value is None:
            if strict_max:
                return column < F.lit(max_value)

            return column <= F.lit(max_value)

        elif max_value is None:
            if strict_min:
                return column > F.lit(min_value)

            return column >= F.lit(min_value)

        else:
            if strict_min and strict_max:
                return (column > F.lit(min_value)) & (column < F.lit(max_value))

            if strict_min:
                return (column > F.lit(min_value)) & (column <= F.lit(max_value))

            if strict_max:
                return (column >= F.lit(min_value)) & (column < F.lit(max_value))

            return (column >= F.lit(min_value)) & (column <= F.lit(max_value))
