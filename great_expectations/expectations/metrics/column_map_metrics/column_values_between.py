from __future__ import annotations

import datetime
from typing import Optional, Union

import pandas as pd
from dateutil.parser import parse

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.not_imported import is_version_greater_or_equal
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


class InvalidColumnTypeError(ValueError):
    def __init__(self, column_type: str):
        message = f"ColumnValuesBetween metrics cannot be computed on column of type {column_type}."
        super().__init__(message)


# Column types that cannot be meaningfully compared against numeric/date bounds.
# Matched case-insensitively against the column's type name via startswith, which covers
# both SQL type names (e.g. "VARCHAR") and Spark type reprs (e.g. "StringType()").
INVALID_COLUMN_TYPES = (
    "VARCHAR",
    "CHAR",
    "NVARCHAR",
    "NCHAR",
    "TEXT",
    "STRING",
    "BOOLEAN",
    "BOOL",
    "BIT",
    "TINYTEXT",
    "MEDIUMTEXT",
    "LONGTEXT",
)


def _raise_if_invalid_column_type(column_type: Optional[str]) -> None:
    """Raise InvalidColumnTypeError if the column type cannot be compared to numeric/date bounds.

    Under engines that reject cross-type comparisons (e.g. Spark with ANSI enabled), an
    implicit string<->numeric comparison would otherwise surface as an opaque engine error.
    """
    if column_type and column_type.upper().startswith(INVALID_COLUMN_TYPES):
        raise InvalidColumnTypeError(column_type=column_type)


def _column_type_from_metrics(metrics: dict, column_name: Optional[str]) -> Optional[str]:
    """Look up a column's type name from the resolved table.column_types metric."""
    column_types = metrics.get("table.column_types", [])
    type_by_column = {ct.get("name"): str(ct.get("type", "")) for ct in column_types}
    return type_by_column.get(column_name)


def _should_reject_incomparable_spark_column_type(min_value, max_value) -> bool:
    """Decide whether an incomparable Spark column type should be rejected up front.

    Only Spark 4 (and later) rejects an implicit string<->numeric comparison under ANSI
    mode; earlier versions coerce instead of raising, so their results are left untouched
    (byte-identical). Even under Spark 4, comparing a string/boolean column against string
    bounds is a valid same-family comparison (e.g. ISO-date or version-string ranges), so
    that case is not rejected -- only genuine string<->numeric comparisons are.
    """
    if not (pyspark.pyspark and is_version_greater_or_equal(pyspark.pyspark.__version__, "4.0.0")):
        return False
    bounds = [b for b in (min_value, max_value) if b is not None]
    return not (bounds and all(isinstance(b, str) for b in bounds))


class ColumnValuesBetween(ColumnMapMetricProvider):
    condition_metric_name = "column_values.between"
    condition_value_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(  # noqa: C901 # FIXME CoP
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs,
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003 # FIXME CoP

        temp_column = column

        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")  # noqa: TRY003 # FIXME CoP

        # Use a vectorized approach for native numpy dtypes
        if column.dtype in [int, float]:
            return cls._pandas_vectorized(temp_column, min_value, max_value, strict_min, strict_max)
        elif isinstance(column.dtype, pd.DatetimeTZDtype) or pd.api.types.is_datetime64_any_dtype(
            column.dtype
        ):
            if min_value is not None and isinstance(min_value, str):
                min_value = parse(min_value)

            if max_value is not None and isinstance(max_value, str):
                max_value = parse(max_value)

            # Coerce plain datetime.date bounds to pd.Timestamp so the vectorised comparison
            # does not raise "Cannot compare Timestamp with datetime.date" on pandas 2.3+.
            if min_value is not None and isinstance(min_value, datetime.date) and not isinstance(
                min_value, datetime.datetime
            ):
                min_value = pd.Timestamp(min_value)

            if max_value is not None and isinstance(max_value, datetime.date) and not isinstance(
                max_value, datetime.datetime
            ):
                max_value = pd.Timestamp(max_value)

            return cls._pandas_vectorized(temp_column, min_value, max_value, strict_min, strict_max)

        def is_between(val):  # noqa: C901, PLR0911, PLR0912 # FIXME CoP
            # TODO Might be worth explicitly defining comparisons between types (for example, between strings and ints).  # noqa: E501 # FIXME CoP
            # Ensure types can be compared since some types in Python 3 cannot be logically compared.  # noqa: E501 # FIXME CoP
            # print type(val), type(min_value), type(max_value), val, min_value, max_value

            if type(val) is None:
                return False

            if min_value is not None and max_value is not None:
                # Type of column values is either string or specific rich type (or "None").  In all cases, type of  # noqa: E501 # FIXME CoP
                # column must match type of constant being compared to column value (otherwise, error is raised).  # noqa: E501 # FIXME CoP
                if (isinstance(val, str) != isinstance(min_value, str)) or (
                    isinstance(val, str) != isinstance(max_value, str)
                ):
                    raise TypeError(  # noqa: TRY003 # FIXME CoP
                        "Column values, min_value, and max_value must either be None or of the same type."  # noqa: E501 # FIXME CoP
                    )

                if strict_min and strict_max:
                    return (val > min_value) and (val < max_value)

                if strict_min:
                    return (val > min_value) and (val <= max_value)

                if strict_max:
                    return (val >= min_value) and (val < max_value)

                return (val >= min_value) and (val <= max_value)

            elif min_value is None and max_value is not None:
                # Type of column values is either string or specific rich type (or "None").  In all cases, type of  # noqa: E501 # FIXME CoP
                # column must match type of constant being compared to column value (otherwise, error is raised).  # noqa: E501 # FIXME CoP
                if isinstance(val, str) != isinstance(max_value, str):
                    raise TypeError(  # noqa: TRY003 # FIXME CoP
                        "Column values, min_value, and max_value must either be None or of the same type."  # noqa: E501 # FIXME CoP
                    )

                if strict_max:
                    return val < max_value

                return val <= max_value

            elif min_value is not None and max_value is None:
                # Type of column values is either string or specific rich type (or "None").  In all cases, type of  # noqa: E501 # FIXME CoP
                # column must match type of constant being compared to column value (otherwise, error is raised).  # noqa: E501 # FIXME CoP
                if isinstance(val, str) != isinstance(min_value, str):
                    raise TypeError(  # noqa: TRY003 # FIXME CoP
                        "Column values, min_value, and max_value must either be None or of the same type."  # noqa: E501 # FIXME CoP
                    )

                if strict_min:
                    return val > min_value

                return val >= min_value

            else:
                return False

        return temp_column.map(is_between)

    @classmethod
    def _pandas_vectorized(  # noqa: C901, PLR0911 # FIXME CoP
        cls,
        column: pd.Series,
        min_value: Optional[Union[int, float, datetime.datetime]],
        max_value: Optional[Union[int, float, datetime.datetime]],
        strict_min: bool,
        strict_max: bool,
    ):
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003 # FIXME CoP

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
    def _sqlalchemy(  # noqa: C901, PLR0911 # FIXME CoP
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs,
    ):
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")  # noqa: TRY003 # FIXME CoP

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003 # FIXME CoP

        # Check that the generated SQL won't raise an error.
        # ColumnValuesBetween metrics only work on numbers/dates,
        # so we check for common string and boolean types.

        # Check that the comparison won't raise on an incomparable column type.
        metrics = kwargs.get("_metrics", {})
        column_type = _column_type_from_metrics(metrics, column.name)
        _raise_if_invalid_column_type(column_type)

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
    def _spark(  # noqa: C901, PLR0911 # FIXME CoP
        cls,
        column,
        min_value=None,
        max_value=None,
        strict_min=None,
        strict_max=None,
        **kwargs,
    ):
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")  # noqa: TRY003 # FIXME CoP

        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003 # FIXME CoP

        # Reject incomparable column types up front so an implicit string<->numeric
        # comparison does not surface as an opaque engine error under ANSI mode.
        if _should_reject_incomparable_spark_column_type(min_value, max_value):
            metrics = kwargs.get("_metrics", {})
            accessor_domain_kwargs = kwargs.get("_accessor_domain_kwargs", {})
            column_type = _column_type_from_metrics(metrics, accessor_domain_kwargs.get("column"))
            _raise_if_invalid_column_type(column_type)

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
