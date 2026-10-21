from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from great_expectations.compatibility import pyspark
from great_expectations.compatibility.not_imported import is_version_greater_or_equal
from great_expectations.compatibility.pyspark import functions as F
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.core.metric_function_types import MetricPartialFunctionTypes
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import metric_partial


class ColumnValuesDecreasing(ColumnMapMetricProvider):
    condition_metric_name = "column_values.decreasing"
    condition_value_keys = ("strictly",)
    default_kwarg_values = {
        "strictly": False,
    }

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        column,
        **kwargs,
    ):
        temp_column = column

        series_diff = temp_column.diff()
        # A datetime column's diff is a timedelta, which compares against a zero timedelta
        # rather than the int 0, so take the zero from the diff's own dtype.
        zero = pd.Timedelta(0) if pd.api.types.is_timedelta64_dtype(series_diff.dtype) else 0

        strictly: bool = kwargs.get("strictly") or False
        if strictly:
            in_order = series_diff < zero
        else:
            in_order = series_diff <= zero

        # The first element's diff is null, so it gets a bye and is always treated as True
        return in_order | series_diff.isnull()

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _spark(
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        # check if column is any type that could have na (numeric types)
        column_name = metric_domain_kwargs["column"]
        table_columns = metrics["table.column_types"]
        column_metadata = [col for col in table_columns if col["name"] == column_name][0]
        if isinstance(
            column_metadata["type"],
            (
                pyspark.types.LongType,
                pyspark.types.DoubleType,
                pyspark.types.IntegerType,
            ),
        ):
            # if column is any type that could have NA values, remove them (not filtered by .isNotNull())  # noqa: E501 # FIXME CoP
            compute_domain_kwargs = execution_engine.add_column_row_condition(
                metric_domain_kwargs,
                filter_null=cls.filter_column_isnull,
                filter_nan=True,
            )
        else:
            compute_domain_kwargs = metric_domain_kwargs

        (
            _df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(compute_domain_kwargs, MetricDomainTypes.COLUMN)

        # instead detect types naturally
        column = F.col(column_name)
        if isinstance(
            column_metadata["type"],
            (pyspark.types.TimestampType, pyspark.types.DateType),
        ):
            diff = F.datediff(
                column,
                F.lag(column).over(pyspark.Window.orderBy(F.lit("constant"))),
            )
        else:
            # Subtracting adjacent integral values can overflow a LongType accumulator at
            # extreme opposite-sign boundaries. Spark 4 evaluates arithmetic under ANSI
            # semantics by default and raises ARITHMETIC_OVERFLOW there, whereas Spark 3
            # silently wraps. On Spark 4 widen integral columns to an exact wide DECIMAL
            # before subtracting so the difference (its sign is all that matters below) is
            # computed without overflow and without the precision loss a DoubleType would
            # introduce above 2**53. On Spark 3 the column is left untouched so results
            # stay byte-identical.
            diff_column = column
            if pyspark.pyspark and is_version_greater_or_equal(
                pyspark.pyspark.__version__, "4.0.0"
            ):
                integral_types = (
                    pyspark.types.ByteType,
                    pyspark.types.ShortType,
                    pyspark.types.IntegerType,
                    pyspark.types.LongType,
                )
                if isinstance(column_metadata["type"], integral_types):
                    diff_column = column.cast(pyspark.types.DecimalType(38, 0))
            diff = diff_column - F.lag(diff_column).over(pyspark.Window.orderBy(F.lit("constant")))
            diff = F.when(diff.isNull(), -1).otherwise(diff)

        # NOTE: because in spark we are implementing the window function directly,
        # we have to return the *unexpected* condition
        if metric_value_kwargs["strictly"]:
            return (
                F.when(diff >= 0, F.lit(True)).otherwise(F.lit(False)),
                compute_domain_kwargs,
                accessor_domain_kwargs,
            )
        # If we expect values to be flat or decreasing then unexpected values are those
        # that are decreasing
        else:
            return (
                F.when(diff > 0, F.lit(True)).otherwise(F.lit(False)),
                compute_domain_kwargs,
                accessor_domain_kwargs,
            )
