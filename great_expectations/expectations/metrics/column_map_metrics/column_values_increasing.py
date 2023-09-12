import datetime
from typing import Any, Dict

import pandas as pd
from dateutil.parser import parse

from great_expectations.compatibility import pyspark
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
from great_expectations.warnings import warn_deprecated_parse_strings_as_datetimes


class ColumnValuesIncreasing(ColumnMapMetricProvider):
    condition_metric_name = "column_values.increasing"
    condition_value_keys = (
        "strictly",
        "parse_strings_as_datetimes",
    )
    default_kwarg_values = {
        "strictly": False,
        "parse_strings_as_datetimes": False,
    }

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(
        cls,
        column,
        **kwargs,
    ):
        parse_strings_as_datetimes: bool = (
            kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column
        else:
            temp_column = column

        series_diff = temp_column.diff()
        # The first element is null, so it gets a bye and is always treated as True
        if parse_strings_as_datetimes:
            series_diff[series_diff.isnull()] = datetime.timedelta(seconds=1)
            series_diff = pd.to_timedelta(series_diff, unit="S")
        else:
            series_diff[series_diff.isnull()] = 1

        strictly: bool = kwargs.get("strictly") or False
        if strictly:
            if parse_strings_as_datetimes:
                return series_diff.dt.total_seconds() > 0.0  # noqa: PLR2004
            return series_diff > 0
        else:
            if parse_strings_as_datetimes:
                return series_diff.dt.total_seconds() >= 0.0  # noqa: PLR2004
            return series_diff >= 0

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _spark(  # noqa: PLR0913
        cls,
        execution_engine: SparkDFExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: Dict[str, Any],
        runtime_configuration: dict,
    ):
        parse_strings_as_datetimes: bool = (
            metric_value_kwargs.get("parse_strings_as_datetimes") or False
        )
        if parse_strings_as_datetimes:
            warn_deprecated_parse_strings_as_datetimes()

        # check if column is any type that could have na (numeric types)
        column_name = metric_domain_kwargs["column"]
        table_columns = metrics["table.column_types"]
        column_metadata = [col for col in table_columns if col["name"] == column_name][
            0
        ]
        if isinstance(
            column_metadata["type"],
            (
                pyspark.types.LongType,
                pyspark.types.DoubleType,
                pyspark.types.IntegerType,
            ),
        ):
            # if column is any type that could have NA values, remove them (not filtered by .isNotNull())
            compute_domain_kwargs = execution_engine.add_column_row_condition(
                metric_domain_kwargs,
                filter_null=cls.filter_column_isnull,
                filter_nan=True,
            )
        else:
            compute_domain_kwargs = metric_domain_kwargs

        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            compute_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )

        # NOTE: 20201105 - parse_strings_as_datetimes is not supported here;
        # instead detect types naturally
        column = F.col(column_name)
        if isinstance(
            column_metadata["type"],
            (
                pyspark.types.TimestampType,
                pyspark.types.DateType,
            ),
        ):
            diff = F.datediff(
                column,
                F.lag(column).over(pyspark.Window.orderBy(F.lit("constant"))),
            )
        else:
            diff = column - F.lag(column).over(
                pyspark.Window.orderBy(F.lit("constant"))
            )
            diff = F.when(diff.isNull(), 1).otherwise(diff)

        # NOTE: because in spark we are implementing the window function directly,
        # we have to return the *unexpected* condition.
        # If we expect values to be *strictly* increasing then unexpected values are those
        # that are flat or decreasing
        if metric_value_kwargs["strictly"] is True:
            return (
                F.when(diff <= 0, F.lit(True)).otherwise(F.lit(False)),
                compute_domain_kwargs,
                accessor_domain_kwargs,
            )
        # If we expect values to be flat or increasing then unexpected values are those
        # that are decreasing
        else:
            return (
                F.when(diff < 0, F.lit(True)).otherwise(F.lit(False)),
                compute_domain_kwargs,
                accessor_domain_kwargs,
            )
