from __future__ import annotations

from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_function_types import (
    MetricPartialFunctionTypeSuffixes,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
    column_function_partial,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )


class ColumnValuesZScore(ColumnMapMetricProvider):
    condition_metric_name = "column_values.z_score.under_threshold"
    condition_value_keys = (
        "double_sided",
        "threshold",
    )
    default_kwarg_values = {"double_sided": True, "threshold": None}

    function_metric_name = "column_values.z_score"
    function_value_keys = tuple()

    @column_function_partial(engine=PandasExecutionEngine)
    def _pandas_function(self, column, _metrics, **kwargs):
        # return the z_score values
        mean = _metrics.get("column.mean")
        std_dev = _metrics.get("column.standard_deviation")

        # Only the divide-by-zero (constant column) case is decided here. std_dev comes
        # from column.std(), which is ddof=1 and returns NaN -- never None -- for a column
        # with fewer than two non-null values, so the None limb is unreachable on this
        # engine. It is kept because the shared guard reads the same on all three, and
        # because on SQL the aggregate passes through convert_to_json_serializable, which
        # maps NaN to None; there the limb catches both n<2 and a NaN aggregate.
        #
        # The guard is still load-bearing here: on an object-dtype column
        # (column - mean) / 0.0 raises ZeroDivisionError rather than producing NaN.
        # Undefined variance that does reach the division falls through to
        # _pandas_condition, which accepts a NaN z-score rather than flagging every row.
        if std_dev is None or std_dev == 0:
            return pd.Series(np.nan, index=column.index)
        try:
            return (column - mean) / std_dev
        except TypeError:
            raise (TypeError("Cannot complete Z-score calculations on a non-numerical column."))  # noqa: TRY003 # FIXME CoP

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas_condition(cls, column, _metrics, threshold, double_sided, **kwargs) -> pd.Series:
        z_score: pd.Series
        z_score, _, _ = _metrics[
            f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}"
        ]
        try:
            if double_sided:
                under_threshold = z_score.abs() < abs(threshold)
            else:
                under_threshold = z_score < threshold
            # An undefined z-score compares False against any threshold, which would flag
            # every row as an outlier. Treat it as meeting the expectation instead,
            # matching the NULL comparison semantics of the SQL implementation. This is
            # where undefined variance is resolved on pandas whenever the guard above did
            # not catch it -- a NaN std_dev from fewer than two non-null values, or from a
            # column containing inf -- as well as a constant column of numeric dtype.
            # Required regardless of how wide that guard is, since _pandas_function
            # returns an all-NaN series and NaN < threshold is False.
            return under_threshold | z_score.isna()
        except TypeError:
            raise (TypeError("Cannot check if a string lies under a numerical threshold"))  # noqa: TRY003 # FIXME CoP

    @column_function_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_function(cls, column, _metrics, _dialect, **kwargs):
        mean = _metrics["column.mean"]
        standard_deviation = _metrics["column.standard_deviation"]

        # standard_deviation is an already-resolved Python scalar, so the divide-by-zero
        # (constant column) and undefined (None) cases can be decided here rather than
        # delegated to the database. Dialects disagree on division by zero -- Postgres,
        # SQL Server and friends raise, while SQLite and MySQL return NULL -- so decide
        # it here to keep every data source consistent. The NULL is cast to a numeric
        # type because dialects such as Postgres cannot resolve abs() over an untyped
        # NULL.
        if standard_deviation is None or standard_deviation == 0:
            return sa.cast(sa.null(), sa.Float)
        return (column - mean) / standard_deviation

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy_condition(cls, column, _metrics, threshold, double_sided, **kwargs):
        z_score, _, _ = _metrics[
            f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}"
        ]
        if double_sided:
            under_threshold = sa.func.abs(z_score) < abs(threshold)
        else:
            under_threshold = z_score < threshold

        return under_threshold

    @column_function_partial(engine=SparkDFExecutionEngine)
    def _spark_function(cls, column, _metrics, **kwargs):
        mean = _metrics["column.mean"]
        standard_deviation = _metrics["column.standard_deviation"]

        # standard_deviation is an already-resolved Python scalar, so the divide-by-zero
        # (constant column) and undefined (None) cases can be decided here rather than
        # per row. Dividing by zero would yield Infinity (or raise under ANSI), so return
        # a null z-score in those cases.
        if standard_deviation is None or standard_deviation == 0:
            return F.lit(None)
        return (column - mean) / standard_deviation

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark_condition(cls, column, _metrics, threshold, double_sided, **kwargs):
        z_score, _, _ = _metrics[
            f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}"
        ]

        if double_sided:
            threshold = abs(threshold)
            z_score = F.abs(z_score)

        return z_score < threshold

    @classmethod
    @override
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration, specifying the metric
        types and their respective domains"""  # noqa: E501 # FIXME CoP
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        if (
            metric.metric_name
            == f"column_values.z_score.under_threshold.{MetricPartialFunctionTypeSuffixes.CONDITION.value}"  # noqa: E501 # FIXME CoP
        ):
            dependencies[f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}"] = (
                MetricConfiguration(
                    metric_name=f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}",
                    metric_domain_kwargs=metric.metric_domain_kwargs,
                )
            )

        if (
            metric.metric_name
            == f"column_values.z_score.{MetricPartialFunctionTypeSuffixes.MAP.value}"
        ):
            dependencies["column.mean"] = MetricConfiguration(
                metric_name="column.mean",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )
            dependencies["column.standard_deviation"] = MetricConfiguration(
                metric_name="column.standard_deviation",
                metric_domain_kwargs=metric.metric_domain_kwargs,
            )

        return dependencies
