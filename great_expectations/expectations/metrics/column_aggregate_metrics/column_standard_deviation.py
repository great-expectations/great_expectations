from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.util import convert_pandas_series_decimal_to_float_dtype
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )

logger = logging.getLogger(__name__)


class ColumnStandardDeviation(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Standard Deviation metric"""

    metric_name = "column.standard_deviation"

    # A sample standard deviation divides by n - 1, so it is undefined below this many values.
    MIN_ROWS_FOR_SAMPLE_STDEV = 2

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Standard Deviation implementation"""
        convert_pandas_series_decimal_to_float_dtype(data=column, inplace=True)
        return column.std()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _dialect, _metrics, **kwargs):
        """SqlAlchemy Standard Deviation implementation"""
        dialect_name = _dialect.name.lower()
        if dialect_name == GXSqlDialect.SQL_SERVER:
            standard_deviation = sa.func.stdev(column)
        elif dialect_name == GXSqlDialect.SQLITE:
            # SQLite has no stddev_samp, so the sample standard deviation is computed in
            # two passes from the mean. This branch must live on the base provider rather
            # than on an engine subclass: a sqlite connection added through
            # context.data_sources.add_sql(...) produces a plain SqlAlchemyExecutionEngine,
            # and a subclass override registered against SqliteExecutionEngine is never
            # consulted for it.
            mean = _metrics["column.mean"]
            nonnull_row_count = _metrics[
                f"column_values.null.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
            ]
            # SQLite has no stddev_samp to return NULL on our behalf when the sample standard
            # deviation is undefined, and the formula below does not degrade gracefully: with one
            # non-null value it divides by zero, and with none it hands a NULL numerator to the
            # sqrt UDF. Both surface as an opaque OperationalError. An empty table resolves
            # nonnull_row_count to None, which fails in Python before any SQL is built.
            # max() of nothing but NULLs is NULL; the aggregate is what keeps this a single-value
            # statistic, which the bundled metric resolver requires.
            if nonnull_row_count is None or nonnull_row_count < cls.MIN_ROWS_FOR_SAMPLE_STDEV:
                return sa.func.max(sa.null())
            standard_deviation = sa.func.sqrt(
                sa.func.sum((1.0 * column - mean) * (1.0 * column - mean))
                / ((1.0 * nonnull_row_count) - 1.0)
            )
        else:
            standard_deviation = sa.func.stddev_samp(column)

        return standard_deviation

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        """Spark Standard Deviation implementation"""
        return F.stddev_samp(column)

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

        if isinstance(execution_engine, SqlAlchemyExecutionEngine):
            dependencies["column.mean"] = MetricConfiguration(
                metric_name="column.mean",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=None,
            )
            dependencies[
                f"column_values.null.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
            ] = MetricConfiguration(
                metric_name=f"column_values.null.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=None,
            )

        return dependencies
