from typing import Optional

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_function_types import SummarizationMetricNameSuffixes
from great_expectations.execution_engine.execution_engine import ExecutionEngine
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    column_aggregate_partial,
)
from great_expectations.expectations.metrics.column_aggregate_metrics.column_standard_deviation import (  # noqa: E501
    ColumnStandardDeviation as BaseColumnStandardDeviation,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

# A sample standard deviation divides by n - 1, so it is undefined below this many values.
MIN_ROWS_FOR_SAMPLE_STDEV = 2


class SqliteExecutionEngine(SqlAlchemyExecutionEngine):
    """SqlAlchemyExecutionEngine for SQLite databases."""

    pass


class ColumnStandardDeviation(BaseColumnStandardDeviation):
    """MetricProvider Class for Aggregate Standard Deviation metric for SQLite databases."""

    # We should change this decorator to compute this metric a completely new way
    @column_aggregate_partial(engine=SqliteExecutionEngine)
    @override
    def _sqlalchemy(cls, column, _dialect, _metrics, **kwargs):
        """Sqlite Standard Deviation implementation"""
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
        if nonnull_row_count is None or nonnull_row_count < MIN_ROWS_FOR_SAMPLE_STDEV:
            return sa.func.max(sa.null())
        standard_deviation = sa.func.sqrt(
            sa.func.sum((1.0 * column - mean) * (1.0 * column - mean))
            / ((1.0 * nonnull_row_count) - 1.0)
        )
        return standard_deviation

    @classmethod
    @override
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        # We don't need to override this here but I wanted to show for completeness
        # If we are changing the decorator on the provider method or we are completely
        # implementing a new datasource, we'll want to override.
        return super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
