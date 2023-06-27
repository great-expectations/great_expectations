import logging
from typing import TYPE_CHECKING, Optional

import pandas as pd

from great_expectations.compatibility.pyspark import functions as F
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core import ExpectationConfiguration
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
from great_expectations.util import (
    convert_ndarray_decimal_to_float_dtype,
    does_ndarray_contain_decimal_dtype,
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)


class ColumnStandardDeviation(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Standard Deviation metric"""

    metric_name = "column.standard_deviation"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Standard Deviation implementation"""
        column_data: np.ndarray = column.to_numpy()
        column_has_decimal: bool = does_ndarray_contain_decimal_dtype(data=column_data)
        if column_has_decimal:
            column_data = convert_ndarray_decimal_to_float_dtype(data=column_data)
            column.update(pd.Series(column_data))

        return column.std()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _dialect, _metrics, **kwargs):
        """SqlAlchemy Standard Deviation implementation"""
        if _dialect.name.lower() == GXSqlDialect.MSSQL:
            standard_deviation = sa.func.stdev(column)
        elif _dialect.name.lower() == GXSqlDialect.SQLITE:
            mean = _metrics["column.mean"]
            nonnull_row_count = _metrics[
                f"column_values.null.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
            ]
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
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        """Returns a dictionary of given metric names and their corresponding configuration, specifying the metric
        types and their respective domains"""
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
