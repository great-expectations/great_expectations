import hashlib
import logging
import math
from typing import Optional

from great_expectations.core import ExpectationConfiguration
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
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.validator.metric_configuration import MetricConfiguration

logger = logging.getLogger(__name__)

try:
    from pyspark.sql.functions import stddev_samp  # noqa: F401
except ImportError as e:
    logger.debug(str(e))
    logger.debug(
        "Unable to load spark context; install optional spark dependency for support."
    )


class ColumnStandardDeviation(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Standard Deviation metric"""

    metric_name = "column.standard_deviation"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Standard Deviation implementation"""
        return column.std()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, _dialect, _metrics, **kwargs):
        """SqlAlchemy Standard Deviation implementation"""
        if _dialect.name.lower() == GXSqlDialect.MSSQL:
            standard_deviation = sa.func.stdev(column)
        elif _dialect.name.lower() == GXSqlDialect.SQLITE:
            _sqlalchemy_engine_backup = kwargs.get("_sqlalchemy_engine_backup")
            if not isinstance(_sqlalchemy_engine_backup, sa.engine.base.Connection):
                assert (
                    _sqlalchemy_engine_backup is not None
                ), f'Using "{cls.__name__}" metric with "{GXSqlDialect.SQLITE.value}" requires ability to exercise "sqlite3" "create_function()" method.'
                raw_connection = _sqlalchemy_engine_backup.raw_connection()
                raw_connection.create_function("sqrt", 1, lambda x: math.sqrt(x))
                raw_connection.create_function(
                    "md5",
                    2,
                    lambda x, d: hashlib.md5(str(x).encode("utf-8")).hexdigest()[
                        -1 * d :
                    ],
                )

            mean = _metrics["column.mean"]
            nonnull_row_count = _metrics["column_values.null.unexpected_count"]
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
            dependencies["column_values.null.unexpected_count"] = MetricConfiguration(
                metric_name="column_values.null.unexpected_count",
                metric_domain_kwargs=metric.metric_domain_kwargs,
                metric_value_kwargs=None,
            )

        return dependencies
