from typing import Optional

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
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnValuesNull(ColumnMapMetricProvider):
    condition_metric_name = "column_values.null"
    filter_column_isnull = False

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.isnull()

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return column == None  # noqa: E711

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return column.isNull()


class ColumnValuesNullCount(MetricProvider):
    """A convenience class to provide an alias for easier access to the null count in a column."""

    metric_name = "column_values.null.count"

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(*, metrics, **kwargs):
        return metrics[
            f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        ]

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(*, metrics, **kwargs):
        return metrics[
            f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        ]

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(*, metrics, **kwargs):
        return metrics[
            f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        ]

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies: dict = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )
        dependencies[
            f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        ] = MetricConfiguration(
            metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}",
            metric_domain_kwargs=metric.metric_domain_kwargs,
        )
        return dependencies
