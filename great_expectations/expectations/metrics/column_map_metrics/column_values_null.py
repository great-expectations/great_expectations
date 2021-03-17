import copy
from typing import Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import (
    MetricProvider,
    metric_value,
)
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnValuesNull(ColumnMapMetricProvider):
    condition_metric_name = "column_values.null"
    filter_column_isnull = False

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column.isnull()

    @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        return column == None

    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return column.isNull()


class ColumnValuesNullCount(MetricProvider):
    """A convenience class to provide an alias for easier access to the null count in a column."""

    metric_name = "column_values.null.count"

    @metric_value(engine=PandasExecutionEngine)
    def _pandas(*, metrics, **kwargs):
        return metrics["column_values.nonnull.unexpected_count"]

    @metric_value(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(*, metrics, **kwargs):
        return metrics["column_values.nonnull.unexpected_count"]

    @metric_value(engine=SparkDFExecutionEngine)
    def _spark(*, metrics, **kwargs):
        return metrics["column_values.nonnull.unexpected_count"]

    @classmethod
    def _get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        dependencies = super()._get_evaluation_dependencies(
            metric=metric,
            configuration=configuration,
            execution_engine=execution_engine,
            runtime_configuration=runtime_configuration,
        )

        dependencies.update(
            {
                "column_values.nonnull.unexpected_count": MetricConfiguration(
                    "column_values.nonnull.unexpected_count",
                    metric.metric_domain_kwargs,
                ),
            }
        )

        metric_domain_kwargs: dict = metric.metric_domain_kwargs

        if "column" in metric.metric_domain_kwargs:
            metric_domain_kwargs = copy.deepcopy(metric_domain_kwargs)
            metric_domain_kwargs.pop("column")

        dependencies.update(
            {
                "table.columns": MetricConfiguration(
                    metric_name="table.columns",
                    metric_domain_kwargs=metric_domain_kwargs,
                    metric_value_kwargs=None,
                    metric_dependencies={
                        "table.column_types": MetricConfiguration(
                            metric_name="table.column_types",
                            metric_domain_kwargs=metric.metric_domain_kwargs,
                            metric_value_kwargs={
                                "include_nested": True,
                            },
                            metric_dependencies=None,
                        ),
                    },
                )
            }
        )

        return dependencies
