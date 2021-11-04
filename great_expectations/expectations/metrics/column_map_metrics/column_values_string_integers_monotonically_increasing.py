import pandas as pd
from typing import Optional

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import F, sa
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
    column_function_partial,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnValuesStringIntegersMonotonicallyIncreasing(ColumnMapMetricProvider):
    function_metric_name = "column_values.string_integers.monotonically_increasing"
    function_value_keys = tuple()


    @column_function_partial(engine=PandasExecutionEngine)
    def _pandas_function(self, data, _metrics, **kwargs):

        try:
            temp_column = data.astype(int)
        except ValueError:
            raise ValueError(
                "Column must be a string-type capable of being cast to int."
            )
            return False

        series_diff = temp_column.diff()
        series_diff[series_diff.isnull()] = 1

        return series_diff >= 0


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

        # if metric.metric_name == "column_values.string_integers.monotonically_increasing.map":
        #     dependencies["column_values.of_type"] = MetricConfiguration(
        #         metric_name="column_values.of_type.condition",
        #         metric_domain_kwargs=metric.metric_domain_kwargs,
        #     )
        #
        #     dependencies["column_values.increasing"] = MetricConfiguration(
        #         metric_name="column_values.increasing.condition",
        #         metric_domain_kwargs=metric.metric_domain_kwargs
        #     )

        return dependencies
