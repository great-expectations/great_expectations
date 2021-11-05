from typing import Optional

import numpy as np
import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.exceptions.exceptions import MetricResolutionError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.import_manager import (
    F,
    Window,
    sa,
    sparktypes,
)
from great_expectations.expectations.metrics.map_metric_provider import (
    ColumnMapMetricProvider,
    column_condition_partial,
    column_function_partial,
    metric_partial,
)
from great_expectations.validator.metric_configuration import MetricConfiguration


class ColumnValuesStringIntegersMonotonicallyIncreasing(ColumnMapMetricProvider):
    function_metric_name = "column_values.string_integers.monotonically_increasing"
    function_value_keys = tuple()

    @column_function_partial(engine=PandasExecutionEngine)
    def _pandas(self, data, _metrics, **kwargs):

        try:
            temp_column = [int(x) for x in data if x.isdigit()]
        except AttributeError:
            raise TypeError(
                "Column must be a string-type capable of being cast to int."
            )

        if len(temp_column) != len(data):
            raise TypeError(
                "Column must be a string-type capable of being cast to int."
            )

        series_diff = np.diff(temp_column)

        return series_diff >= 0

    @metric_partial(
        engine=SparkDFExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.WINDOW_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _spark(
        cls,
        execution_engine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        column_name = metric_domain_kwargs["column"]
        table_columns = metrics["table.column_types"]
        column_metadata = [col for col in table_columns if col["name"] == column_name][
            0
        ]

        if isinstance(column_metadata["type"], (sparktypes.StringType)):
            try:
                column = F.col(column_name).cast(sparktypes.IntegerType())
            except TypeError:
                raise TypeError(
                    "Column must be a string-type capable of being cast to int."
                )
        else:
            raise TypeError(
                "Column must be a string-type capable of being cast to int."
            )

        compute_domain_kwargs = metric_domain_kwargs

        (
            df,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            compute_domain_kwargs, domain_type=MetricDomainTypes.COLUMN
        )

        if any(np.array(df.select(column.isNull()).collect())):
            raise TypeError(
                "Column must be a string-type capable of being cast to int."
            )

        diff = column - F.lag(column).over(Window.orderBy(F.lit("constant")))
        diff = F.when(diff.isNull(), 1).otherwise(diff)
        diff = F.when(diff < 0, F.lit(False)).otherwise(F.lit(True))

        return (
            np.array(df.select(diff).collect()).reshape(-1)[1:],
            compute_domain_kwargs,
            accessor_domain_kwargs,
        )

    # @metric_partial(
    #     engine=SqlAlchemyExecutionEngine,
    #     partial_fn_type=MetricPartialFunctionTypes.WINDOW_FN,
    # )

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

        table_domain_kwargs: dict = {
            k: v for k, v in metric.metric_domain_kwargs.items() if k != "column"
        }
        dependencies["table.column_types"] = MetricConfiguration(
            metric_name="table.column_types",
            metric_domain_kwargs=table_domain_kwargs,
            metric_value_kwargs={
                "include_nested": True,
            },
            metric_dependencies=None,
        )

        return dependencies
