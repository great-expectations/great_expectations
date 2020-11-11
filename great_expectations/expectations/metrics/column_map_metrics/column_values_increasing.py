from typing import Optional

import pandas as pd

from great_expectations.core import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_map_metric import (
    ColumnMapMetricProvider,
    column_map_condition,
)
from great_expectations.expectations.metrics.import_manager import F, Window, sparktypes
from great_expectations.validator.validation_graph import MetricConfiguration


class ColumnValuesIncreasing(ColumnMapMetricProvider):
    condition_metric_name = "column_values.increasing"
    condition_value_keys = ("strictly",)

    @column_map_condition(engine=PandasExecutionEngine)
    def _pandas(cls, column, strictly=None, **kwargs):
        series_diff = column.diff()
        # The first element is null, so it gets a bye and is always treated as True
        series_diff[series_diff.isnull()] = 1

        if strictly:
            return series_diff > 0
        else:
            return series_diff >= 0

    @column_map_condition(engine=SparkDFExecutionEngine)
    def _spark(cls, column, strictly, _metrics, _accessor_domain_kwargs, **kwargs):
        # check if column is any type that could have na (numeric types)
        column_name = _accessor_domain_kwargs["column"]
        table_columns = _metrics["table.column_types"]
        column_metadata = [col for col in table_columns if col["name"] == column_name]
        na_types = [
            isinstance(column_metadata["dataType"], typ)
            for typ in [
                sparktypes.LongType,
                sparktypes.DoubleType,
                sparktypes.IntegerType,
            ]
        ]

        # if column is any type that could have NA values, remove them (not filtered by .isNotNull())
        if any(na_types):
            column = column.filter(~F.isnan(column))

        # NOTE: 20201105 - parse_strings_as_datetimes is not supported here; instead detect types naturally
        if isinstance(
            column_metadata["dataType"], (sparktypes.TimestampType, sparktypes.DateType)
        ):
            diff = F.datediff(
                column, F.lag(column).over(Window.orderBy(F.lit("constant")))
            )
        else:
            diff = F.col(column - F.lag(column).over(Window.orderBy(F.lit("constant"))))
            diff = F.when(diff.isNull(), -1).otherwise(diff)

        if strictly:
            return F.when(diff >= -1, F.lit(True)).otherwise(F.lit(False))

        else:
            return F.when(diff >= 0, F.lit(True)).otherwise(F.lit(False))

    def get_evaluation_dependencies(
        cls,
        metric: MetricConfiguration,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        if isinstance(execution_engine, SparkDFExecutionEngine):
            return {
                "table.column_types": MetricConfiguration(
                    "table.column_types",
                    metric.metric_domain_kwargs,
                    {"include_nested": True},
                )
            }

        return dict()
