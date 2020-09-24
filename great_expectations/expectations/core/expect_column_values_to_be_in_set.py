from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

from ...core.batch import Batch
from ...data_asset.util import parse_result_format
from ...execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from ..expectation import (
    ColumnMapDatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from ..registry import extract_metrics, get_metric_kwargs

try:
    import sqlalchemy as sa
except ImportError:
    pass


class ExpectColumnValuesToBeInSet(ColumnMapDatasetExpectation):
    map_metric = "column_values.in_set"
    metric_dependencies = (
        "column_values.in_set.count",
        "column_values.nonnull.count",
    )
    success_keys = (
        "value_set",
        "mostly",
        "parse_strings_as_datetimes",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "parse_strings_as_datetimes": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert "value_set" in configuration.kwargs, "value_set is required"
            assert isinstance(
                configuration.kwargs["value_set"], (list, set)
            ), "value_set must be a list or a set"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.in_set",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("value_set",),
        metric_dependencies=tuple(),
        filter_column_isnull=True,
    )
    def _pandas_column_values_in_set(
        self,
        series: pd.Series,
        value_set: Union[list, set],
        runtime_configuration: dict = None,
    ):
        if value_set is None:
            # Vacuously true
            return np.ones(len(series), dtype=np.bool_)
        if pd.api.types.is_datetime64_any_dtype(series):
            parsed_value_set = PandasExecutionEngine.parse_value_set(
                value_set=value_set
            )
        else:
            parsed_value_set = value_set

        return pd.DataFrame({"column_values.in_set": series.isin(parsed_value_set)})

    @SqlAlchemyExecutionEngine.column_map_metric(
        metric_name="column_values.in_set",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("value_set",),
        metric_dependencies=tuple(),
    )
    def _sqlalchemy_in_set(
        self,
        column: sa.column,
        value_set: Union[list, set],
        runtime_configuration: dict = None,
    ):
        if value_set is None:
            # vacuously true
            return True

        return column.in_(tuple(value_set))

    @SparkDFExecutionEngine.column_map_metric(
        metric_name="column_values.in_set",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("value_set",),
        metric_dependencies=tuple(),
    )
    def _spark_in_set(
        self,
        data: "pyspark.sql.DataFrame",
        column: str,
        value_set: Union[list, set],
        runtime_configuration: dict = None,
    ):
        import pyspark.sql.functions as F

        if value_set is None:
            # vacuously true
            return data.withColumn(column + "__success", F.lit(True))

        return data.withColumn(column + "__success", F.col(column).isin(value_set))

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        validation_dependencies = self.get_validation_dependencies(
            configuration, execution_engine
        )["metrics"]
        metric_vals = extract_metrics(validation_dependencies, metrics, configuration)
        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )
        else:
            result_format = self.default_kwarg_values.get("result_format")
        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=(
                metric_vals.get("column_values.in_set.count")
                / metric_vals.get("column_values.nonnull.count")
            )
            >= mostly,
            element_count=metric_vals.get("column_values.count"),
            nonnull_count=metric_vals.get("column_values.nonnull.count"),
            unexpected_count=metric_vals.get("column_values.nonnull.count")
            - metric_vals.get("column_values.in_set.count"),
            unexpected_list=metric_vals.get("column_values.in_set.unexpected_values"),
            unexpected_index_list=metric_vals.get(
                "column_values.in_set.unexpected_index_list"
            ),
        )

    #
    # @renders(StringTemplate, modes=())
    # def lkjdsf(self, mode={prescriptive}, {descriptive}, {valiation}):
    #     return "I'm a thing"
