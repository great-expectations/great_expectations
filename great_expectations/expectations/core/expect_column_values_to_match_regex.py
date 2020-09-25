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


class ExpectColumnValuesToMatchRegex(ColumnMapDatasetExpectation):
    map_metric = "column_values.match_regex"
    metric_dependencies = (
        "column_values.match_regex.count",
        "column_values.nonnull.count",
    )
    success_keys = (
        "regex",
        "mostly",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert "regex" in configuration.kwargs, "regex is required"
            assert isinstance(
                configuration.kwargs["regex"], str
            ), "regex must be a string"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.match_regex",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("regex",),
        metric_dependencies=tuple(),
        filter_column_isnull=True,
    )
    def _pandas_column_values_match_regex(
        self, series: pd.Series, regex: str, runtime_configuration: dict = None,
    ):
        return pd.DataFrame(
            {"column_values.match_regex": series.astype(str).str.contains(regex)}
        )

    # @SqlAlchemyExecutionEngine.column_map_metric(
    #     metric_name="column_values.match_regex",
    #     metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
    #     metric_value_keys=("regex",),
    #     metric_dependencies=tuple(),
    # )
    # def _sqlalchemy_match_regex(
    #     self,
    #     column: sa.column,
    #     regex: str,
    #     runtime_configuration: dict = None,
    # ):
    #     regex_expression = execution_engine._get_dialect_regex_expression(column, regex)
    #     if regex_expression is None:
    #         logger.warning(
    #             "Regex is not supported for dialect %s" % str(self.sql_engine_dialect)
    #         )
    #         raise NotImplementedError
    #
    #     return regex_expression
    #     if regex is None:
    #         # vacuously true
    #         return True
    #
    #     return column.in_(tuple(regex))
    #
    # @SparkDFExecutionEngine.column_map_metric(
    #     metric_name="column_values.match_regex",
    #     metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
    #     metric_value_keys=("regex",),
    #     metric_dependencies=tuple(),
    # )
    # def _spark_match_regex(
    #     self,
    #     data: "pyspark.sql.DataFrame",
    #     column: str,
    #     regex: str,
    #     runtime_configuration: dict = None,
    # ):
    #     import pyspark.sql.functions as F
    #
    #     if regex is None:
    #         # vacuously true
    #         return data.withColumn(column + "__success", F.lit(True))
    #
    #     return data.withColumn(column + "__success", F.col(column).isin(regex))

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        metric_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
        metric_vals = extract_metrics(
            metric_dependencies, metrics, configuration, runtime_configuration
        )
        mostly = self.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format",
                configuration.kwargs.get(
                    "result_format", self.default_kwarg_values.get("result_format")
                ),
            )
        else:
            result_format = configuration.kwargs.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )

        if metric_vals.get("column_values.nonnull.count") > 0:
            success = metric_vals.get(
                "column_values.match_regex.count"
            ) / metric_vals.get("column_values.nonnull.count")
        else:
            # TODO: Setting this to 1 based on the notion that tests on empty columns should be vacuously true. Confirm.
            success = 1
        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success >= mostly,
            element_count=metric_vals.get("column_values.count"),
            nonnull_count=metric_vals.get("column_values.nonnull.count"),
            unexpected_count=metric_vals.get("column_values.nonnull.count")
            - metric_vals.get("column_values.match_regex.count"),
            unexpected_list=metric_vals.get(
                "column_values.match_regex.unexpected_values"
            ),
            unexpected_index_list=metric_vals.get(
                "column_values.match_regex.unexpected_index_list"
            ),
        )
