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


class ExpectColumnValuesToBeUnique(ColumnMapDatasetExpectation):
    """Expect each column value to be unique.

    This expectation detects duplicates. All duplicated values are counted as exceptions.

    For example, `[1, 2, 3, 3, 3]` will return `[3, 3, 3]` in `result.exceptions_list`, with \
    `unexpected_percent = 60.0`.

    expect_column_values_to_be_unique is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
    """

    map_metric = "column_values.are_unique"
    metric_dependencies = (
        "column_values.are_unique.count",
        "column_values.nonnull.count",
    )
    success_keys = ("mostly",)

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "parse_strings_as_datetimes": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        try:
            assert (
                "column" in configuration.kwargs
            ), "'column' parameter is required for column map expectations"
            if "mostly" in configuration.kwargs:
                mostly = configuration.kwargs["mostly"]
                assert isinstance(
                    mostly, (int, float)
                ), "'mostly' parameter must be an integer or float"
                assert 0 <= mostly <= 1, "'mostly' parameter must be between 0 and 1"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.are_unique",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=tuple(),
        metric_dependencies=tuple(),
        filter_column_isnull=True,
    )
    def _pandas_column_values_are_unique(
        self,
        series: pd.Series,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
        filter_column_isnull: bool = True,
    ):
        return pd.DataFrame(
            {"column_values.are_unique": ~series.duplicated(keep=False)}
        )

    # @SqlAlchemyExecutionEngine.column_map_metric(
    #     metric_name="column_values.are_unique",
    #     metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
    #     metric_value_keys=("value_set",),
    #     metric_dependencies=tuple(),
    # )
    # def _sqlalchemy_are_unique(
    #     self,
    #     column: sa.column,
    #     metrics: dict,
    #     metric_domain_kwargs: dict,
    #     metric_value_kwargs: dict,
    #     runtime_configuration: dict = None,
    #     filter_column_isnull: bool = True,

    # ):
    #     value_set = metric_value_kwargs["value_set"]
    #
    #     if value_set is None:
    #         # vacuously true
    #         return True
    #
    #     return column.in_(tuple(value_set))
    #
    # @SparkDFExecutionEngine.column_map_metric(
    #     metric_name="column_values.are_unique",
    #     metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
    #     metric_value_keys=("value_set",),
    #     metric_dependencies=tuple(),
    # )
    # def _spark_are_unique(
    #     self,
    #     data: "pyspark.sql.DataFrame",
    #     column: str,
    #     metrics: dict,
    #     metric_domain_kwargs: dict,
    #     metric_value_kwargs: dict,
    #     runtime_configuration: dict = None,
    #     filter_column_isnull: bool = True,
    # ):
    #     import pyspark.sql.functions as F
    #
    #     value_set = metric_value_kwargs["value_set"]
    #
    #     if value_set is None:
    #         # vacuously true
    #         return data.withColumn(column + "__success", F.lit(True))
    #
    #     return data.withColumn(column + "__success", F.col(column).isin(value_set))

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
                "column_values.are_unique.count"
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
            - metric_vals.get("column_values.are_unique.count"),
            unexpected_list=metric_vals.get(
                "column_values.are_unique.unexpected_values"
            ),
            unexpected_index_list=metric_vals.get(
                "column_values.are_unique.unexpected_index_list"
            ),
        )
