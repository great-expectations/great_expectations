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


class ExpectColumnValuesToNotMatchRegexList(ColumnMapDatasetExpectation):
    map_metric = "column_values.not_match_regex_list"
    metric_dependencies = (
        "column_values.not_match_regex_list.count",
        "column_values.nonnull.count",
    )
    success_keys = (
        "regex_list",
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
            assert "regex_list" in configuration.kwargs, "regex_list is required"
            assert isinstance(
                configuration.kwargs["regex_list"], list
            ), "regex_list must be a list of regexes"
            if len(configuration.kwargs["regex_list"]) > 0:
                for i in configuration.kwargs["regex_list"]:
                    assert isinstance(i, str), "regexes in list must be strings"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.not_match_regex_list",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("regex_list",),
        metric_dependencies=tuple(),
        filter_column_isnull=False,
    )
    def _pandas_column_values_not_match_regex_list(
        self,
        series: pd.Series,
        metrics: dict,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        runtime_configuration: dict = None,
    ):
        regex_list = metric_value_kwargs["regex_list"]

        regex_matches = []
        for regex in regex_list:
            regex_matches.append(series.astype(str).str.contains(regex))
        regex_match_df = pd.concat(regex_matches, axis=1, ignore_index=True)

        return pd.DataFrame(
            {"column_values.not_match_regex_list": ~regex_match_df.any(axis="columns")}
        )

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
                "column_values.not_match_regex_list.count"
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
            - metric_vals.get("column_values.not_match_regex_list.count"),
            unexpected_list=metric_vals.get(
                "column_values.not_match_regex_list.unexpected_values"
            ),
            unexpected_index_list=metric_vals.get(
                "column_values.not_match_regex_list.unexpected_index_list"
            ),
        )
