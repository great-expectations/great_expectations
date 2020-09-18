from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from dateutil.parser import parse

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine

from ...core.batch import Batch
from ...data_asset.util import parse_result_format
from ..expectation import (
    ColumnMapDatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from ..registry import extract_metrics, get_metric_kwargs


class ExpectColumnValuesToBeIncreasing(ColumnMapDatasetExpectation):
    map_metric = "map.increasing"
    metric_dependencies = ("map.increasing.count", "column_values.nonnull.count")
    success_keys = ("strictly", "mostly", "parse_strings_as_datetimes")

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "strictly": None,
        "parse_strings_as_datetimes": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        return super().validate_configuration(configuration)

    @PandasExecutionEngine.column_map_metric(
        metric_name="map.increasing",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("strictly", "parse_strings_as_datetimes",),
        metric_dependencies=tuple(),
    )
    def _pandas_map_increasing(
        self,
        series: pd.Series,
        strictly: Union[bool, None],
        parse_strings_as_datetimes: Union[bool, None],
        runtime_configuration: dict = None,
    ):
        if parse_strings_as_datetimes:
            temp_series = series.map(parse)

            series_diff = temp_series.diff()

            # The first element is null, so it gets a bye and is always treated as True
            series_diff[0] = pd.Timedelta(1)

            if strictly:
                return series_diff > pd.Timedelta(0)
            else:
                return series_diff >= pd.Timedelta(0)

        else:
            series_diff = series.diff()
            # The first element is null, so it gets a bye and is always treated as True
            series_diff[series_diff.isnull()] = 1

            if strictly:
                return series_diff > 0
            else:
                return series_diff >= 0

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        validation_dependencies = self.get_validation_dependencies(configuration)[
            "metrics"
        ]
        metric_vals = extract_metrics(validation_dependencies, metrics, configuration)
        mostly = configuration.get_success_kwargs().get(
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
                metric_vals.get("map.increasing.count")
                / metric_vals.get("column_values.nonnull.count")
            )
            >= mostly,
            element_count=metric_vals.get("column_values.count"),
            nonnull_count=metric_vals.get("column_values.nonnull.count"),
            unexpected_count=metric_vals.get("column_values.nonnull.count")
            - metric_vals.get("column_values.increasing.count"),
            unexpected_list=metric_vals.get(
                "column_values.increasing.unexpected_values"
            ),
            unexpected_index_list=metric_vals.get(
                "column_values.increasing.unexpected_index"
            ),
        )
