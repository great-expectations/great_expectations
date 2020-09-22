from typing import Dict, List, Optional, Union

import pandas as pd
import numpy as np

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine

from ...data_asset.util import parse_result_format
from ..expectation import (
    ColumnMapDatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from ..registry import extract_metrics


class ExpectColumnValuesToBeBetween(ColumnMapDatasetExpectation):

    map_metric = "column_values.is_between"
    metric_dependencies = ("column_values.is_between.count", "column_values.nonnull.count")
    success_keys = ("min_value", "max_value", "strict_min", "strict_max", "allow_cross_type_comparisons","mostly",
                    "parse_strings_as_datetimes", "allow_cross_type_comparisons")

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "min_value": None,
        "max_value": None,
        "strict_min": False,
        "strict_max": False,  # tolerance=1e-9,
        "parse_strings_as_datetimes": None,
        "allow_cross_type_comparisons": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta":None,

    }

    """ A Column Map Metric Decorator for the Mean"""

    @PandasExecutionEngine.column_map_metric(
        metric_name="column_values.is_between",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=("min_value", "max_value", "strict_min", "strict_max"),
        metric_dependencies=(),
    )
    def _pandas_is_between(
            self,
            series: pd.Series,
            min_value = None,
            max_value = None,
            strict_min = None,
            strict_max = None,
            allow_cross_type_comparisons = None,
            runtime_configuration: dict = None,
    ):
        def is_between(self, val, min_value, max_value, strict_min = None, strict_max = None, allow_cross_type_comparisons = None):
            """Given minimum and maximum values, checks if a given number is between 2 thresholds"""
            if min_value is not None and max_value is not None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_min and strict_max:
                            return (min_value < val) and (val < max_value)
                        elif strict_min:
                            return (min_value < val) and (val <= max_value)
                        elif strict_max:
                            return (min_value <= val) and (val < max_value)
                        else:
                            return (min_value <= val) and (val <= max_value)
                    except TypeError:
                        return False

                else:
                    if (isinstance(val, str) != isinstance(min_value, str)) or (
                            isinstance(val, str) != isinstance(max_value, str)
                    ):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_min and strict_max:
                        return (min_value < val) and (val < max_value)
                    elif strict_min:
                        return (min_value < val) and (val <= max_value)
                    elif strict_max:
                        return (min_value <= val) and (val < max_value)
                    else:
                        return (min_value <= val) and (val <= max_value)

            elif min_value is None and max_value is not None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_max:
                            return val < max_value
                        else:
                            return val <= max_value
                    except TypeError:
                        return False

                else:
                    if isinstance(val, str) != isinstance(max_value, str):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_max:
                        return val < max_value
                    else:
                        return val <= max_value

            elif min_value is not None and max_value is None:
                if allow_cross_type_comparisons:
                    try:
                        if strict_min:
                            return min_value < val
                        else:
                            return min_value <= val
                    except TypeError:
                        return False

                else:
                    if isinstance(val, str) != isinstance(min_value, str):
                        raise TypeError(
                            "Column values, min_value, and max_value must either be None or of the same type."
                        )

                    if strict_min:
                        return min_value < val
                    else:
                        return min_value <= val

            else:
                return False
        """Checks whether or not column values are between 2 predefined thresholds"""
        return series.apply(is_between, args=(min_value, max_value, strict_min,
                                                   strict_max, allow_cross_type_comparisons))

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        neccessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        min_val = None
        max_val = None

        # Setting these values if they are available
        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]

        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]

        try:
            # Ensuring Proper interval has been provided
            assert min_val or max_val, "min_value and max_value cannot both be None"
            assert min_val is None or isinstance(min_val, (float, int)), "Provided min threshold must be a number"
            assert max_val is None or isinstance(max_val, (float, int)), "Provided max threshold must be a number"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        if min_val is not None and max_val is not None and min_val > max_val:
            raise InvalidExpectationConfigurationError("Minimum Threshold cannot be larger than Maximum Threshold")

        return True

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
            self,
            configuration: ExpectationConfiguration,
            metrics: dict,
            runtime_configuration: dict = None,
    ):
        """Validates the given data against a minimum and maximum threshold, returning a nested dictionary documenting the
        validation."""

        validation_dependencies = self.get_validation_dependencies(configuration)[
            "metrics"
        ]
        metric_vals = extract_metrics(validation_dependencies, metrics, configuration)

        # Obtaining value for "mostly" and "threshold" arguments to evaluate success
        mostly = configuration.get_success_kwargs().get(
            "mostly", self.default_kwarg_values.get("mostly")
        )

        # If result_format is changed by the runtime configuration
        if runtime_configuration:
            result_format = runtime_configuration.get(
                "result_format", self.default_kwarg_values.get("result_format")
            )
        else:
            result_format = self.default_kwarg_values.get("result_format")

        # Returning dictionary output with necessary metrics based on the format
        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=(metric_vals.get("column_values.is_between.count") / metric_vals.get("column_values.nonull_count"))
                    > mostly,
            element_count=metric_vals.get("column_values.count"),
            nonnull_count=metric_vals.get("column_values.nonnull.count"),
            unexpected_count=metric_vals.get("column_values.is_between.unexpected_count"),
            unexpected_list=metric_vals.get("column_values.is_between.unexpected_values"),
            unexpected_index_list=metric_vals.get("column_values.is_between.unexpected_index"),
        )
