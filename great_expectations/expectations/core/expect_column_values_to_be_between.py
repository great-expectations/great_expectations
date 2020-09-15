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

    map_metric = "map.is_between"
    metric_dependencies = ("map.is_between.count", "map.nonnull.count")
    success_keys = ("min_value", "max_value", "strict_min", "strict_max", "allow_cross_type_comparisons","mostly",
                    "parse_strings_as_datetimes")

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "min_value": None,
        "max_value": None,
        "strict_min": False,
        "strict_max": False,  # tolerance=1e-9,
        "parse_strings_as_datetimes": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta":None,
    }

    """ A Column Map Metric Decorator for the Mean"""

    @PandasExecutionEngine.column_map_metric(
        metric_name="map.is_between",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=(),
        metric_dependencies=("min_value", "max_value"),
    )
    def _pandas_is_between(
            self,
            series: pd.Series,
            runtime_configuration: dict = None,
    ):
        """Checks whether or not column values are between 2 predefined thresholds"""

        pass

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
        super().validate_configurations(configuration)
        if configuration is None:
            configuration = self.configuration

        min = None
        max = None

        # Setting these values if they are available
        if "min_value" in configuration:
            min = configuration.kwargs["min_value"]

        if "max_value" in configuration:
            max =  configuration.kwargs["max_value"]


        try:
            # Ensuring Proper interval has been provided
            assert "min_value" in configuration or "max_value" in configuration, "min_value and max_value cannot both be None"
            assert min is None or isinstance(min, (float, int)), "Provided min threshold must be a number"
            assert max is None or isinstance(max, (float, int)), "Provided max threshold must be a number"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        if min is not None and max is not None and min > max:
            raise InvalidExpectationConfigurationError("Minimum Threshold cannot be larger than Maximum Threshold")

        return True

    def get_validation_dependencies(self, configuration: Optional[ExpectationConfiguration] = None):
        """
        Obtains and returns neccessary validation metric dependencies, based on the result format indicated by the
        user or the default result format/

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            metric_dependencies (dict): \
                A dictionary of all metrics neccessary for the validation format beyond computational defaults
        """
        # Building a dictionary of dependencies
        dependencies = super().get_validation_dependencies(configuration)
        metric_dependencies = set(self.metric_dependencies)
        dependencies["metrics"] = metric_dependencies
        result_format_str = dependencies["result_format"].get("result_format")
        if result_format_str == "BOOLEAN ONLY":
            return dependencies

        # Count and unexpected values needed for basic/summary modes
        metric_dependencies.add("map.count")
        metric_dependencies.add("map.in_set.unexpected_values")
        if result_format_str in ["BASIC", "SUMMARY"]:
            return dependencies

        # Complete mode requires unexpected rows
        metric_dependencies.add("map.in_set.unexpected_rows")
        return metric_dependencies

    @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
            self,
            configuration: ExpectationConfiguration,
            metrics: dict,
            runtime_configuration: dict = None,
    ):
        """Validates the given data against the set Z Score threshold, returning a nested dictionary documenting the
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
            success= (metric_vals.get("map.z_scores.number_over_threshold") / metric_vals.get("map.nonull_count"))
                    > mostly,
            element_count=metric_vals.get("map.count"),
            nonnull_count=metric_vals.get("map.nonnull.count"),
            unexpected_count=metric_vals.get("map.nonnull.count")
                             - "map.z_scores.number_over_threshold",
            unexpected_list=metric_vals.get("map.in_set.unexpected_values"),
            unexpected_index_list=metric_vals.get("map.is_in.unexpected_index"),
        )

















# --------------------------------Metric computations defined below----------------------

















    def expect_column_values_to_be_between(
        self,
        column,
        min_value=None,
        max_value=None,
        strict_min=False,
        strict_max=False,  # tolerance=1e-9,
        parse_strings_as_datetimes=None,
        output_strftime_format=None,
        allow_cross_type_comparisons=None,
        mostly=None,
        row_condition=None,
        condition_parser=None,
        result_format=None,
        include_config=True,
        catch_exceptions=None,
        meta=None,
    ):


        # if strict_min and min_value:
        #     min_value += tolerance
        #
        # if strict_max and max_value:
        #     max_value -= tolerance

        if parse_strings_as_datetimes:
            # tolerance = timedelta(days=tolerance)
            if min_value:
                min_value = parse(min_value)

            if max_value:
                max_value = parse(max_value)

            try:
                temp_column = column.map(parse)
            except TypeError:
                temp_column = column

        else:
            temp_column = column


        def is_between(val):
            # TODO Might be worth explicitly defining comparisons between types (for example, between strings and ints).
            # Ensure types can be compared since some types in Python 3 cannot be logically compared.
            # print type(val), type(min_value), type(max_value), val, min_value, max_value

            if type(val) is None:
                return False

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

        return temp_column.map(is_between)
