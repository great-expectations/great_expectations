
from typing import Dict, List, Optional, Union

import pandas as pd
import numpy as np

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import PandasExecutionEngine

from ...data_asset.util import parse_result_format
from ..expectation import (
    ColumnMapDatasetExpectation,
    DatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
)
from ..registry import extract_metrics


class ExpectColumnMeanToBeBetween(DatasetExpectation):
    """
    Expect the Z-scores of a columns values to be less than a given threshold

            expect_column_values_to_be_of_type is a :func:`column_map_expectation \
            <great_expectations.execution_engine.execution_engine.MetaExecutionEngine.column_map_expectation>` for
            typed-column
            backends,
            and also for PandasExecutionEngine where the column dtype and provided type_ are unambiguous constraints (any
            dtype
            except 'object' or dtype of 'object' with type_ specified as 'object').

            Parameters:
                column (str): \
                    The column name of a numerical column.
                threshold (number): \
                    A maximum Z-score threshold. All column Z-scores that are lower than this threshold will evaluate
                    successfully.


            Keyword Args:
                mostly (None or a float between 0 and 1): \
                    Return `"success": True` if at least mostly fraction of values match the expectation. \
                    For more detail, see :ref:`mostly`.
                double_sided (boolean): \
                    A True of False value indicating whether to evaluate double sidedly.
                    Example:
                    double_sided = True, threshold = 2 -> Z scores in non-inclusive interval(-2,2)
                    double_sided = False, threshold = 2 -> Z scores in non-inclusive interval (-infinity,2)


            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                    For more detail, see :ref:`result_format <result_format>`.
                include_config (boolean): \
                    If True, then include the Expectation config as part of the result object. \
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
    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = tuple("mean",)
    success_keys = ("min_value", "strict_min", "max_value", "strict_max")


    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    """ A Column Map Metric Decorator for the Mean"""
    @PandasExecutionEngine.metric(
        metric_name="mean",
        metric_domain_keys=ColumnMapDatasetExpectation.domain_keys,
        metric_value_keys=(),
        metric_dependencies=tuple(),
    )
    def _pandas_mean(
            self,
            series: pd.Series,
            runtime_configuration: dict = None,
    ):
        """Mean Metric Function"""

        return series.mean()

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
        min_val = None
        max_val = None

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # Ensuring basic configuration parameters are properly set
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

        # Validating that Minimum and Maximum values are of the proper format and type
        if "min_value" in configuration:
            min_val = configuration.kwargs["min_value"]

        if "max_value" in configuration:
            max_val = configuration.kwargs["max_value"]

        try:
            # Ensuring Proper interval has been provided
            assert "min_value" in configuration or "max_value" in configuration, "min_value and max_value " \
                                                                                 "cannot both be None"
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
        """Validates the given data against the set Z Score threshold, returning a nested dictionary documenting the
        validation."""
        # Obtaining dependencies used to validate the expectation
        validation_dependencies = self.get_validation_dependencies(configuration)[
            "metrics"
        ]
        metric_vals = extract_metrics(validation_dependencies, metrics, configuration)
        column_mean = metric_vals.get("standard_deviation")

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_mean > min_value
            else:
                above_min = column_mean >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_mean < max_value
            else:
                below_max = column_mean <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_mean}}