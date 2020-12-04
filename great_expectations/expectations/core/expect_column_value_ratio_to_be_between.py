from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine

from ..expectation import (
    ColumnMapExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    TableExpectation,
    _format_map_output,
)
from ..registry import extract_metrics, get_domain_metrics_dict_by_name


class ExpectColumnValueRatioToBeBetween(TableExpectation):
    """
       Expect the Ratio of a value in a Column to be between a Minimum and Maximum Threshold

               expect_column_values_to_be_of_type is a :func:`dataset_expectation \
               <great_expectations.execution_engine.execution_engine.MetaExecutionEngine.dataset_expectation>` for
               typed-column
               backends,
               and also for PandasExecutionEngine where the column dtype and provided type_ are unambiguous constraints (any
               dtype
               except 'object' or dtype of 'object' with type_ specified as 'object').

               Parameters:
                   column (str): \
                       The column name of a numerical column.
                   value (any type): \
                        A value whose ratio is tested
                   min_value (float or None): \
                        The minimum threshold for the value ratio.
                    max_value (float or None): \
                        The maximum threshold for the value ratio.
                    strict_min (boolean):
                        If True, the value ratio must be strictly larger than min_value, default=False
                    strict_max (boolean):
                        If True, the column value ratio must be strictly smaller than max_value, default=False

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
    metric_dependencies = ("column.value_ratio",)
    success_keys = ("value", "min_value", "strict_min", "max_value", "strict_max")

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "value": None,
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    """ A Column Map MetricProvider Decorator for the Value ratio"""

    def _pandas_value_ratio(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: Dict,
        metric_value_kwargs: Dict,
        metrics: Dict,
        runtime_configuration: dict = None,
    ):
        """Value Ratio MetricProvider Function, extracts nonnull count to use for obtaining the value ratio"""
        # Column Extraction
        series = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )

        domain_metrics_lookup = get_domain_metrics_dict_by_name(
            metrics=metrics, metric_domain_kwargs=metric_domain_kwargs
        )
        nonnull_count = domain_metrics_lookup["column_values.nonnull.unexpected_count"]

        wanted_value = metric_value_kwargs["value"]

        # Checking that the wanted value is indeed in the value set itself
        if wanted_value in series.value_counts():
            value_count = series.value_counts()[wanted_value]
        else:
            value_count = 0

        return value_count / nonnull_count

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

        # Ensuring basic configuration parameters are properly set
        try:
            assert (
                "value" in configuration.kwargs
            ), "A value whose ratio will be computed is required"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        # Validating that Minimum and Maximum values are of the proper format and type
        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]

        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]

        try:
            # Ensuring Proper interval has been provided
            assert (
                min_val is not None or max_val is not None
            ), "min_value and max_value cannot both be None"

            if isinstance(min_val, dict):
                assert (
                    "$PARAMETER" in min_val
                ), 'Evaluation Parameter dict for min_value kwarg must have "$PARAMETER" key.'
            else:
                assert min_val is None or isinstance(
                    min_val, (float, int)
                ), "Provided min threshold must be a number"
                assert min_val is None or 0 <= min_val <= 1, (
                    "The minimum and maximum are ratios and thus must be between"
                    "0 and 1"
                )

            if isinstance(max_val, dict):
                assert (
                    "$PARAMETER" in min_val
                ), 'Evaluation Parameter dict for max_value kwarg must have "$PARAMETER" key.'
            else:
                assert max_val is None or isinstance(
                    max_val, (float, int)
                ), "Provided max threshold must be a number"
                assert max_val is None or 0 <= max_val <= 1, (
                    "The minimum and maximum are ratios and thus must be between"
                    "0 and 1"
                )
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        if min_val is not None and max_val is not None and min_val > max_val:
            raise InvalidExpectationConfigurationError(
                "Minimum Threshold cannot be larger than Maximum Threshold"
            )

        return True

    # @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set minimum and maximum value thresholds for the desired value ratio"""
        # Obtaining dependencies used to validate the expectation
        validation_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
        # Extracting metrics
        metric_vals = extract_metrics(
            validation_dependencies, metrics, configuration, runtime_configuration
        )

        # Runtime configuration has preference
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

        value_ratio = metric_vals.get("column.value_ratio")

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = value_ratio > min_value
            else:
                above_min = value_ratio >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = value_ratio < max_value
            else:
                below_max = value_ratio <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": value_ratio}}
