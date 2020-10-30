from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)

from ...execution_engine.sqlalchemy_execution_engine import SqlAlchemyExecutionEngine
from ...render.types import RenderedStringTemplateContent
from ...render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

try:
    import sqlalchemy as sa
except ImportError:
    pass


from ...render.renderer.renderer import renderer
from ..expectation import (
    ColumnMapExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    TableExpectation,
    _format_map_output,
)
from ..registry import extract_metrics


class ExpectColumnMaxToBeBetween(TableExpectation):
    """Expect the column max to be between an min and max value

           expect_column_max_to_be_between is a \
           :func:`column_aggregate_expectation
           <great_expectations.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.

           Args:
               column (str): \
                   The column name
               min_value (comparable type or None): \
                   The minimum number of unique values allowed.
               max_value (comparable type or None): \
                   The maximum number of unique values allowed.

           Keyword Args:
               parse_strings_as_datetimes (Boolean or None): \
                   If True, parse min_value, max_values, and all non-null column values to datetimes before making \
                   comparisons.
               output_strftime_format (str or None): \
                   A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.
               strict_min (boolean):
                   If True, the minimal column minimum must be strictly larger than min_value, default=False
               strict_max (boolean):
                   If True, the maximal column minimum must be strictly smaller than max_value, default=False

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

           Notes:
               These fields in the result object are customized for this expectation:
               ::

                   {
                       "observed_value": (list) The actual column max
                   }


               * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
               * If min_value is None, then max_value is treated as an upper bound
               * If max_value is None, then min_value is treated as a lower bound

           """

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.aggregate.max",)
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

    """ A Column Map MetricProvider Decorator for the Maximum"""

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
            ), "min_value and max_value cannot both be none"
            assert min_val is None or isinstance(
                min_val, (float, int)
            ), "Provided min threshold must be a number"
            assert max_val is None or isinstance(
                max_val, (float, int)
            ), "Provided max threshold must be a number"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        if min_val is not None and max_val is not None and min_val > max_val:
            raise InvalidExpectationConfigurationError(
                "Minimum Threshold cannot be larger than Maximum Threshold"
            )

        return True

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = include_column_name if include_column_name is not None else True
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "maximum value may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"maximum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"maximum value must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"maximum value must be {at_least_str} $min_value."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    @renderer(renderer_type="renderer.descriptive.stats_table.max_row")
    def _descriptive_stats_table_max_row_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        assert result, "Must pass in result."
        return [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Maximum",
                    "tooltip": {"content": "expect_column_max_to_be_between"},
                },
            },
            "{:.2f}".format(result.result["observed_value"])
        ]

    # @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates the given data against the set minimum and maximum value thresholds for the column max"""
        column_max = metrics.get("column.aggregate.max")

        # Obtaining components needed for validation
        min_value = self.get_success_kwargs(configuration).get("min_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        # Checking if mean lies between thresholds
        if min_value is not None:
            if strict_min:
                above_min = column_max > min_value
            else:
                above_min = column_max >= min_value
        else:
            above_min = True

        if max_value is not None:
            if strict_max:
                below_max = column_max < max_value
            else:
                below_max = column_max <= max_value
        else:
            below_max = True

        success = above_min and below_max

        return {"success": success, "result": {"observed_value": column_max}}
