from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine

from ...render.types import RenderedStringTemplateContent
from ...render.util import (
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from ..expectation import (
    AggregateExpectation,
    ColumnMapExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
    _format_map_output,
    renderer,
)
from ..registry import extract_metrics


class ExpectColumnDistinctValuesToBeInSet(AggregateExpectation):
    """Expect the set of distinct column values to be contained by a given set.

            The success value for this expectation will match that of expect_column_values_to_be_in_set. However,
            expect_column_distinct_values_to_be_in_set is a \
            :func:`column_aggregate_expectation \
            <great_expectations.execution_engine.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.

            For example:
            ::

                # my_df.my_col = [1,2,2,3,3,3]
                >>> my_df.expect_column_distinct_values_to_be_in_set(
                    "my_col",
                    [2, 3, 4]
                )
                {
                  "success": false
                  "result": {
                    "observed_value": [1,2,3],
                    "details": {
                      "value_counts": [
                        {
                          "value": 1,
                          "count": 1
                        },
                        {
                          "value": 2,
                          "count": 1
                        },
                        {
                          "value": 3,
                          "count": 1
                        }
                      ]
                    }
                  }
                }

            Args:
                column (str): \
                    The column name.
                value_set (set-like): \
                    A set of objects used for comparison.

            Keyword Args:
                parse_strings_as_datetimes (boolean or None) : If True values provided in value_set will be parsed \
                as datetimes before making comparisons.

            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`. \
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

            See Also:
                :func:`expect_column_distinct_values_to_contain_set \
                <great_expectations.execution_engine.execution_engine.ExecutionEngine
                .expect_column_distinct_values_to_contain_set>`

            """

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.value_counts",)
    success_keys = (
        "value_set",
        "parse_strings_as_datetimes",
    )

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "value_set": None,
        "parse_strings_as_datetimes": None,
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    @classmethod
    @renderer(renderer_name="descriptive")
    def _descriptive_renderer(
        cls, expectation_configuration, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation_configuration.kwargs,
            ["column", "value_set", "row_condition", "condition_parser"],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:

            if include_column_name:
                template_str = "$column distinct values must belong to this set: [ ]"
            else:
                template_str = "distinct values must belong to a set, but that set is not specified."

        else:

            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )

            if include_column_name:
                template_str = (
                    "$column distinct values must belong to this set: "
                    + values_string
                    + "."
                )
            else:
                template_str = (
                    "distinct values must belong to this set: " + values_string + "."
                )

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

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """Validating that user has inputted a value set and that configuration has been initialized"""
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

    def validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates that the Distinct values are a superset of the value set"""
        # Obtaining dependencies used to validate the expectation
        validation_dependencies = self.get_validation_dependencies(
            configuration, execution_engine, runtime_configuration
        )["metrics"]
        metric_vals = extract_metrics(
            validation_dependencies, metrics, configuration, runtime_configuration
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

        parse_strings_as_datetimes = self.get_success_kwargs(configuration).get(
            "parse_strings_as_datetimes"
        )
        observed_value_counts = metric_vals.get("column.value_counts")
        observed_value_set = set(observed_value_counts.index)
        value_set = self.get_success_kwargs(configuration).get("value_set")

        if parse_strings_as_datetimes:
            parsed_value_set = PandasExecutionEngine._parse_value_set(value_set)
        else:
            parsed_value_set = value_set

        expected_value_set = set(parsed_value_set)

        return {
            "success": observed_value_set.issubset(expected_value_set),
            "result": {
                "observed_value": sorted(list(observed_value_set)),
                "details": {"value_counts": observed_value_counts},
            },
        }
