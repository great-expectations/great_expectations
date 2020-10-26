from itertools import zip_longest
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine

from ...render.renderer.renderer import renderer
from ...render.types import RenderedStringTemplateContent
from ...render.util import substitute_none_for_missing
from ..expectation import (
    DatasetExpectation,
    Expectation,
    InvalidExpectationConfigurationError,
)
from ..registry import extract_metrics


class ExpectTableColumnsToMatchOrderedList(DatasetExpectation):
    """Expect the columns to exactly match a specified list.

    expect_table_columns_to_match_ordered_list is a :func:`expectation \
    <great_expectations.validator.validator.Validator.expectation>`, not a
    ``column_map_expectation`` or ``column_aggregate_expectation``.

    Args:
        column_list (list of str): \
            The column names, in the correct order.

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

    metric_dependencies = ("columns",)
    success_keys = ("ordered_list",)
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "ordered_list": None,
        "result_format": "BASIC",
        "column": None,
        "column_index": None,
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }

    """ A Metric Decorator for the table columns"""

    # @PandasExecutionEngine.metric(
    #        metric_name="columns",
    #        metric_domain_keys=("batch_id", "table", "row_condition", "condition_parser"),
    #        metric_value_keys=(),
    #        metric_dependencies=(),
    #        filter_column_isnull=False,
    #    )
    def _pandas_columns(
        self,
        batches: Dict[str, Batch],
        execution_engine: PandasExecutionEngine,
        metric_domain_kwargs: dict,
        metric_value_kwargs: dict,
        metrics: dict,
        runtime_configuration: dict = None,
    ):
        """Metric which returns all columns in a DataFrame"""
        df = execution_engine.get_domain_dataframe(
            domain_kwargs=metric_domain_kwargs, batches=batches
        )

        cols = df.columns
        return cols.tolist()

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

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

        # Ensuring that a proper value has been provided
        try:
            assert "ordered_list" in configuration.kwargs, "ordered_list is required"
            assert isinstance(
                configuration.kwargs["ordered_list"], (list, set)
            ), "ordered_list must be a list or a set"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    @classmethod
    @renderer(renderer_type="descriptive")
    def _descriptive_renderer(
        cls, expectation_configuration, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation_configuration.kwargs, ["column_list"]
        )

        if params["column_list"] is None:
            template_str = "Must have a list of columns in a specific order, but that order is not specified."

        else:
            template_str = "Must have these columns in this order: "
            for idx in range(len(params["column_list"]) - 1):
                template_str += "$column_list_" + str(idx) + ", "
                params["column_list_" + str(idx)] = params["column_list"][idx]

            last_idx = len(params["column_list"]) - 1
            template_str += "$column_list_" + str(last_idx)
            params["column_list_" + str(last_idx)] = params["column_list"][last_idx]

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

    # @Expectation.validates(metric_dependencies=metric_dependencies)
    def _validates(
        self,
        configuration: ExpectationConfiguration,
        metrics: dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        """Validates given column count against expected value"""
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

        # Obtaining columns and ordered list for sake of comparison
        columns = metric_vals.get("columns")
        column_list = self.get_success_kwargs(configuration).get("ordered_list")

        if column_list is None or list(columns) == list(column_list):
            return {"success": True, "result": {"observed_value": list(columns)}}
        else:
            # In the case of differing column lengths between the defined expectation and the observed column set, the
            # max is determined to generate the column_index.
            number_of_columns = max(len(column_list), len(columns))
            column_index = range(number_of_columns)

            # Create a list of the mismatched details
            compared_lists = list(
                zip_longest(column_index, list(column_list), list(columns))
            )
            mismatched = [
                {"Expected Column Position": i, "Expected": k, "Found": v}
                for i, k, v in compared_lists
                if k != v
            ]
            return {
                "success": False,
                "result": {
                    "observed_value": list(columns),
                    "details": {"mismatched": mismatched},
                },
            }
