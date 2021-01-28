from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from dateutil.parser import parse

from great_expectations.core.batch import Batch
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.util import render_evaluation_parameter_string

from ...render.renderer.renderer import renderer
from ...render.types import RenderedStringTemplateContent
from ...render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from ..expectation import ColumnPairMapExpectation, InvalidExpectationConfigurationError

try:
    import sqlalchemy as sa
except ImportError:
    pass


class ExpectColumnPairValuesToBeInSet(ColumnPairMapExpectation):
    """
    Expect paired values from columns A and B to belong to a set of valid pairs.

    Args:
        column_A (str): The first column name
        column_B (str): The second column name
        value_pairs_set (list of tuples): All the valid pairs to be matched

    Keyword Args:
        ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "never"

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

    map_metric = ("column_pair_values.in_set",)
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
        "column_A",
        "column_B",
    )
    success_keys = ("value_pairs_set", "ignore_row_if", "mostly")

    default_kwarg_values = {
        "mostly": 1,
        "ignore_row_if": "both_values_are_missing",
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration
        try:
            assert (
                "column_A" in configuration.kwargs
                and "column_B" in configuration.kwargs
            ), "both columns must be provided"
            assert (
                "value_pairs_set" in configuration.kwargs
            ), "must provide value_pairs_set"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))
        return True

    # TODO: fill out prescriptive renderer
    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_A",
                "column_B",
                "parse_strings_as_datetimes",
                "ignore_row_if",
                "mostly",
                "or_equal",
                "row_condition",
                "condition_parser",
            ],
        )

    # def _validate(
    #     self,
    #     configuration: ExpectationConfiguration,
    #     metrics: Dict,
    #     runtime_configuration: dict = None,
    #     execution_engine: ExecutionEngine = None,
    # ):
    #     equal_columns = metrics.get("column_pair_values.in_set")
    #     return {"success": equal_columns.all()}
