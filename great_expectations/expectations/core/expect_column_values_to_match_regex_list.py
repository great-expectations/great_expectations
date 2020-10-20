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


class ExpectColumnValuesToMatchRegexList(ColumnMapDatasetExpectation):
    """Expect the column entries to be strings that can be matched to either any of or all of a list of regular
    expressions. Matches can be anywhere in the string.

    expect_column_values_to_match_regex_list is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        regex_list (list): \
            The list of regular expressions which the column entries should match

    Keyword Args:
        match_on= (string): \
            "any" or "all".
            Use "any" if the value should match at least one regular expression in the list.
            Use "all" if it should match each regular expression in the list.
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

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

    See Also:
        :func:`expect_column_values_to_match_regex \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine.expect_column_values_to_match_regex>`

        :func:`expect_column_values_to_not_match_regex \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_values_to_not_match_regex>`

    """

    map_metric = "column_values.match_regex_list"
    success_keys = (
        "regex_list",
        "match_on" "mostly",
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
