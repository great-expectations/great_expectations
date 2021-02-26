import json
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from edtf import parse_edtf

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration,
)
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ColumnValuesEdtfParseable(ColumnMapMetricProvider):
    condition_metric_name = "column_values.edtf_parseable"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def is_parseable(val):
            try:
                if type(val) != str:
                    raise TypeError(
                        "Values passed to expect_column_values_to_be_edtf_parseable must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format."
                    )

                parse_edtf(val)
                return True

            except (ValueError, OverflowError):
                return False

        return column.map(is_parseable)


class ExpectColumnValuesToBeEdtfParseable(ColumnMapExpectation):
    """Expect column entries to be parsable using the [Extended Date/Time Format (EDTF) specification](https://www.loc.gov/standards/datetime/).

    expect_column_values_to_be_edtf_parseable is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.

    Keyword Args:
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

    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "mostly_edtf": [
                    "1979-08",  # ISO8601 Date
                    "2004-01-01T10:10:10+05:00",  # ISO8601 Datetime
                    "1979-08-28/1979-09-25",  # Interval (start/end)
                    "1979-08~",  # Uncertain/Approximate dates
                    "1979-08-uu",  # Unspecified dates
                    "1984-06-02?/2004-08-08~",  # Extended intervals
                    "y-12000",  # Years exceeding four digits
                    "asdwefefef",
                    "Octobre 12",
                    None,
                    None,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "mostly_edtf", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                }
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["edtf", "datetime", "glam"],
        "contributors": ["@mielvds"],
        "package": "experimental_expectations",
        "requirements": ["edtf"],
    }

    map_metric = "column_values.edtf_parsable"
    success_keys = ("mostly",)

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
        return True

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
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        template_str = "values must be parseable by edtf"

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

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


if __name__ == "__main__":
    diagnostics_report = ExpectColumnValuesToBeEdtfParseable().run_diagnostics()
    print(json.dumps(diagnostics_report, indent=2))
