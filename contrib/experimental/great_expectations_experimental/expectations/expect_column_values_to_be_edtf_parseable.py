from typing import Optional

from edtf_validate.valid_edtf import (
    conformsLevel0,
    conformsLevel1,
    conformsLevel2,
    is_valid,
)

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


def complies_to_level(value, level=None):
    if level == 0:
        return conformsLevel0(value)
    elif level == 1:
        return conformsLevel1(value)
    elif level == 2:
        return conformsLevel2(value)

    return is_valid(value)


class ColumnValuesEdtfParseable(ColumnMapMetricProvider):
    condition_metric_name = "column_values.edtf_parseable"

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, level=None, **kwargs):
        def is_parseable(val):
            try:
                if type(val) != str:
                    raise TypeError(
                        "Values passed to expect_column_values_to_be_edtf_parseable must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format."
                    )

                return complies_to_level(val, level)

            except (ValueError, OverflowError):
                return False

        if level is not None and type(level) != int:
            raise TypeError("level must be of type int.")

        return column.map(is_parseable)


## When the correct map_metric was added to ExpectColumnValuesToBeEdtfParseable below
## and tests were run, the tests for spark were failing with
## `ModuleNotFoundError: No module named 'expectations'`, so commenting out for now

#     @column_condition_partial(engine=SparkDFExecutionEngine)
#     def _spark(cls, column, level=None, **kwargs):
#         def is_parseable(val):
#             try:
#                 if type(val) != str:
#                     raise TypeError(
#                         "Values passed to expect_column_values_to_be_edtf_parseable must be of type string.\nIf you want to validate a column of dates or timestamps, please call the expectation before converting from string format."
#                     )
#
#                 return complies_to_level(val, level)
#
#             except (ValueError, OverflowError):
#                 return False
#
#         if level is not None and type(level) != int:
#             raise TypeError("level must be of type int.")
#
#         is_parseable_udf = F.udf(is_parseable, sparktypes.BooleanType())
#         return is_parseable_udf(column)


class ExpectColumnValuesToBeEdtfParseable(ColumnMapExpectation):
    """Expect column entries to be parsable using the [Extended Date/Time Format (EDTF) specification](https://www.loc.gov/standards/datetime/).

    expect_column_values_to_be_edtf_parseable is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        level (int or None): \
            The EDTF level to comply to.

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
                "all_edtf_l0": [
                    "1964/2008",
                    "2004-06/2006-08",
                    "2004-02-01/2005-02-08",
                    "2004-02-01/2005-02",
                    "2004-02-01/2005",
                    "2005/2006-02",
                    "0000/0000",
                    "0000-02/1111",
                    "0000-01/0000-01-03",
                    "0000-01-13/0000-01-23",
                    "1111-01-01/1111",
                    "0000-01/0000",
                ],
            },
            "tests": [
                {
                    "title": "positive_level0_test_exact",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "all_edtf_l0", "level": 0},
                    "out": {
                        "success": True,
                        "unexpected_count": 0,
                    },
                },
            ],
        },
        {
            "data": {
                "all_edtf_l1": [
                    "-1000/-0999",
                    "-2004-02-01/2005",
                    "-1980-11-01/1989-11-30",
                    "1923-21/1924",
                    "2019-12/2020%",
                    "1984~/2004-06",
                    "1984/2004-06~",
                    "1984~/2004~",
                    "-1984?/2004%",
                    "1984?/2004-06~",
                    "1984-06?/2004-08?",
                    "1984-06-02?/2004-08-08~",
                    "2004-06~/2004-06-11%",
                    "1984-06-02?/",
                    "2003/2004-06-11%",
                    "1952-23~/1953",
                    "-2004-06-01/",
                    "1985-04-12/",
                    "1985-04/",
                    "1985/",
                    "/1985-04-12",
                    "/1985-04",
                    "/1985",
                    "2003-22/2004-22",
                    "2003-22/2003-22",
                    "2003-22/2003-23",
                    "1985-04-12/..",
                    "1985-04/..",
                    "1985/..",
                    "../1985-04-12",
                    "../1985-04",
                    "../1985",
                    "/..",
                    "../",
                    "../..",
                    "-1985-04-12/..",
                    "-1985-04/..",
                    "-1985/",
                ]
            },
            "tests": [
                {
                    "title": "positive_level1_test_exact",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "all_edtf_l1", "level": 1},
                    "out": {
                        "success": True,
                        "unexpected_count": 0,
                    },
                },
            ],
        },
        {
            "data": {
                "all_edtf_l2": [
                    "2004-06-~01/2004-06-~20",
                    "-2004-06-?01/2006-06-~20",
                    "-2005-06-%01/2006-06-~20",
                    "2019-12/%2020",
                    "1984?-06/2004-08?",
                    "-1984-?06-02/2004-08-08~",
                    "1984-?06-02/2004-06-11%",
                    "2019-~12/2020",
                    "2003-06-11%/2004-%06",
                    "2004-06~/2004-06-%11",
                    "1984?/2004~-06",
                    "?2004-06~-10/2004-06-%11",
                    "2004-06-XX/2004-07-03",
                    "2003-06-25/2004-X1-03",
                    "20X3-06-25/2004-X1-03",
                    "XXXX-12-21/1890-09-2X",
                    "1984-11-2X/1999-01-01",
                    "1984-11-12/1984-11-XX",
                    "198X-11-XX/198X-11-30",
                    "2000-12-XX/2012",
                    "-2000-12-XX/2012",
                    "2000-XX/2012",
                    "2000-XX-XX/2012",
                    "2000-XX-XX/2012",
                    "-2000-XX-10/2012",
                    "2000/2000-XX-XX",
                    "198X/199X",
                    "198X/1999",
                    "1987/199X",
                    "1919-XX-02/1919-XX-01",
                    "1919-0X-02/1919-01-03",
                    "1865-X2-02/1865-03-01",
                    "1930-X0-10/1930-10-30",
                    "1981-1X-10/1981-11-09",
                    "1919-12-02/1919-XX-04",
                    "1919-11-02/1919-1X-01",
                    "1919-09-02/1919-X0-01",
                    "1919-08-02/1919-0X-01",
                    "1919-10-02/1919-X1-01",
                    "1919-04-01/1919-X4-02",
                    "1602-10-0X/1602-10-02",
                    "2018-05-X0/2018-05-11",
                    "-2018-05-X0/2018-05-11",
                    "1200-01-X4/1200-01-08",
                    "1919-07-30/1919-07-3X",
                    "1908-05-02/1908-05-0X",
                    "0501-11-18/0501-11-1X",
                    "1112-08-22/1112-08-2X",
                    "2015-02-27/2015-02-X8",
                    "2016-02-28/2016-02-X9",
                    "1984-06-?02/2004-06-11%",
                ]
            },
            "tests": [
                {
                    "title": "positive_level2_test_exact",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "all_edtf_l2", "level": 2},
                    "out": {
                        "success": True,
                        "unexpected_count": 0,
                    },
                },
            ],
        },
        {
            "data": {
                "invalid_edtf_dates": [
                    "1863- 03-29",
                    " 1863-03-29",
                    "1863-03 -29",
                    "1863-03- 29",
                    "1863-03-29 ",
                    "18 63-03-29",
                    "1863-0 3-29",
                    "1960-06-31",
                    "20067890%",
                    "Y2006",
                    "-0000",
                    "Y20067890-14-10%",
                    "20067890%",
                    "+2006%",
                    "NONE/",
                    "2000/12-12",
                    "2012-10-10T1:10:10",
                    "2012-10-10T10:1:10",
                    "2005-07-25T10:10:10Z/2006-01-01T10:10:10Z",
                    "[1 760-01, 1760-02, 1760-12..]",
                    "[1667,1668, 1670..1672]",
                    "[..176 0-12-03]",
                    "{-1667,1668, 1670..1672}",
                    "{-1667,1 668-10,1670..1672}",
                    "2001-21^southernHemisphere",
                    "",
                ]
            },
            "tests": [
                {
                    "title": "negative_test_exact",
                    "include_in_gallery": True,
                    "exact_match_out": False,
                    "in": {"column": "invalid_edtf_dates"},
                    "out": {
                        "success": False,
                        "unexpected_count": 25,
                    },
                },
            ],
        },
        {
            "data": {
                "mostly_edtf": [
                    "1979-08",  # ISO8601 Date
                    "2004-01-01T10:10:10+05:00",  # ISO8601 Datetime
                    "1979-08-28/1979-09-25",  # Interval (start/end)
                    "1979-08~",  # Uncertain/Approximate dates
                    "1979-08-XX",  # Unspecified dates
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
                        "unexpected_count": 2,
                    },
                }
            ],
        },
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["edtf", "datetime", "glam"],
        "contributors": ["@mielvds"],
        "requirements": ["edtf_validate"],
    }

    map_metric = "column_values.edtf_parseable"
    success_keys = ("mostly",)

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        language: Optional[str] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
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
    ExpectColumnValuesToBeEdtfParseable().print_diagnostic_checklist()
