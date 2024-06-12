from __future__ import annotations

from typing import TYPE_CHECKING, List, Literal, Optional, Union

from great_expectations.compatibility import pydantic
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
)
from great_expectations.render.components import (
    LegacyRendererType,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import num_to_str, substitute_none_for_missing

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationConfiguration,
    )
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )


class ExpectColumnValuesToMatchLikePatternList(ColumnMapExpectation):
    """Expect the column entries to be strings that match any of a provided list of like pattern expressions.

    expect_column_values_to_match_like_pattern_list is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            The column name.
        like_pattern_list (List[str]): \
            The list of SQL like pattern expressions the column entries should match.
        match_on (string): \
            "any" or "all". \
            Use "any" if the value should match at least one like pattern in the list. \
            Use "all" if it should match each like pattern in the list.

    Other Parameters:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format and meta.

    See Also:
        [expect_column_values_to_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_match_regex)
        [expect_column_values_to_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_match_regex_list)
        [expect_column_values_to_not_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex)
        [expect_column_values_to_not_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list)
        [expect_column_values_to_match_like_pattern](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern)
        [expect_column_values_to_not_match_like_pattern](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern)
        [expect_column_values_to_not_match_like_pattern_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern_list)

    Supported Datasources:
        [Snowflake](https://docs.greatexpectations.io/docs/application_integration_support/)
        [PostgreSQL](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        Pattern Matching

    Example Data:
                test 	test2
            0 	"aaa"   "ade"
            1 	"abb"   "adb"
            2 	"acc"   "aaa"

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValuesToMatchLikePatternList(
                    column="test",
                    like_pattern_list=["[aa]%", "[ab]%", "[ac]%"],
                    match_on="any"
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 3,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0
                  },
                  "meta": {},
                  "success": true
                }

        Failing Case:
            Input:
                ExpectColumnValuesToMatchLikePatternList(
                    column="test2",
                    like_pattern_list=["[ad]%", "[a]%"],
                    match_on="all"
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 3,
                    "unexpected_count": 1,
                    "unexpected_percent": 33.33333333333333,
                    "partial_unexpected_list": [
                      "aaa",
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 33.33333333333333,
                    "unexpected_percent_nonmissing": 33.33333333333333
                  },
                  "meta": {},
                  "success": false
                }
    """  # noqa: E501

    like_pattern_list: Union[List[str], SuiteParameterDict]
    match_on: Literal["any", "all"] = "any"

    @pydantic.validator("like_pattern_list")
    def validate_like_pattern_list(
        cls, like_pattern_list: list[str] | SuiteParameterDict
    ) -> list[str] | SuiteParameterDict:
        if len(like_pattern_list) < 1:
            raise ValueError("At least one like_pattern must be supplied in the like_pattern_list.")  # noqa: TRY003

        return like_pattern_list

    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_values.match_like_pattern_list"
    success_keys = ("mostly", "like_pattern_list", "match_on")
    args_keys = (
        "column",
        "like_pattern_list",
    )

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> List[RenderedStringTemplateContent]:
        runtime_configuration = runtime_configuration or {}
        _ = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")

        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "like_pattern_list", "mostly"],
        )
        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)

        if not params.get("like_pattern_list") or len(params.get("like_pattern_list")) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["like_pattern_list"]):
                params[f"v__{i!s}"] = v
            values_string = " ".join(
                [f"$v__{i!s}" for i, v in enumerate(params["like_pattern_list"])]
            )

        template_str = "Values must match the following like patterns: " + values_string

        if params["mostly"] is not None and params["mostly"] < 1.0:
            params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

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
