from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    MOSTLY_DESCRIPTION,
)
from great_expectations.render.components import (
    LegacyRendererType,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import num_to_str, substitute_none_for_missing

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationConfiguration,
    )
    from great_expectations.core.expectation_validation_result import (
        ExpectationValidationResult,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs

EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the column entries to be strings that do NOT match "
    "any of a provided list of like pattern expressions."
)
LIKE_PATTERN_LIST_DESCRIPTION = (
    "The list of SQL like pattern expressions the column entries should NOT match."
)
DATA_QUALITY_ISSUES = ["Pattern matching"]
SUPPORTED_DATA_SOURCES = ["SQLite", "PostgreSQL", "MySQL", "MSSQL", "Redshift"]


class ExpectColumnValuesToNotMatchLikePatternList(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnValuesToNotMatchLikePatternList is a \
    Column Map Expectation.

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        like_pattern_list (List[str]): \
            {LIKE_PATTERN_LIST_DESCRIPTION}

    Other Parameters:
        mostly (None or a float between 0 and 1): \
            {MOSTLY_DESCRIPTION} \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    See Also:
        [ExpectColumnValuesToMatchRegex](https://greatexpectations.io/expectations/expect_column_values_to_match_regex)
        [ExpectColumnValuesToMatchRegexList](https://greatexpectations.io/expectations/expect_column_values_to_match_regex_list)
        [ExpectColumnValuesToNotMatchRegex](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex)
        [ExpectColumnValuesToNotMatchRegexList](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list)
        [ExpectColumnValuesToMatchLikePattern](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern)
        [ExpectColumnValuesToMatchLikePatternList](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern_list)
        [ExpectColumnValuesToNotMatchLikePattern](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern)

    Supported Datasources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                test 	test2
            0 	"aaa"   "ade"
            1 	"abb"   "adb"
            2 	"acc"   "aaa"

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValuesToNotMatchLikePatternList(
                    column="test2",
                    like_pattern_list=["[aa]%", "[ab]%", "[ac]%"],
                    mostly=.66
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
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
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnValuesToNotMatchLikePatternList(
                    column="test",
                    like_pattern_list=["[aa]%", "[ab]%", "[ac]%"],
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 3,
                    "unexpected_count": 3,
                    "unexpected_percent": 100,
                    "partial_unexpected_list": [
                      "aaa",
                      "abb",
                      "acc",
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 100,
                    "unexpected_percent_nonmissing": 100
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    like_pattern_list: Union[List[str], SuiteParameterDict] = pydantic.Field(
        description=LIKE_PATTERN_LIST_DESCRIPTION
    )

    @pydantic.validator("like_pattern_list")
    def validate_like_pattern_list(
        cls, like_pattern_list: list[str] | SuiteParameterDict
    ) -> list[str] | SuiteParameterDict:
        if len(like_pattern_list) < 1:
            raise ValueError("At least one like_pattern must be supplied in the like_pattern_list.")  # noqa: TRY003

        return like_pattern_list

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    map_metric = "column_values.not_match_like_pattern_list"
    success_keys = (
        "like_pattern_list",
        "mostly",
    )
    args_keys = (
        "column",
        "like_pattern_list",
    )

    class Config:
        title = "Expect column values to not match like pattern list"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnValuesToNotMatchLikePatternList]
        ) -> None:
            ColumnMapExpectation.Config.schema_extra(schema, model)
            schema["properties"]["metadata"]["properties"].update(
                {
                    "data_quality_issues": {
                        "title": "Data Quality Issues",
                        "type": "array",
                        "const": DATA_QUALITY_ISSUES,
                    },
                    "library_metadata": {
                        "title": "Library Metadata",
                        "type": "object",
                        "const": model._library_metadata,
                    },
                    "short_description": {
                        "title": "Short Description",
                        "type": "string",
                        "const": EXPECTATION_SHORT_DESCRIPTION,
                    },
                    "supported_data_sources": {
                        "title": "Supported Data Sources",
                        "type": "array",
                        "const": SUPPORTED_DATA_SOURCES,
                    },
                }
            )

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("like_pattern_list", RendererValueType.ARRAY),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if renderer_configuration.include_column_name:
            template_str = "$column values must not match "
        else:
            template_str = "Values must not match "

        if params.like_pattern_list:
            array_param_name = "like_pattern_list"
            param_prefix = "like_pattern_list_"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            template_str += "the following like patterns: " + cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

        if params.mostly and params.mostly.value < 1.0:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str += " , at least $mostly_pct % of the time."

        renderer_configuration.template_str = template_str

        return renderer_configuration

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

        template_str = "Values must not match the following like patterns: " + values_string

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
