from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    MOSTLY_DESCRIPTION,
)
from great_expectations.render import (
    LegacyDescriptiveRendererType,
    LegacyRendererType,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs

try:
    import sqlalchemy as sa  # noqa: F401, TID251
except ImportError:
    pass

EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the column entries to be strings that do NOT match a given regular expression."
)
REGEX_DESCRIPTION = "The regular expression the column entries should NOT match."
DATA_QUALITY_ISSUES = ["Pattern matching"]
SUPPORTED_DATA_SOURCES = ["Pandas", "Spark", "PostgreSQL", "MySQL", "Redshift"]


from great_expectations._docs_decorators import public_api


@public_api
class ExpectColumnValuesToNotMatchRegex(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    The regex must not match \
    any portion of the provided string. For example, "[at]+" would identify the following strings as expected: \
    "fish", "dog", and the following as unexpected: "cat", "hat".

    ExpectColumnValuesToNotMatchRegex is a \
    Column Map Expectation.

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        regex (str): \
            {REGEX_DESCRIPTION}

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
        [ExpectColumnValuesToNotMatchRegexList](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list)
        [ExpectColumnValuesToMatchLikePattern](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern)
        [ExpectColumnValuesToMatchLikePatternList](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern_list)
        [ExpectColumnValuesToNotMatchLikePattern](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern)
        [ExpectColumnValuesToNotMatchLikePatternList](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern_list)

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
            0 	"aaa"   "bcc"
            1 	"abb"   "bdd"
            2 	"acc"   "abc"

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValuesToNotMatchRegex(
                    column="test2",
                    regex="^a.*",
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
                        "abc",
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
                ExpectColumnValuesToNotMatchRegex(
                    column="test",
                    regex="^a.*",
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

    regex: Union[str, SuiteParameterDict] = pydantic.Field(description=REGEX_DESCRIPTION)

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

    map_metric = "column_values.not_match_regex"
    success_keys = (
        "regex",
        "mostly",
    )
    args_keys = (
        "column",
        "regex",
    )

    class Config:
        title = "Expect column values to not match regex"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnValuesToNotMatchRegex]
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
            ("regex", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.regex:
            template_str = "values must not match a regular expression but none was specified."
        else:
            if renderer_configuration.include_column_name:
                template_str = "$column values must not match this regular expression: $regex"
            else:
                template_str = "values must not match this regular expression: $regex"

            if params.mostly and params.mostly.value < 1.0:
                renderer_configuration = cls._add_mostly_pct_param(
                    renderer_configuration=renderer_configuration
                )
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "regex", "mostly", "row_condition", "condition_parser"],
        )

        if not params.get("regex"):
            template_str = "values must not match a regular expression but none was specified."
        else:  # noqa: PLR5501
            if params["mostly"] is not None and params["mostly"] < 1.0:
                params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")  # noqa: E501
                if include_column_name:
                    template_str = "$column values must not match this regular expression: $regex, at least $mostly_pct % of the time."  # noqa: E501
                else:
                    template_str = "values must not match this regular expression: $regex, at least $mostly_pct % of the time."  # noqa: E501
            else:  # noqa: PLR5501
                if include_column_name:
                    template_str = "$column values must not match this regular expression: $regex."
                else:
                    template_str = "values must not match this regular expression: $regex."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
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
    @renderer(renderer_type=LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_REGEX_COUNT_ROW)
    def _descriptive_column_properties_table_regex_count_row_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        expectation_config = configuration or result.expectation_config
        expectation_kwargs = expectation_config.kwargs
        regex = expectation_kwargs.get("regex")
        unexpected_count = result.result.get("unexpected_count", "--")
        if regex == "^\\s+|\\s+$":
            return ["Leading or trailing whitespace (n)", unexpected_count]
        else:
            return [f"Regex: {regex}", unexpected_count]
