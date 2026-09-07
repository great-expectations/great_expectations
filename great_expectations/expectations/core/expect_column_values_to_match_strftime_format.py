from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TC001 # FIXME CoP
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    _style_row_condition,
    render_suite_parameter_string,
)
from great_expectations.expectations.metadata_types import DataQualityIssues, SupportedDataSources
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    FAILURE_SEVERITY_DESCRIPTION,
    MOSTLY_DESCRIPTION,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string,
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

EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the column entries to be strings representing a date or time with a given format."
)
STRFTIME_FORMAT_DESCRIPTION = "A strftime format string to use for matching."
DATA_QUALITY_ISSUES = [DataQualityIssues.VALIDITY.value]
SUPPORTED_DATA_SOURCES = [
    SupportedDataSources.PANDAS.value,
    SupportedDataSources.SPARK.value,
]


class ExpectColumnValuesToMatchStrftimeFormat(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnValuesToMatchStrftimeFormat is a \
    Column Map Expectation.

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.
    SQL data sources are not currently supported: strftime format tokens do not map cleanly onto the date-format models of SQL dialects.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        strftime_format (str or SuiteParameterDict): \
            {STRFTIME_FORMAT_DESCRIPTION}

    Other Parameters:
        mostly (None or a float between 0 and 1): \
            {MOSTLY_DESCRIPTION} \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).
        severity (str or None): \
            {FAILURE_SEVERITY_DESCRIPTION} \
            For more detail, see [failure severity](https://docs.greatexpectations.io/docs/cloud/expectations/expectations_overview/#failure-severity).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    Supported Data Sources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Issues:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                event_date      invalid_date
            0   "2024-01-15"    "01/15/2024"
            1   "2024-06-20"    "06/20/2024"
            2   "2024-12-31"    "12/31/2024"

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValuesToMatchStrftimeFormat(
                    column="event_date",
                    strftime_format="%Y-%m-%d",
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
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnValuesToMatchStrftimeFormat(
                    column="invalid_date",
                    strftime_format="%Y-%m-%d",
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
                    "unexpected_percent": 100.0,
                    "partial_unexpected_list": [
                      "01/15/2024",
                      "06/20/2024",
                      "12/31/2024"
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 100.0,
                    "unexpected_percent_nonmissing": 100.0
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501 # FIXME CoP

    strftime_format: Union[str, SuiteParameterDict] = pydantic.Field(
        description=STRFTIME_FORMAT_DESCRIPTION
    )

    @pydantic.validator("strftime_format")
    def validate_strftime_format(
        cls, strftime_format: str | SuiteParameterDict
    ) -> str | SuiteParameterDict:
        if isinstance(strftime_format, str):
            try:
                datetime.strptime(  # noqa: DTZ007 # FIXME CoP
                    datetime.strftime(datetime.now(tz=timezone.utc), strftime_format),  # FIXME CoP
                    strftime_format,
                )
            except ValueError as e:
                raise ValueError(f"Unable to use provided strftime_format. {e!s}") from e  # noqa: TRY003 # FIXME CoP

        return strftime_format

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

    map_metric = "column_values.match_strftime_format"
    success_keys = (
        "strftime_format",
        "mostly",
    )
    args_keys = (
        "column",
        "strftime_format",
    )

    class Config:
        title = "Expect column values to match strftime format"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnValuesToMatchStrftimeFormat]
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
    ):
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("strftime_format", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.strftime_format:
            template_str = "values must match a strftime format but none was specified."
        else:
            template_str = "values must match the following strftime format: $strftime_format"
            if params.mostly and params.mostly.value < 1.0:
                renderer_configuration = cls._add_mostly_pct_param(
                    renderer_configuration=renderer_configuration
                )
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

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
            [
                "column",
                "strftime_format",
                "mostly",
                "row_condition",
                "condition_parser",
            ],
        )

        if not params.get("strftime_format"):
            template_str = "values must match a strftime format but none was specified."
        else:
            template_str = "values must match the following strftime format: $strftime_format"
            if params["mostly"] is not None:
                if isinstance(params["mostly"], (int, float)) and params["mostly"] < 1.0:
                    params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
                    # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")  # noqa: E501 # FIXME CoP
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            conditional_template_str = parse_row_condition_string(params["row_condition"])

            template_str, styling = _style_row_condition(
                conditional_template_str,
                template_str,
                params,
                styling,
            )

        return [
            RenderedStringTemplateContent(
                content_block_type="string_template",
                string_template={
                    "template": template_str,
                    "params": params,
                    "styling": styling,
                },
            )
        ]
