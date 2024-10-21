from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import root_validator
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.types import Comparable  # noqa: TCH001
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    MOSTLY_DESCRIPTION,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    handle_strict_min_max,
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


EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the column entries to be between a minimum value and a maximum value (inclusive)."
)
MIN_VALUE_DESCRIPTION = "The minimum value for a column entry."
MAX_VALUE_DESCRIPTION = "The maximum value for a column entry."
STRICT_MIN_DESCRIPTION = "If True, values must be strictly larger than min_value."
STRICT_MAX_DESCRIPTION = "If True, values must be strictly smaller than max_value."
DATA_QUALITY_ISSUES = ["Distribution"]
SUPPORTED_DATA_SOURCES = [
    "Pandas",
    "Spark",
    "SQLite",
    "PostgreSQL",
    "MSSQL",
    "Redshift",
    "BigQuery",
    "Snowflake",
]


class ExpectColumnValuesToBeBetween(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnValuesToBeBetween is a \
    Column Map Expectation

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        min_value (comparable type or None): \
        {MIN_VALUE_DESCRIPTION}
        max_value (comparable type or None): \
        {MAX_VALUE_DESCRIPTION}
        strict_min (boolean): \
            {STRICT_MIN_DESCRIPTION} Default=False.
        strict_max (boolean): \
            {STRICT_MAX_DESCRIPTION} Default=False.

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

    Notes:
        * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
        * If min_value is None, then max_value is treated as an upper bound, and there is no minimum value checked.
        * If max_value is None, then min_value is treated as a lower bound, and there is no maximum value checked.

    See Also:
        [ExpectColumnValueLengthsToBeBetween](https://greatexpectations.io/expectations/expect_column_value_lengths_to_be_between)

    Supported Datasources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[7]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                test 	test2
            0 	1       1
            1 	1.3     7
            2 	.8      2.5
            3   2       3

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValuesToBeBetween(
                    column="test",
                    min_value=.5,
                    max_value=2
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 4,
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
                ExpectColumnValuesToBeBetween(
                    column="test",
                    min_value=1,
                    max_value=7,
                    strict_min=False,
                    strict_max=True
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "element_count": 4,
                    "unexpected_count": 1,
                    "unexpected_percent": 25.0,
                    "partial_unexpected_list": [
                      7.0
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 25.0,
                    "unexpected_percent_nonmissing": 25.0
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    min_value: Optional[Comparable] = pydantic.Field(
        default=None, description=MIN_VALUE_DESCRIPTION
    )
    max_value: Optional[Comparable] = pydantic.Field(
        default=None, description=MAX_VALUE_DESCRIPTION
    )
    strict_min: bool = pydantic.Field(default=False, description=STRICT_MIN_DESCRIPTION)
    strict_max: bool = pydantic.Field(default=False, description=MAX_VALUE_DESCRIPTION)

    @classmethod
    @root_validator(pre=True)
    def check_min_val_or_max_val(cls, values: dict) -> dict:
        min_val = values.get("min_val")
        max_val = values.get("max_val")

        if min_val is None and max_val is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003

        return values

    # This dictionary contains metadata for display in the public gallery
    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    map_metric = "column_values.between"
    success_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "mostly",
    )

    args_keys = (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    class Config:
        title = "Expect column values to be between"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnValuesToBeBetween]
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
    @override
    def _prescriptive_template(  # noqa: C901 - too complex
        cls,
        renderer_configuration: RendererConfiguration,
    ):
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("min_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("max_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("mostly", RendererValueType.NUMBER),
            ("strict_min", RendererValueType.BOOLEAN),
            ("strict_max", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        template_str = ""
        at_least_str = ""
        at_most_str = ""
        if not params.min_value and not params.max_value:
            template_str += "may have any numerical value."
        else:
            at_least_str = "greater than or equal to"
            if params.strict_min:
                at_least_str = cls._get_strict_min_string(
                    renderer_configuration=renderer_configuration
                )
            at_most_str = "less than or equal to"
            if params.strict_max:
                at_most_str = cls._get_strict_max_string(
                    renderer_configuration=renderer_configuration
                )

            if params.min_value and params.max_value:
                template_str += (
                    f"values must be {at_least_str} $min_value and {at_most_str} $max_value"
                )
            elif not params.min_value:
                template_str += f"values must be {at_most_str} $max_value"
            else:
                template_str += f"values must be {at_least_str} $min_value"

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

    # NOTE: This method is a pretty good example of good usage of `params`.
    @classmethod
    @override
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
            configuration.kwargs if configuration else {},
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str += "may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            mostly_str = ""
            if params["mostly"] is not None and params["mostly"] < 1.0:
                params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")  # noqa: E501
                mostly_str = ", at least $mostly_pct % of the time"

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str += f"values must be {at_least_str} $min_value and {at_most_str} $max_value{mostly_str}."  # noqa: E501

            elif params["min_value"] is None:
                template_str += f"values must be {at_most_str} $max_value{mostly_str}."

            elif params["max_value"] is None:
                template_str += f"values must be {at_least_str} $min_value{mostly_str}."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

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
