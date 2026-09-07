from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, List, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import (
    root_validator,
)
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
from great_expectations.render import (
    LegacyRendererType,
    RenderedBulletListContent,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    handle_strict_min_max,
    num_to_str,
    parse_row_condition_string,
    substitute_none_for_missing,
)

try:
    import sqlalchemy as sa  # noqa: F401, TID251 # FIXME CoP
except ImportError:
    pass

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs


EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the column entries to be strings with length between "
    "a minimum value and a maximum value (inclusive)."
)
MIN_VALUE_DESCRIPTION = "The minimum value for a column entry length."
MAX_VALUE_DESCRIPTION = "The maximum value for a column entry length."
STRICT_MIN_DESCRIPTION = "If True, values must be strictly larger than min_value."
STRICT_MAX_DESCRIPTION = "If True, values must be strictly smaller than max_value."
DATA_QUALITY_ISSUES = [DataQualityIssues.VALIDITY.value]
SUPPORTED_DATA_SOURCES = [
    SupportedDataSources.PANDAS.value,
    SupportedDataSources.SPARK.value,
    SupportedDataSources.SQLITE.value,
    SupportedDataSources.POSTGRESQL.value,
    SupportedDataSources.AURORA.value,
    SupportedDataSources.CITUS.value,
    SupportedDataSources.ALLOY.value,
    SupportedDataSources.NEON.value,
    SupportedDataSources.MYSQL.value,
    SupportedDataSources.SQL_SERVER.value,
    SupportedDataSources.BIGQUERY.value,
    SupportedDataSources.SNOWFLAKE.value,
    SupportedDataSources.DATABRICKS.value,
    SupportedDataSources.REDSHIFT.value,
]


class ExpectColumnValueLengthsToBeBetween(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    This expectation only works for string-type values. Invoking it on ints or floats will raise a TypeError.

    ExpectColumnValueLengthsToBeBetween is a \
    Column Map Expectation.

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        min_value (int or None): \
            {MIN_VALUE_DESCRIPTION}
        max_value (int or None): \
            {MAX_VALUE_DESCRIPTION}
        strict_min (boolean): \
            {STRICT_MIN_DESCRIPTION} Default=False
        strict_max (boolean): \
            {STRICT_MAX_DESCRIPTION} Default=False

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

    Notes:
        * min_value and max_value are both inclusive.
        * If min_value is None, then max_value is treated as an upper bound, and the number of acceptable rows has \
          no minimum.
        * If max_value is None, then min_value is treated as a lower bound, and the number of acceptable rows has \
          no maximum.

    See Also:
        [ExpectColumnValueLengthsToEqual](https://greatexpectations.io/expectations/expect_column_value_lengths_to_equal)

    Supported Data Sources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[7]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[8]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[9]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[10]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[11]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[12]}](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Issues:
        {DATA_QUALITY_ISSUES[0]}

    Example Data:
                test 	test2
            0 	"12345" "A"
            1 	"abcde" "13579"
            2 	"1b3d5" "24680"

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValueLengthsToBeBetween(
                    column="test2",
                    min_value=1,
                    max_value=5
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
                ExpectColumnValueLengthsToBeBetween(
                    column="test",
                    min_value=5,
                    max_value=5,
                    strict_min=True,
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
                    "element_count": 3,
                    "unexpected_count": 3,
                    "unexpected_percent": 100.0,
                    "partial_unexpected_list": [
                        "12345",
                        "abcde",
                        "1b3d5"
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

    min_value: Optional[Union[int, SuiteParameterDict]] = pydantic.Field(
        default=None, description=MIN_VALUE_DESCRIPTION
    )
    max_value: Optional[Union[int, SuiteParameterDict]] = pydantic.Field(
        default=None, description=MAX_VALUE_DESCRIPTION
    )
    strict_min: Union[bool, SuiteParameterDict] = pydantic.Field(
        default=False, description=STRICT_MIN_DESCRIPTION
    )
    strict_max: Union[bool, SuiteParameterDict] = pydantic.Field(
        default=False, description=STRICT_MAX_DESCRIPTION
    )

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

    map_metric = "column_values.value_length.between"
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
    )

    class Config:
        title = "Expect column value lengths to be between"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnValueLengthsToBeBetween]
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

    @root_validator
    def _validate_min_or_max_set(cls, values):
        min_value = values.get("min_value")
        max_value = values.get("max_value")
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")  # noqa: TRY003 # FIXME CoP
        return values

    @classmethod
    def _prescriptive_template(  # noqa: C901, PLR0912 # FIXME CoP
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
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

        if not params.min_value and not params.max_value:
            template_str = "values may have any length."
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

            if params.mostly and params.mostly.value < 1.0:
                renderer_configuration = cls._add_mostly_pct_param(
                    renderer_configuration=renderer_configuration
                )
                if params.min_value and params.max_value:
                    template_str = f"values must be {at_least_str} $min_value and {at_most_str} $max_value characters long, at least $mostly_pct % of the time."  # noqa: E501 # FIXME CoP
                elif not params.min_value:
                    template_str = f"values must be {at_most_str} $max_value characters long, at least $mostly_pct % of the time."  # noqa: E501 # FIXME CoP
                else:
                    template_str = f"values must be {at_least_str} $min_value characters long, at least $mostly_pct % of the time."  # noqa: E501 # FIXME CoP
            else:  # noqa: PLR5501 # FIXME CoP
                if params.min_value and params.max_value:
                    template_str = f"values must always be {at_least_str} $min_value and {at_most_str} $max_value characters long."  # noqa: E501 # FIXME CoP
                elif not params.min_value:
                    template_str = (
                        f"values must always be {at_most_str} $max_value characters long."
                    )
                else:
                    template_str = (
                        f"values must always be {at_least_str} $min_value characters long."
                    )

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    def _prescriptive_renderer(  # noqa: PLR0912, C901 #  too complex
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> List[
        Union[
            dict,
            str,
            RenderedStringTemplateContent,
            RenderedTableContent,
            RenderedBulletListContent,
            RenderedGraphContent,
            Any,
        ]
    ]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
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

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "values may have any length."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["mostly"] is not None:
                if isinstance(params["mostly"], (int, float)) and params["mostly"] < 1.0:
                    params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
                    # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")  # noqa: E501 # FIXME CoP
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must be {at_least_str} $min_value and {at_most_str} $max_value characters long, at least $mostly_pct % of the time."  # noqa: E501 # FIXME CoP

                elif params["min_value"] is None:
                    template_str = f"values must be {at_most_str} $max_value characters long, at least $mostly_pct % of the time."  # noqa: E501 # FIXME CoP

                elif params["max_value"] is None:
                    template_str = f"values must be {at_least_str} $min_value characters long, at least $mostly_pct % of the time."  # noqa: E501 # FIXME CoP
            else:  # noqa: PLR5501 # FIXME CoP
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must always be {at_least_str} $min_value and {at_most_str} $max_value characters long."  # noqa: E501 # FIXME CoP

                elif params["min_value"] is None:
                    template_str = (
                        f"values must always be {at_most_str} $max_value characters long."
                    )

                elif params["max_value"] is None:
                    template_str = (
                        f"values must always be {at_least_str} $min_value characters long."
                    )

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
