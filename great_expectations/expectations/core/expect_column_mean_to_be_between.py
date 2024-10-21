from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.types import Comparable  # noqa: TCH001
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.model_field_descriptions import COLUMN_DESCRIPTION
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
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs

EXPECTATION_SHORT_DESCRIPTION = (
    "Expect the column mean to be between a minimum value and a maximum value (inclusive)."
)
MIN_VALUE_DESCRIPTION = "The minimum value for the column mean."
MAX_VALUE_DESCRIPTION = "The maximum value for the column mean."
STRICT_MIN_DESCRIPTION = (
    "If True, the column mean must be strictly larger than min_value, default=False"
)
STRICT_MAX_DESCRIPTION = (
    "If True, the column mean must be strictly smaller than max_value, default=False"
)
SUPPORTED_DATA_SOURCES = [
    "Pandas",
    "Spark",
    "SQLite",
    "PostgreSQL",
    "MySQL",
    "MSSQL",
    "Redshift",
    "BigQuery",
    "Snowflake",
]
DATA_QUALITY_ISSUES = ["Numerical data"]


class ExpectColumnMeanToBeBetween(ColumnAggregateExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnMeanToBeBetween is a \
    Column Aggregate Expectation.

    Column Aggregate Expectations are one of the most common types of Expectation.
    They are evaluated for a single column, and produce an aggregate Metric, such as a mean, standard deviation, number of unique values, column type, etc.
    If that Metric meets the conditions you set, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        min_value (float or None): \
            {MIN_VALUE_DESCRIPTION}
        max_value (float or None): \
            {MAX_VALUE_DESCRIPTION}
        strict_min (boolean): \
            {STRICT_MIN_DESCRIPTION}
        strict_max (boolean): \
            {STRICT_MAX_DESCRIPTION}

    Other Parameters:
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
        * If min_value is None, then max_value is treated as an upper bound.
        * If max_value is None, then min_value is treated as a lower bound.
        * observed_value field in the result object is customized for this expectation to be a float \
            representing the true mean for the column

    See Also:
        [ExpectColumnMedianToBeBetween](https://greatexpectations.io/expectations/expect_column_median_to_be_between)
        [ExpectColumnStdevToBeBetween](https://greatexpectations.io/expectations/expect_column_stdev_to_be_between)

    Supported Datasources:
        [{SUPPORTED_DATA_SOURCES[0]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[1]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[2]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[3]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[4]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[5]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[6]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[7]}](https://docs.greatexpectations.io/docs/application_integration_support/)
        [{SUPPORTED_DATA_SOURCES[8]}](https://docs.greatexpectations.io/docs/application_integration_support/)

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
                ExpectColumnMeanToBeBetween(
                    column="test",
                    min_value=1,
                    max_value=3
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": 1.275
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnMeanToBeBetween(
                    column="test2",
                    min_value=1,
                    max_value=3
            )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": 3.375
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
    strict_min: bool = pydantic.Field(default=False, description=STRICT_MAX_DESCRIPTION)
    strict_max: bool = pydantic.Field(default=False, description=STRICT_MIN_DESCRIPTION)

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column aggregate expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\  # noqa: E501
    metric_dependencies = ("column.mean",)
    success_keys = (
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
    )

    args_keys = (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    class Config:
        title = "Expect column mean to be between"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ExpectColumnMeanToBeBetween]) -> None:
            ColumnAggregateExpectation.Config.schema_extra(schema, model)
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
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("min_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("max_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("strict_min", RendererValueType.BOOLEAN),
            ("strict_max", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.min_value and not params.max_value:
            template_str = "mean may have any numerical value."
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
                template_str = (
                    f"mean must be {at_least_str} $min_value and {at_most_str} $max_value."
                )
            elif not params.min_value:
                template_str = f"mean must be {at_most_str} $max_value."
            else:
                template_str = f"mean must be {at_least_str} $min_value."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @override
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    def _prescriptive_renderer(  # type: ignore[override] # TODO: Fix this type ignore
        cls,
        configuration: ExpectationConfiguration,
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
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "mean may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = (
                    f"mean must be {at_least_str} $min_value and {at_most_str} $max_value."
                )
            elif params["min_value"] is None:
                template_str = f"mean must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"mean must be {at_least_str} $min_value."

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

    @classmethod
    @renderer(renderer_type=LegacyDescriptiveRendererType.STATS_TABLE_MEAN_ROW)
    def _descriptive_stats_table_mean_row_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        return [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Mean",
                    "tooltip": {"content": "expect_column_mean_to_be_between"},
                },
            },
            f"{result.result['observed_value']:.2f}",
        ]

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.mean",
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )
