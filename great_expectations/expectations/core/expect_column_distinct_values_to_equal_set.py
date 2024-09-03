from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001  # used in pydantic validation
)
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    render_suite_parameter_string,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    VALUE_SET_DESCRIPTION,
)
from great_expectations.expectations.model_field_types import (
    ValueSet,  # noqa: TCH001  # type needed in pydantic validation
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
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

EXPECTATION_SHORT_DESCRIPTION = "Expect the set of distinct column values to equal a given set."
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
DATA_QUALITY_ISSUES = ["Sets"]


class ExpectColumnDistinctValuesToEqualSet(ColumnAggregateExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnDistinctValuesToEqualSet is a \
    Column Aggregate Expectation.

    Column Aggregate Expectations are one of the most common types of Expectation.
    They are evaluated for a single column, and produce an aggregate Metric, such as a mean, standard deviation, number of unique values, column type, etc.
    If that Metric meets the conditions you set, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}
        value_set (set-like): \
            {VALUE_SET_DESCRIPTION}

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

    See Also:
        [ExpectColumnDistinctValuesToBeInSet](https://greatexpectations.io/expectations/expect_column_distinct_values_to_be_in_set)
        [ExpectColumnDistinctValuesToContainSet](https://greatexpectations.io/expectations/expect_column_distinct_values_to_contain_set)

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
            1 	2       1
            2 	4       1

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnDistinctValuesToEqualSet(
                    column="test",
                    value_set=[1, 2, 4]
                )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": [
                      1,
                      2,
                      4
                    ],
                    "details": {{
                      "value_counts": [
                        {{
                          "value": 1,
                          "count": 1
                        }},
                        {{
                          "value": 2,
                          "count": 1
                        }},
                        {{
                          "value": 4,
                          "count": 1
                        }}
                      ]
                    }}
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnDistinctValuesToEqualSet(
                    column="test2",
                    value_set=[3, 2, 4]
                )

            Output:
                {{
                  "exception_info": {{
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  }},
                  "result": {{
                    "observed_value": [
                      1
                    ],
                    "details": {{
                      "value_counts": [
                        {{
                          "value": 1,
                          "count": 3
                        }}
                      ]
                    }}
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    value_set: Union[Optional[ValueSet], SuiteParameterDict] = pydantic.Field(
        description=VALUE_SET_DESCRIPTION
    )

    # This dictionary contains metadata for display in the public gallery
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
    metric_dependencies = ("column.value_counts",)
    success_keys = ("value_set",)
    args_keys = (
        "column",
        "value_set",
    )

    class Config:
        title = "Expect column distinct values to equal set"

        @staticmethod
        def schema_extra(
            schema: Dict[str, Any], model: Type[ExpectColumnDistinctValuesToEqualSet]
        ) -> None:
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

    @override
    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("value_set", RendererValueType.ARRAY),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params
        template_str = ""

        if params.value_set:
            array_param_name = "value_set"
            param_prefix = "v__"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            value_set_str: str = cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            template_str += f"distinct values must match this set: {value_set_str}."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @override
    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
    def _prescriptive_renderer(  # noqa: C901 - too complex
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        params = substitute_none_for_missing(
            renderer_configuration.kwargs,
            [
                "column",
                "value_set",
                "row_condition",
                "condition_parser",
            ],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params[f"v__{i!s}"] = v

            values_string = " ".join([f"$v__{i!s}" for i, v in enumerate(params["value_set"])])

        template_str = f"distinct values must match this set: {values_string}."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params[f"v__{i!s}"] = v

            values_string = " ".join([f"$v__{i!s}" for i, v in enumerate(params["value_set"])])

        template_str = f"distinct values must match this set: {values_string}."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

        styling = runtime_configuration.get("styling", {}) if runtime_configuration else {}

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

    @override
    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        observed_value_counts = metrics["column.value_counts"]
        observed_value_set = set(observed_value_counts.index)
        value_set = self._get_success_kwargs()["value_set"]

        parsed_value_set = value_set

        expected_value_set = set(parsed_value_set)

        return {
            "success": observed_value_set == expected_value_set,
            "result": {
                "observed_value": sorted(list(observed_value_set)),
                "details": {"value_counts": observed_value_counts},
            },
        }
