from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.core.suite_parameters import (
    SuiteParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_suite_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import substitute_none_for_missing

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs


EXPECTATION_SHORT_DESCRIPTION = "Expect the columns in a table to match an unordered set."
COLUMN_SET_DESCRIPTION = "The column names, in any order."
EXACT_MATCH_DESCRIPTION = (
    "If True, the list of columns must exactly match the observed columns. "
    "If False, observed columns must include column_set but additional columns will pass."
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
DATA_QUALITY_ISSUES = ["Schema"]


class ExpectTableColumnsToMatchSet(BatchExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectTableColumnsToMatchSet is a \
    Batch Expectation.

    BatchExpectations are one of the most common types of Expectation.
    They are evaluated for an entire Batch, and answer a semantic question about the Batch itself.

    Args:
        column_set (list of str): {COLUMN_SET_DESCRIPTION}
        exact_match (boolean): \
            {EXACT_MATCH_DESCRIPTION} Default True.

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
            0 	1.00 	2
            1 	2.30 	5
            2 	4.33 	0

    Code Examples:
        Passing Case:
            Input:
                ExpectTableColumnsToMatchSet(
                    column_set=["test"],
                    exact_match=False
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
                      "test",
                      "test2"
                    ],
                    "details": {{
                      "mismatched": {{
                        "unexpected": [
                          "test2"
                        ]
                      }}
                    }}
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectTableColumnsToMatchSet(
                    column_set=["test2", "test3"],
                    exact_match=True
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
                      "test",
                      "test2"
                    ],
                    "details": {{
                      "mismatched": {{
                        "unexpected": [
                          "test"
                        ],
                        "missing": [
                          "test3"
                        ]
                      }}
                    }}
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    column_set: Union[list, set, SuiteParameterDict, None] = pydantic.Field(
        description=COLUMN_SET_DESCRIPTION
    )
    exact_match: Union[bool, None] = pydantic.Field(description=EXACT_MATCH_DESCRIPTION)

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "table expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    metric_dependencies = ("table.columns",)
    success_keys = (
        "column_set",
        "exact_match",
    )
    args_keys = (
        "column_set",
        "exact_match",
    )

    class Config:
        title = "Expect table columns to match set"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ExpectTableColumnsToMatchSet]) -> None:
            BatchExpectation.Config.schema_extra(schema, model)
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
            ("column_set", RendererValueType.ARRAY),
            ("exact_match", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.column_set:
            template_str = "Must specify a set or list of columns."
        else:
            array_param_name = "column_set"
            param_prefix = "column_set_"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            column_set_str: str = cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

            exact_match_str = (
                "exactly" if params.exact_match and params.exact_match.value is True else "at least"
            )

            template_str = (
                f"Must have {exact_match_str} these columns (in any order): {column_set_str}"
            )

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
        _ = runtime_configuration.get("include_column_name") is not False
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["column_set", "exact_match"])

        if params["column_set"] is None:
            template_str = "Must specify a set or list of columns."

        else:
            # standardize order of the set for output
            params["column_list"] = list(params["column_set"])

            column_list_template_str = ", ".join(
                [f"$column_list_{idx}" for idx in range(len(params["column_list"]))]
            )

            exact_match_str = "exactly" if params["exact_match"] is True else "at least"

            template_str = f"Must have {exact_match_str} these columns (in any order): {column_list_template_str}"  # noqa: E501

            for idx in range(len(params["column_list"])):
                params[f"column_list_{idx!s}"] = params["column_list"][idx]

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

    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        # Obtaining columns and ordered list for sake of comparison
        expected_column_set = self._get_success_kwargs().get("column_set")
        expected_column_set = set(expected_column_set) if expected_column_set is not None else set()
        actual_column_list = metrics.get("table.columns")
        actual_column_set = set(actual_column_list)
        exact_match = self._get_success_kwargs().get("exact_match")

        if (
            (expected_column_set is None) and (exact_match is not True)
        ) or actual_column_set == expected_column_set:
            return {"success": True, "result": {"observed_value": actual_column_list}}
        else:
            # Convert to lists and sort to lock order for testing and output rendering
            # unexpected_list contains items from the dataset columns that are not in expected_column_set  # noqa: E501
            unexpected_list = sorted(list(actual_column_set - expected_column_set))
            # missing_list contains items from expected_column_set that are not in the dataset columns  # noqa: E501
            missing_list = sorted(list(expected_column_set - actual_column_set))
            # observed_value contains items that are in the dataset columns
            observed_value = sorted(actual_column_list)

            mismatched = {}
            if len(unexpected_list) > 0:
                mismatched["unexpected"] = unexpected_list
            if len(missing_list) > 0:
                mismatched["missing"] = missing_list

            result = {
                "observed_value": observed_value,
                "details": {"mismatched": mismatched},
            }

            return_success = {
                "success": True,
                "result": result,
            }
            return_failed = {
                "success": False,
                "result": result,
            }

            if exact_match:
                return return_failed
            else:  # noqa: PLR5501
                # Failed if there are items in the missing list (but OK to have unexpected_list)
                if len(missing_list) > 0:
                    return return_failed
                # Passed if there are no items in the missing list
                else:
                    return return_success
