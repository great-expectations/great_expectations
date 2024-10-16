from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional, Tuple, Type, Union

from great_expectations.core.metric_function_types import (
    SummarizationMetricNameSuffixes,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    _format_map_output,
    render_suite_parameter_string,
)
from great_expectations.expectations.expectation_configuration import (
    parse_result_format,
)
from great_expectations.expectations.model_field_descriptions import (
    COLUMN_DESCRIPTION,
    MOSTLY_DESCRIPTION,
)
from great_expectations.render import (
    LegacyDiagnosticRendererType,
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
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs
    from great_expectations.validator.validator import (
        ValidationDependencies,
    )

EXPECTATION_SHORT_DESCRIPTION = "Expect the column values to be null."
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
DATA_QUALITY_ISSUES = ["Missingness"]


class ExpectColumnValuesToBeNull(ColumnMapExpectation):
    __doc__ = f"""{EXPECTATION_SHORT_DESCRIPTION}

    ExpectColumnValuesToBeNull is a \
    Column Map Expectation.

    Column Map Expectations are one of the most common types of Expectation.
    They are evaluated for a single column and ask a yes/no question for every row in that column.
    Based on the result, they then calculate the percentage of rows that gave a positive answer. If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column (str): \
            {COLUMN_DESCRIPTION}

    Other Parameters:
        mostly (None or a float between 0 and 1): \
            {MOSTLY_DESCRIPTION} \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
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
        [ExpectColumnValuesToNotBeNull](https://greatexpectations.io/expectations/expect_column_values_to_not_be_null)

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
            0 	NaN     "A"
            1 	True    NaN
            2 	False   NaN

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnValuesToBeNull(
                    column="test2",
                    mostly=0.66
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
                      "A"
                    ]
                  }},
                  "meta": {{}},
                  "success": true
                }}

        Failing Case:
            Input:
                ExpectColumnValuesToBeNull(
                    column="test"
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
                    "unexpected_count": 2,
                    "unexpected_percent": 66.66666666666666,
                    "partial_unexpected_list": [
                      true,
                      false
                    ]
                  }},
                  "meta": {{}},
                  "success": false
                }}
    """  # noqa: E501

    domain_keys: ClassVar[Tuple[str, ...]] = (
        "column",
        "row_condition",
        "condition_parser",
    )

    library_metadata: ClassVar[Dict[str, Union[str, list, bool]]] = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }
    _library_metadata = library_metadata

    map_metric = "column_values.null"
    args_keys = ("column",)

    class Config:
        title = "Expect column values to be null"

        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type[ExpectColumnValuesToBeNull]) -> None:
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
        cls, renderer_configuration: RendererConfiguration
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if params.mostly and params.mostly.value < 1.0:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str = "values must be null, at least $mostly_pct % of the time."
        else:
            template_str = "values must be null."

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
    ):
        renderer_configuration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        params = substitute_none_for_missing(
            renderer_configuration.configuration.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None and params["mostly"] < 1.0:
            params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = "values must be null, at least $mostly_pct % of the time."
        else:
            template_str = "values must be null."

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
    @renderer(renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE)
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        result_dict = result.result

        try:
            notnull_percent = result_dict["unexpected_percent"]
            return num_to_str(100 - notnull_percent, precision=5, use_locale=True) + "% null"
        except KeyError:
            return "unknown % null"
        except TypeError:
            return "NaN% null"

    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine, runtime_configuration
        )
        # We do not need this metric for a null metric
        validation_dependencies.remove_metric_configuration(
            metric_name=f"column_values.nonnull.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )
        return validation_dependencies

    def _validate(
        self,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        result_format = self._get_result_format(runtime_configuration=runtime_configuration)
        mostly = self._get_success_kwargs().get("mostly")
        total_count = metrics.get("table.row_count")
        unexpected_count = metrics.get(
            f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
        )

        if total_count is None or total_count == 0:
            # Vacuously true
            success = True
        else:
            success_ratio = (total_count - unexpected_count) / total_count
            success = success_ratio >= mostly

        nonnull_count = None

        return _format_map_output(
            result_format=parse_result_format(result_format),
            success=success,
            element_count=metrics.get("table.row_count"),
            nonnull_count=nonnull_count,
            unexpected_count=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_COUNT.value}"
            ),
            unexpected_list=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_VALUES.value}"
            ),
            unexpected_index_list=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_LIST.value}"
            ),
            unexpected_index_query=metrics.get(
                f"{self.map_metric}.{SummarizationMetricNameSuffixes.UNEXPECTED_INDEX_QUERY.value}"
            ),
        )
