from copy import deepcopy
from typing import Dict, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_evaluation_parameter_string,
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
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,  # noqa: TCH001
)
from great_expectations.validator.validator import (
    ValidationDependencies,
)


class ExpectTableRowCountToEqualOtherTable(BatchExpectation):
    """Expect the number of rows to equal the number in another table.

    expect_table_row_count_to_equal_other_table is a \
    [Table Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations).

    Args:
        other_table_name (str): \
            The name of the other table.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        include_config (boolean): \
            If True, then include the expectation config as part of the result object.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.

    See Also:
        [expect_table_row_count_to_be_between](https://greatexpectations.io/expectations/expect_table_row_count_to_be_between)
        [expect_table_row_count_to_equal](https://greatexpectations.io/expectations/expect_table_row_count_to_equal)
    """

    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "table expectation", "multi-table expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    metric_dependencies = ("table.row_count",)
    success_keys = ("other_table_name",)
    default_kwarg_values = {
        "other_table_name": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = ("other_table_name",)

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        renderer_configuration.add_param(
            name="other_table_name", param_type=RendererValueType.STRING
        )
        renderer_configuration.template_str = (
            "Row count must equal the row count of table $other_table_name."
        )
        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["other_table_name"])
        template_str = "Row count must equal the row count of table $other_table_name."

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
        if not result.result.get("observed_value"):
            return "--"

        self_table_row_count = num_to_str(result.result["observed_value"]["self"])
        other_table_row_count = num_to_str(result.result["observed_value"]["other"])

        return RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Row Count: $self_table_row_count<br>Other Table Row Count: $other_table_row_count",
                    "params": {
                        "self_table_row_count": self_table_row_count,
                        "other_table_row_count": other_table_row_count,
                    },
                    "styling": {"classes": ["mb-2"]},
                },
            }
        )

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = (
            super().get_validation_dependencies(
                configuration, execution_engine, runtime_configuration
            )
        )
        other_table_name = configuration.kwargs.get("other_table_name")
        # create copy of table.row_count metric and modify "table" metric domain kwarg to be other table name
        table_row_count_metric_config_other: MetricConfiguration = deepcopy(
            validation_dependencies.get_metric_configuration(
                metric_name="table.row_count"
            )
        )
        table_row_count_metric_config_other.metric_domain_kwargs[
            "table"
        ] = other_table_name
        # rename original "table.row_count" metric to "table.row_count.self"
        validation_dependencies.set_metric_configuration(
            metric_name="table.row_count.self",
            metric_configuration=validation_dependencies.get_metric_configuration(
                metric_name="table.row_count"
            ),
        )
        validation_dependencies.remove_metric_configuration(
            metric_name="table.row_count"
        )
        # add a new metric dependency named "table.row_count.other" with modified metric config
        validation_dependencies.set_metric_configuration(
            "table.row_count.other", table_row_count_metric_config_other
        )
        return validation_dependencies

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """Validates the configuration of an Expectation. This expectation has no configuration."""
        super().validate_configuration(configuration)

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        table_row_count_self = metrics["table.row_count.self"]
        table_row_count_other = metrics["table.row_count.other"]

        return {
            "success": table_row_count_self == table_row_count_other,
            "result": {
                "observed_value": {
                    "self": table_row_count_self,
                    "other": table_row_count_other,
                }
            },
        }
