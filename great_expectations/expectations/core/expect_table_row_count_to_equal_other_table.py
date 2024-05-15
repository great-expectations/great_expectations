from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Dict, Optional

from great_expectations.compatibility.typing_extensions import override
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_suite_parameter_string,
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
from great_expectations.validator.metric_configuration import (  # noqa: TCH001
    MetricConfiguration,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.validator.validator import ValidationDependencies


class ExpectTableRowCountToEqualOtherTable(BatchExpectation):
    """Expect the number of rows to equal the number in another table within the same database.

    expect_table_row_count_to_equal_other_table is a \
    [Batch Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_batch_expectations).

    BatchExpectations are one of the most common types of Expectation.
    They are evaluated for an entire Batch, and answer a semantic question about the Batch itself.

    Args:
        other_table_name (str): \
            The name of the other table. Other table must be located within the same database.

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
        [expect_table_row_count_to_be_between](https://greatexpectations.io/expectations/expect_table_row_count_to_be_between)
        [expect_table_row_count_to_equal](https://greatexpectations.io/expectations/expect_table_row_count_to_equal)

    Supported Datasources:
        [Snowflake](https://docs.greatexpectations.io/docs/application_integration_support/)
        [PostgreSQL](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        Volume

    Example Data:
            test_table
                test 	test2
            0 	1.00 	2
            1 	2.30 	5
            2 	4.33 	0

            test_table_two
                test 	test2
            0 	1.00 	2
            1 	2.30 	5
            2 	4.33 	0

            test_table_three
                test 	test2
            0 	1.00 	2
            1 	2.30 	5

    Code Examples:
        Passing Case:
            Input:
                ExpectTableRowCountToEqualOtherTable(
                    other_table_name=test_table_two
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "observed_value": 3
                  },
                  "meta": {},
                  "success": true
                }

        Failing Case:
            Input:
                ExpectTableRowCountToEqualOtherTable(
                    other_table_name=test_table_three
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "observed_value": 2
                  },
                  "meta": {},
                  "success": false
                }
    """  # noqa: E501

    other_table_name: str

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
    args_keys = ("other_table_name",)

    @override
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

    @override
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
        styling = runtime_configuration.get("styling")
        if not configuration:
            raise ValueError("configuration is required for prescriptive renderer")  # noqa: TRY003
        params = substitute_none_for_missing(configuration.kwargs, ["other_table_name"])
        template_str = "Row count must equal the row count of table $other_table_name."

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
    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE)
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        if not result or result.result.get("observed_value"):
            return "--"

        self_table_row_count = num_to_str(result.result["observed_value"]["self"])
        other_table_row_count = num_to_str(result.result["observed_value"]["other"])

        return RenderedStringTemplateContent(
            content_block_type="string_template",
            string_template={
                "template": "Row Count: $self_table_row_count<br>Other Table Row Count: $other_table_row_count",  # noqa: E501
                "params": {
                    "self_table_row_count": self_table_row_count,
                    "other_table_row_count": other_table_row_count,
                },
                "styling": {"classes": ["mb-2"]},
            },
        )

    @override
    def get_validation_dependencies(
        self,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = super().get_validation_dependencies(
            execution_engine, runtime_configuration
        )

        configuration = self.configuration
        kwargs = configuration.kwargs if configuration else {}
        other_table_name = kwargs.get("other_table_name")

        # create copy of table.row_count metric and modify "table" metric domain kwarg to be other table name  # noqa: E501
        table_row_count_metric_config_other: Optional[MetricConfiguration] = deepcopy(
            validation_dependencies.get_metric_configuration(metric_name="table.row_count")
        )
        assert (
            table_row_count_metric_config_other
        ), "table_row_count_metric_config_other should not be None"

        table_row_count_metric_config_other.metric_domain_kwargs["table"] = other_table_name
        # rename original "table.row_count" metric to "table.row_count.self"
        table_row_count_metric = validation_dependencies.get_metric_configuration(
            metric_name="table.row_count"
        )
        assert table_row_count_metric, "table_row_count_metric should not be None"
        validation_dependencies.set_metric_configuration(
            metric_name="table.row_count.self",
            metric_configuration=table_row_count_metric,
        )
        validation_dependencies.remove_metric_configuration(metric_name="table.row_count")
        # add a new metric dependency named "table.row_count.other" with modified metric config
        validation_dependencies.set_metric_configuration(
            "table.row_count.other", table_row_count_metric_config_other
        )
        return validation_dependencies

    @override
    def _validate(
        self,
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
