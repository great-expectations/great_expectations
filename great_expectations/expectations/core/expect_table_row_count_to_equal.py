from typing import Dict, List, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    BatchExpectation,
    InvalidExpectationConfigurationError,
    render_evaluation_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import substitute_none_for_missing


class ExpectTableRowCountToEqual(BatchExpectation):
    """Expect the number of rows to equal a value.

    expect_table_row_count_to_equal is a \
    [Table Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations).

    Args:
        value (int): \
            The expected number of rows.

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
    """

    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "table expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    metric_dependencies = ("table.row_count",)
    success_keys = ("value",)
    default_kwarg_values = {
        "value": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }
    args_keys = ("value",)

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validate the configuration of an Expectation.

        For `expect_table_row_count_to_equal` we require that the `configuraton.kwargs` contain
        a `value` key that is either an `int` or a `dict` with an Evaluation Parameter.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: An `ExpectationConfiguration` to validate. If no configuration is provided, it will be pulled
                from the configuration attribute of the Expectation instance.

        Raises:
            `InvalidExpectationConfigurationError`: The configuration does not contain the values required
                by the Expectation.
        """

        # Setting up a configuration
        super().validate_configuration(configuration)

        value: Optional[int] = (
            configuration.kwargs.get("value")
            if configuration and configuration.kwargs
            else None
        )

        try:
            assert value is not None, "An expected row count must be provided"

            if not isinstance(value, (int, dict)):
                raise ValueError("Provided row count must be an integer")

            if isinstance(value, dict):
                assert (
                    "$PARAMETER" in value
                ), 'Evaluation Parameter dict for value kwarg must have "$PARAMETER" key.'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        renderer_configuration.add_param(
            name="value", param_type=RendererValueType.NUMBER
        )
        renderer_configuration.template_str = "Must have exactly $value rows."
        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[RenderedStringTemplateContent]:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        params = substitute_none_for_missing(
            renderer_configuration.kwargs,
            ["value"],
        )
        template_str = "Must have exactly $value rows."

        styling = (
            runtime_configuration.get("styling", {}) if runtime_configuration else {}
        )

        return [
            RenderedStringTemplateContent(
                **{  # type: ignore[arg-type]
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
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        expected_table_row_count = self.get_success_kwargs().get("value")
        actual_table_row_count = metrics.get("table.row_count")

        return {
            "success": actual_table_row_count == expected_table_row_count,
            "result": {"observed_value": actual_table_row_count},
        }
