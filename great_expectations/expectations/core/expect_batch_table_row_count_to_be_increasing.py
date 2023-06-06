# TODO: <Alex>ALEX -- Remove unused imports after final implementation (including configuration validation and rendering) is completed.</Alex>
from typing import Dict, List, Optional

import numpy as np

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    TableExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import RendererConfiguration


class ExpectBatchTableRowCountToBeIncreasing(TableExpectation):
    """Expect the number of rows in each Batch to be monotinically increasing (Batch order as in Validator Batch list).

    expect_batch_table_row_count_to_be_increasing is a \
    [Table Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations).

    Args:
        N/A
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
    success_keys = ()
    default_kwarg_values = {
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }
    args_keys = ()

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        # Setting up a configuration
        super().validate_configuration(configuration)

        # TODO: <Alex>ALEX</Alex>
        # value = configuration.kwargs.get("value")
        #
        # try:
        #     assert value is not None, "An expected row count must be provided"
        #
        #     if not isinstance(value, (int, dict)):
        #         raise ValueError("Provided row count must be an integer")
        #
        #     if isinstance(value, dict):
        #         assert (
        #             "$PARAMETER" in value
        #         ), 'Evaluation Parameter dict for value kwarg must have "$PARAMETER" key.'
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))
        # TODO: <Alex>ALEX</Alex>

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        # TODO: <Alex>ALEX</Alex>
        # renderer_configuration.add_param(
        #     name="value", schema_type=ParamSchemaType.NUMBER
        # )
        # renderer_configuration.template_str = "Must have exactly $value rows."
        # TODO: <Alex>ALEX</Alex>
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
        # TODO: <Alex>ALEX</Alex>
        # renderer_configuration = RendererConfiguration(
        #     configuraiton=configuration,
        #     result=result,
        #     runtime_configuration=runtime_configuration,
        # )
        # params = substitute_none_for_missing(
        #     renderer_configuration.kwargs,
        #     ["value"],
        # )
        # template_str = "Must have exactly $value rows."
        # TODO: <Alex>ALEX</Alex>

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        # TODO: <Alex>ALEX</Alex>
                        # "template": template_str,
                        # "params": params,
                        # "styling": renderer_configuration.styling,
                        # TODO: <Alex>ALEX</Alex>
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
        actual_table_row_count = metrics.get("table.row_count")
        multi_batch_actual_table_row_count_values = [
            element[0] for element in actual_table_row_count.values()
        ]
        success = np.all(np.diff(multi_batch_actual_table_row_count_values) > 0)

        return {
            "success": success,
            "result": {"observed_value": multi_batch_actual_table_row_count_values},
        }
