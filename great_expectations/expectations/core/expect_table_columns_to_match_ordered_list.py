from itertools import zip_longest
from typing import Dict, Optional

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


class ExpectTableColumnsToMatchOrderedList(BatchExpectation):
    """Expect the columns to exactly match a specified list.

    expect_table_columns_to_match_ordered_list is a \
    [Table Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations).

    Args:
        column_list (list of str): \
            The column names, in the correct order.

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

    metric_dependencies = ("table.columns",)
    success_keys = ("column_list",)
    domain_keys = (
        "batch_id",
        "table",
        "row_condition",
        "condition_parser",
    )
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "column_list": None,
        "result_format": "BASIC",
        "column": None,
        "column_index": None,
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
    }
    args_keys = ("column_list",)

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration of an Expectation.

        For this expectation it is required that:

        - `column_list` has been provided.
        - `column_list` is one of the following types: `list`, `set`, or `None`
        - If `column_list` is a `dict`, it is assumed to be an Evaluation Parameter, and therefore the
          dictionary keys must be `$PARAMETER`.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: An `ExpectationConfiguration` to validate. If no configuration is provided, it will be pulled
                from the configuration attribute of the Expectation instance.

        Raises:
            InvalidExpectationConfigurationError: The configuration does not contain the values required by the
                Expectation.
        """
        # Setting up a configuration
        super().validate_configuration(configuration)

        # Ensuring that a proper value has been provided
        try:
            assert "column_list" in configuration.kwargs, "column_list is required"
            assert (
                isinstance(configuration.kwargs["column_list"], (list, set, dict))
                or configuration.kwargs["column_list"] is None
            ), "column_list must be a list, set, or None"
            if isinstance(configuration.kwargs["column_list"], dict):
                assert (
                    "$PARAMETER" in configuration.kwargs["column_list"]
                ), 'Evaluation Parameter dict for column_list kwarg must have "$PARAMETER" key.'

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        renderer_configuration.add_param(
            name="column_list", param_type=RendererValueType.ARRAY
        )
        params = renderer_configuration.params

        if not params.column_list:
            template_str = "Must have a list of columns in a specific order, but that order is not specified."
        else:
            template_str = "Must have these columns in this order: "
            array_param_name = "column_list"
            param_prefix = "column_list_"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            template_str += cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

        renderer_configuration.template_str = template_str

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
        _ = False if runtime_configuration.get("include_column_name") is False else True
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(configuration.kwargs, ["column_list"])

        if params["column_list"] is None:
            template_str = "Must have a list of columns in a specific order, but that order is not specified."

        else:
            template_str = "Must have these columns in this order: "
            for idx in range(len(params["column_list"]) - 1):
                template_str += f"$column_list_{str(idx)}, "
                params[f"column_list_{str(idx)}"] = params["column_list"][idx]

            last_idx = len(params["column_list"]) - 1
            template_str += f"$column_list_{str(last_idx)}"
            params[f"column_list_{str(last_idx)}"] = params["column_list"][last_idx]

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
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        # Obtaining columns and ordered list for sake of comparison
        expected_column_list = self.get_success_kwargs(configuration).get("column_list")
        actual_column_list = metrics.get("table.columns")

        if expected_column_list is None or list(actual_column_list) == list(
            expected_column_list
        ):
            return {
                "success": True,
                "result": {"observed_value": list(actual_column_list)},
            }
        else:
            # In the case of differing column lengths between the defined expectation and the observed column set, the
            # max is determined to generate the column_index.
            number_of_columns = max(len(expected_column_list), len(actual_column_list))
            column_index = range(number_of_columns)

            # Create a list of the mismatched details
            compared_lists = list(
                zip_longest(
                    column_index, list(expected_column_list), list(actual_column_list)
                )
            )
            mismatched = [
                {"Expected Column Position": i, "Expected": k, "Found": v}
                for i, k, v in compared_lists
                if k != v
            ]
            return {
                "success": False,
                "result": {
                    "observed_value": list(actual_column_list),
                    "details": {"mismatched": mismatched},
                },
            }
