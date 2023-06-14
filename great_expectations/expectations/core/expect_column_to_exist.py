from typing import TYPE_CHECKING, Dict, Optional

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
from great_expectations.render.util import ordinal, substitute_none_for_missing

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnToExist(BatchExpectation):
    """Expect the specified column to exist.

    expect_column_to_exist is a \
    [Table Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations).

    Args:
        column (str): \
            The column name.

    Other Parameters:
        column_index (int or None): \
            If not None, checks the order of the columns. The expectation will fail if the \
            column is not in location column_index (zero-indexed).
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

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "table expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    metric_dependencies = ("table.columns",)
    success_keys = (
        "column",
        "column_index",
    )
    domain_keys = (
        "batch_id",
        "table",
    )
    default_kwarg_values = {
        "column": None,
        "column_index": None,
    }
    args_keys = ("column", "column_index")

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration of an Expectation.

        For `expect_column_to_exist` it is required that the `configuration.kwargs` contain a `column` key that is a
        string (the name of the column). Also, if `configuration.kwargs` contains `column_index`, then `column_index`
        must be an `int` or `dict`; if a `dict`, then `column_index` must contain the `$PARAMETER` key.

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
        configuration = configuration or self.configuration

        # Ensuring that a proper value has been provided
        try:
            assert "column" in configuration.kwargs, "A column name must be provided"
            assert isinstance(
                configuration.kwargs["column"], str
            ), "Column name must be a string"
            assert (
                isinstance(configuration.kwargs.get("column_index"), (int, dict))
                or configuration.kwargs.get("column_index") is None
            ), "column_index must be an integer, dict, or None"
            if isinstance(configuration.kwargs.get("column_index"), dict):
                assert "$PARAMETER" in configuration.kwargs.get(
                    "column_index"
                ), 'Evaluation Parameter dict for column_index kwarg must have "$PARAMETER" key.'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("column_index", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.column_index:
            if renderer_configuration.include_column_name:
                template_str = "$column is a required field."
            else:
                template_str = "is a required field."
        else:
            renderer_configuration.add_param(
                name="column_indexth",
                param_type=RendererValueType.STRING,
                value=ordinal(params.column_index.value),
            )
            if renderer_configuration.include_column_name:
                template_str = "$column must be the $column_indexth field."
            else:
                template_str = "must be the $column_indexth field."

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
        include_column_name = (
            False if runtime_configuration.get("include_column_name") is False else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            ["column", "column_index"],
        )

        if params["column_index"] is None:
            if include_column_name:
                template_str = "$column is a required field."
            else:
                template_str = "is a required field."
        else:
            params["column_indexth"] = ordinal(params["column_index"])
            if include_column_name:
                template_str = "$column must be the $column_indexth field."
            else:
                template_str = "must be the $column_indexth field."

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
        actual_columns = metrics.get("table.columns")
        expected_column_name = self.get_success_kwargs().get("column")
        expected_column_index = self.get_success_kwargs().get("column_index")

        if expected_column_index:
            try:
                success = actual_columns[expected_column_index] == expected_column_name
            except IndexError:
                success = False
        else:
            success = expected_column_name in actual_columns

        return {"success": success}
