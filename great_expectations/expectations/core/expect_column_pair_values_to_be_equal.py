from typing import TYPE_CHECKING, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
    InvalidExpectationConfigurationError,
    render_evaluation_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
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
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnPairValuesToBeEqual(ColumnPairMapExpectation):
    """Expect the values in column A to be the same as column B.

    expect_column_pair_values_to_be_equal is a \
    [Column Pair Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations).

    Args:
        column_A (str): The first column name
        column_B (str): The second column name

    Keyword Args:
        ignore_row_if (str): "both_values_are_missing", "either_value_is_missing", "neither"

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
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
            For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, include_config, catch_exceptions, and meta.
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": [
            "core expectation",
            "column pair map expectation",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_pair_values.equal"
    success_keys = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "mostly",
    )
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "ignore_row_if": "both_values_are_missing",
        "mostly": 1.0,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column_A",
        "column_B",
    )

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration of an Expectation.

        For `expect_column_pair_values_to_be_equal` it is required that the `configuration.kwargs` contain both
        `column_A` and `column_B` keys.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: An `ExpectationConfiguration` to validate. If no configuration is provided, it will be pulled
                from the configuration attribute of the Expectation instance.

        Raises:
            InvalidExpectationConfigurationError: The configuration does not contain the values required by the
                Expectation.
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        try:
            assert (
                "column_A" in configuration.kwargs
                and "column_B" in configuration.kwargs
            ), "both columns must be provided"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column_A", RendererValueType.STRING),
            ("column_B", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
            ("ignore_row_if", RendererValueType.STRING),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params
        template_str = ""

        if not params.column_A or not params.column_B:
            template_str += "Unrecognized kwargs for expect_column_pair_values_to_be_equal: missing column. "

        if not params.mostly or params.mostly.value == 1.0:  # noqa: PLR2004
            template_str += "Values in $column_A and $column_B must always be equal."
        else:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str = "Values in $column_A and $column_B must be equal, at least $mostly_pct % of the time."

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
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_A",
                "column_B",
                "ignore_row_if",
                "mostly",
                "row_condition",
                "condition_parser",
            ],
        )

        # NOTE: This renderer doesn't do anything with "ignore_row_if"

        if (params["column_A"] is None) or (params["column_B"] is None):
            template_str = " unrecognized kwargs for expect_column_pair_values_to_be_equal: missing column."
            params["row_condition"] = None

        if params["mostly"] is None or params["mostly"] == 1.0:  # noqa: PLR2004
            template_str = "Values in $column_A and $column_B must always be equal."
        else:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = "Values in $column_A and $column_B must be equal, at least $mostly_pct % of the time."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

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
