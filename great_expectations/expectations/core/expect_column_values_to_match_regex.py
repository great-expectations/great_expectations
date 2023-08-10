from typing import TYPE_CHECKING, List, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
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
from great_expectations.rule_based_profiler.config.base import (
    ParameterBuilderConfig,
    RuleBasedProfilerConfig,
)
from great_expectations.rule_based_profiler.parameter_container import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY,
    PARAMETER_KEY,
    VARIABLES_KEY,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnValuesToMatchRegex(ColumnMapExpectation):
    """Expect the column entries to be strings that match a given regular expression.

    Valid matches can be found \
    anywhere in the string, for example "[at]+" will identify the following strings as expected: "cat", "hat", \
    "aa", "a", and "t", and the following strings as unexpected: "fish", "dog".

    expect_column_values_to_match_regex is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).

    Args:
        column (str): \
            The column name.
        regex (str): \
            The regular expression the column entries should match.

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

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
        [expect_column_values_to_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_match_regex_list)
        [expect_column_values_to_not_match_regex](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex)
        [expect_column_values_to_not_match_regex_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_regex_list)
        [expect_column_values_to_match_like_pattern](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern)
        [expect_column_values_to_match_like_pattern_list](https://greatexpectations.io/expectations/expect_column_values_to_match_like_pattern_list)
        [expect_column_values_to_not_match_like_pattern](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern)
        [expect_column_values_to_not_match_like_pattern_list](https://greatexpectations.io/expectations/expect_column_values_to_not_match_like_pattern_list)
    """

    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_values.match_regex"
    success_keys = (
        "regex",
        "mostly",
        "auto",
        "profiler_config",
    )

    regex_pattern_string_parameter_builder_config: ParameterBuilderConfig = (
        ParameterBuilderConfig(
            module_name="great_expectations.rule_based_profiler.parameter_builder",
            class_name="RegexPatternStringParameterBuilder",
            name="regex_pattern_string_parameter_builder",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=None,
        )
    )
    validation_parameter_builder_configs: List[ParameterBuilderConfig] = [
        regex_pattern_string_parameter_builder_config
    ]
    default_profiler_config = RuleBasedProfilerConfig(
        name="expect_column_values_to_match_regex",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        variables={},
        rules={
            "default_expect_column_values_to_match_regex_rule": {
                "variables": {
                    "mostly": 1.0,
                },
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                },
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_values_to_match_regex",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        "validation_parameter_builder_configs": validation_parameter_builder_configs,
                        "column": f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
                        "regex": f"{PARAMETER_KEY}{regex_pattern_string_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
                        "mostly": f"{VARIABLES_KEY}mostly",
                        "meta": {
                            "profiler_details": f"{PARAMETER_KEY}{regex_pattern_string_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                        },
                    },
                ],
            },
        },
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,  # we expect this to be explicitly set whenever a row_condition is passed
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": True,
        "regex": "(?s).*",
        "auto": False,
        "profiler_config": default_profiler_config,
    }
    args_keys = (
        "column",
        "regex",
    )

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration for the Expectation.

        For `expect_column_values_to_match_regex`
        we require that the `configuraton.kwargs` contain a `regex` key that is either a `str` or `dict`
        containing `$PARAMETER` key and `str` value.

        Args:
            configuration: The ExpectationConfiguration to be validated.

        Raises:
            InvalidExpectationConfigurationError: The configuraton does not contain the values required by the Expectation
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration

        # supports extensibility by allowing value_set to not be provided in config but captured via child-class default_kwarg_values, e.g. parameterized expectations
        regex = configuration.kwargs.get("regex") or self.default_kwarg_values.get(
            "regex"
        )

        try:
            assert "regex" in configuration.kwargs or regex, "regex is required"
            assert isinstance(regex, (str, dict)), "regex must be a string"
            if isinstance(regex, dict):
                assert (
                    "$PARAMETER" in regex
                ), 'Evaluation Parameter dict for regex kwarg must have "$PARAMETER" key.'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    @renderer(renderer_type=LegacyRendererType.QUESTION)
    def _question_renderer(cls, configuration, result=None, runtime_configuration=None):
        column = configuration.kwargs.get("column")
        mostly = configuration.kwargs.get("mostly")
        regex = configuration.kwargs.get("regex")

        return f'Do at least {mostly * 100}% of values in column "{column}" match the regular expression {regex}?'

    @classmethod
    @renderer(renderer_type=LegacyRendererType.ANSWER)
    def _answer_renderer(
        cls, configuration=None, result=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        mostly = result.expectation_config.kwargs.get("mostly")
        regex = result.expectation_config.kwargs.get("regex")
        if result.success:
            return f'At least {mostly * 100}% of values in column "{column}" match the regular expression {regex}.'
        else:
            return f'Less than {mostly * 100}% of values in column "{column}" match the regular expression {regex}.'

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
            ("regex", RendererValueType.STRING),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.regex:
            template_str = (
                "values must match a regular expression but none was specified."
            )
        else:
            template_str = "values must match this regular expression: $regex"
            if params.mostly and params.mostly.value < 1.0:  # noqa: PLR2004
                renderer_configuration = cls._add_mostly_pct_param(
                    renderer_configuration=renderer_configuration
                )
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

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
            ["column", "regex", "mostly", "row_condition", "condition_parser"],
        )

        if not params.get("regex"):
            template_str = (
                "values must match a regular expression but none was specified."
            )
        else:
            template_str = "values must match this regular expression: $regex"
            if params["mostly"] is not None and params["mostly"] < 1.0:  # noqa: PLR2004
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if include_column_name:
            template_str = f"$column {template_str}"

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

        params_with_json_schema = {  # noqa: F841 # unused
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "mostly": {"schema": {"type": "number"}, "value": params.get("mostly")},
            "mostly_pct": {
                "schema": {"type": "number"},
                "value": params.get("mostly_pct"),
            },
            "regex": {"schema": {"type": "string"}, "value": params.get("regex")},
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
        }

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

    # examples = [
    #     {
    #         "data": {
    #             "a": ["aaa", "abb", "acc", "add", "bee"],
    #             "b": ["aaa", "abb", "acc", "bdd", None],
    #             "column_name with space": ["aaa", "abb", "acc", "add", "bee"],
    #         },
    #         "tests": [
    #             {
    #                 "title": "negative_test_insufficient_mostly_and_one_non_matching_value",
    #                 "exact_match_out": False,
    #                 "in": {"column": "a", "regex": "^a", "mostly": 0.9},
    #                 "out": {
    #                     "success": False,
    #                     "unexpected_index_list": [4],
    #                     "unexpected_list": ["bee"],
    #                 },
    #                 "include_in_gallery": True,
    #                 "suppress_test_for": ["sqlite", "mssql"],
    #             },
    #             {
    #                 "title": "positive_test_exact_mostly_w_one_non_matching_value",
    #                 "exact_match_out": False,
    #                 "in": {"column": "a", "regex": "^a", "mostly": 0.8},
    #                 "out": {
    #                     "success": True,
    #                     "unexpected_index_list": [4],
    #                     "unexpected_list": ["bee"],
    #                 },
    #                 "include_in_gallery": True,
    #                 "suppress_test_for": ["sqlite", "mssql"],
    #             },
    #         ],
    #     }
    # ]
