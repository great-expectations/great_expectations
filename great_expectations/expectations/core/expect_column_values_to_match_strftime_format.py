from datetime import datetime
from typing import List, Optional

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


class ExpectColumnValuesToMatchStrftimeFormat(ColumnMapExpectation):
    """Expect the column entries to be strings representing a date or time with a given format.

    expect_column_values_to_match_strftime_format is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        strftime_format (str): \
            A strftime format string to use for matching

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Return `"success": True` if at least mostly fraction of values match the expectation. \
            For more detail, see :ref:`mostly`.

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

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

    map_metric = "column_values.match_strftime_format"
    success_keys = (
        "strftime_format",
        "mostly",
        "auto",
        "profiler_config",
    )

    date_format_string_parameter_builder_config: ParameterBuilderConfig = (
        ParameterBuilderConfig(
            module_name="great_expectations.rule_based_profiler.parameter_builder",
            class_name="SimpleDateFormatStringParameterBuilder",
            name="date_format_string_parameter_builder",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=None,
        )
    )
    validation_parameter_builder_configs: List[ParameterBuilderConfig] = [
        date_format_string_parameter_builder_config
    ]
    default_profiler_config = RuleBasedProfilerConfig(
        name="expect_column_values_to_match_strftime_format",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        variables={},
        rules={
            "default_expect_column_values_to_match_strftime_format_rule": {
                "variables": {
                    "mostly": 1.0,
                },
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                },
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_values_to_match_strftime_format",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        "validation_parameter_builder_configs": validation_parameter_builder_configs,
                        "column": f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
                        "strftime_format": f"{PARAMETER_KEY}{date_format_string_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
                        "mostly": f"{VARIABLES_KEY}mostly",
                        "meta": {
                            "profiler_details": f"{PARAMETER_KEY}{date_format_string_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
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
        "auto": False,
        "profiler_config": default_profiler_config,
    }
    args_keys = (
        "column",
        "strftime_format",
    )

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        assert "strftime_format" in configuration.kwargs, "strftime_format is required"

        strftime_format = configuration.kwargs["strftime_format"]

        try:
            if isinstance(strftime_format, dict):
                assert (
                    "$PARAMETER" in strftime_format
                ), 'Evaluation Parameter dict for strftime_format kwarg must have "$PARAMETER" key.'
            else:
                datetime.strptime(
                    datetime.strftime(datetime.now(), strftime_format),
                    strftime_format,
                )
        except ValueError as e:
            raise ValueError(f"Unable to use provided strftime_format. {str(e)}")
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        language: Optional[str] = None,
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
            [
                "column",
                "strftime_format",
                "mostly",
                "row_condition",
                "condition_parser",
            ],
        )
        params_with_json_schema = {
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "strftime_format": {
                "schema": {"type": "string"},
                "value": params.get("strftime_format"),
            },
            "mostly": {"schema": {"type": "number"}, "value": params.get("mostly")},
            "mostly_pct": {
                "schema": {"type": "string"},
                "value": params.get("mostly_pct"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
        }

        if not params.get("strftime_format"):
            template_str = "values must match a strftime format but none was specified."
        else:
            template_str = (
                "values must match the following strftime format: $strftime_format"
            )
            if params["mostly"] is not None and params["mostly"] < 1.0:
                params_with_json_schema["mostly_pct"]["value"] = num_to_str(
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
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = f"{conditional_template_str}, then {template_str}"
            params_with_json_schema.update(conditional_params)

        return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        language: Optional[str] = None,
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
            [
                "column",
                "strftime_format",
                "mostly",
                "row_condition",
                "condition_parser",
            ],
        )

        if not params.get("strftime_format"):
            template_str = "values must match a strftime format but none was specified."
        else:
            template_str = (
                "values must match the following strftime format: $strftime_format"
            )
            if params["mostly"] is not None and params["mostly"] < 1.0:
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
