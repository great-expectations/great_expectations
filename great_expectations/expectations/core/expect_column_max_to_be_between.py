from typing import TYPE_CHECKING, Dict, List, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    render_evaluation_parameter_string,
)
from great_expectations.render import (
    LegacyDescriptiveRendererType,
    LegacyRendererType,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.rule_based_profiler.config import (
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

try:
    import sqlalchemy as sa  # noqa: F401, TID251
except ImportError:
    pass


from great_expectations.expectations.expectation import ColumnAggregateExpectation
from great_expectations.render.renderer.renderer import renderer

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnMaxToBeBetween(ColumnAggregateExpectation):
    """Expect the column maximum to be between a minimum value and a maximum value.

    expect_column_max_to_be_between is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations)

    Args:
        column (str): \
            The column name
        min_value (comparable type or None): \
            The minimum value of the acceptable range for the column maximum.
        max_value (comparable type or None): \
            The maximum value of the acceptable range for the column maximum.
        strict_min (boolean): \
            If True, the lower bound of the column maximum acceptable range must be strictly larger than min_value, default=False
        strict_max (boolean): \
            If True, the upper bound of the column maximum acceptable range must be strictly smaller than max_value, default=False

    Keyword Args:
        parse_strings_as_datetimes (Boolean or None): \
            If True, parse min_value, max_values, and all non-null column values to datetimes before making \
            comparisons.
        output_strftime_format (str or None): \
            A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.

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

    Notes:
        * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
        * If min_value is None, then max_value is treated as an upper bound
        * If max_value is None, then min_value is treated as a lower bound
        * observed_value field in the result object is customized for this expectation to be a list \
            representing the actual column max

    See Also:
        [expect_column_min_to_be_between](https://greatexpectations.io/expectations/expect_column_min_to_be_between)
    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column aggregate expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.max",)
    success_keys = (
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
        "parse_strings_as_datetimes",
        "auto",
        "profiler_config",
    )

    max_range_estimator_parameter_builder_config = ParameterBuilderConfig(
        module_name="great_expectations.rule_based_profiler.parameter_builder",
        class_name="NumericMetricRangeMultiBatchParameterBuilder",
        name="max_range_estimator",
        metric_name="column.max",
        metric_multi_batch_parameter_builder_name=None,
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs=None,
        enforce_numeric_metric=True,
        replace_nan_with_zero=True,
        reduce_scalar_metric=True,
        false_positive_rate=f"{VARIABLES_KEY}false_positive_rate",
        estimator=f"{VARIABLES_KEY}estimator",
        n_resamples=f"{VARIABLES_KEY}n_resamples",
        random_seed=f"{VARIABLES_KEY}random_seed",
        quantile_statistic_interpolation_method=f"{VARIABLES_KEY}quantile_statistic_interpolation_method",
        quantile_bias_correction=f"{VARIABLES_KEY}quantile_bias_correction",
        quantile_bias_std_error_ratio_threshold=f"{VARIABLES_KEY}quantile_bias_std_error_ratio_threshold",
        include_estimator_samples_histogram_in_details=f"{VARIABLES_KEY}include_estimator_samples_histogram_in_details",
        truncate_values=f"{VARIABLES_KEY}truncate_values",
        round_decimals=f"{VARIABLES_KEY}round_decimals",
        evaluation_parameter_builder_configs=None,
    )
    validation_parameter_builder_configs: List[ParameterBuilderConfig] = [
        max_range_estimator_parameter_builder_config,
    ]
    default_profiler_config = RuleBasedProfilerConfig(
        name="expect_column_max_to_be_between",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        variables={},
        rules={
            "default_expect_column_max_to_be_between_rule": {
                "variables": {
                    "strict_min": False,
                    "strict_max": False,
                    "estimator": "exact",
                    "include_estimator_samples_histogram_in_details": False,
                    "truncate_values": {
                        "lower_bound": None,
                        "upper_bound": None,
                    },
                    "round_decimals": 1,
                },
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                },
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_max_to_be_between",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        "validation_parameter_builder_configs": validation_parameter_builder_configs,
                        "column": f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
                        "min_value": f"{PARAMETER_KEY}{max_range_estimator_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[0]",
                        "max_value": f"{PARAMETER_KEY}{max_range_estimator_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}[1]",
                        "strict_min": f"{VARIABLES_KEY}strict_min",
                        "strict_max": f"{VARIABLES_KEY}strict_max",
                        "meta": {
                            "profiler_details": f"{PARAMETER_KEY}{max_range_estimator_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                        },
                    },
                ],
            },
        },
    )

    # Default values
    default_kwarg_values = {
        "min_value": None,
        "max_value": None,
        "strict_min": None,
        "strict_max": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "parse_strings_as_datetimes": False,
        "auto": False,
        "profiler_config": default_profiler_config,
    }
    args_keys = ("column", "min_value", "max_value", "strict_min", "strict_max")

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration for the Expectation.

        For this expectation, `configuraton.kwargs` may contain `min_value` and `max_value` whose value is either
        a number or date.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation

        Raises:
            InvalidExpectationConfigurationError: if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("min_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("max_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("parse_strings_as_datetimes", RendererValueType.BOOLEAN),
            ("strict_min", RendererValueType.BOOLEAN),
            ("strict_max", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.min_value and not params.max_value:
            template_str = "maximum value may have any numerical value."
        else:
            at_least_str = "greater than or equal to"
            if params.strict_min:
                at_least_str = cls._get_strict_min_string(
                    renderer_configuration=renderer_configuration
                )
            at_most_str = "less than or equal to"
            if params.strict_max:
                at_most_str = cls._get_strict_max_string(
                    renderer_configuration=renderer_configuration
                )

            if params.min_value and params.max_value:
                if params.min_value == params.max_value:
                    template_str = "maximum value must be $min_value"
                else:
                    template_str = f"maximum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif not params.min_value:
                template_str = f"maximum value must be {at_most_str} $max_value."
            else:
                template_str = f"maximum value must be {at_least_str} $min_value."

        if params.parse_strings_as_datetimes:
            template_str += " Values should be parsed as datetimes."

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
            [
                "column",
                "min_value",
                "max_value",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "maximum value may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"maximum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"maximum value must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"maximum value must be {at_least_str} $min_value."
            else:
                template_str = ""

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

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

    @classmethod
    @renderer(renderer_type=LegacyDescriptiveRendererType.STATS_TABLE_MAX_ROW)
    def _descriptive_stats_table_max_row_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        return [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Maximum",
                    "tooltip": {"content": "expect_column_max_to_be_between"},
                },
            },
            f"{result.result['observed_value']:.2f}",
        ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.max",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )
