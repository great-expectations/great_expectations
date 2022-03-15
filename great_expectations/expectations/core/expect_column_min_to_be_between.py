from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig


class ExpectColumnMinToBeBetween(ColumnExpectation):
    """Expect the column minimum to be between an min and max value

            expect_column_min_to_be_between is a \
            :func:`column_aggregate_expectation
                <great_expectations.execution_engine.MetaExecutionEngine.column_aggregate_expectation>`.

            Args:
                column (str): \
                    The column name
                min_value (comparable type or None): \
                    The minimal column minimum allowed.
                max_value (comparable type or None): \
                    The maximal column minimum allowed.
                strict_min (boolean):
                    If True, the minimal column minimum must be strictly larger than min_value, default=False
                strict_max (boolean):
                    If True, the maximal column minimum must be strictly smaller than max_value, default=False

            Keyword Args:
                parse_strings_as_datetimes (Boolean or None): \
                    If True, parse min_value, max_values, and all non-null column values to datetimes before making \
                    comparisons.
                output_strftime_format (str or None): \
                    A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.

            Other Parameters:
                result_format (str or None): \
                    Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`. \
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

            Notes:
                These fields in the result object are customized for this expectation:
                ::

                    {
                        "observed_value": (list) The actual column min
                    }


                * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
                * If min_value is None, then max_value is treated as an upper bound
                * If max_value is None, then min_value is treated as a lower bound

            """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "package": "great_expectations",
        "tags": ["core expectation", "column aggregate expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
    }

    # Setting necessary computation metric dependencies and defining kwargs, as well as assigning kwargs default values\
    metric_dependencies = ("column.min",)
    success_keys = (
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
        "parse_strings_as_datetimes",
        "auto",
        "profiler_config",
    )

    default_profiler_config: RuleBasedProfilerConfig = RuleBasedProfilerConfig(
        name="expect_column_min_to_be_between",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        class_name="RuleBasedProfilerConfig",
        module_name="great_expectations.rule_based_profiler",
        variables={
            "strict_min": False,
            "strict_max": False,
            "false_positive_rate": 0.05,
            "sampling_method": "bootstrap",
            "num_bootstrap_samples": 9999,
            "bootstrap_random_seed": None,
            "round_decimals": None,
            "truncate_values": {
                "lower_bound": None,
                "upper_bound": None,
            },
        },
        rules={
            "default_expect_column_min_to_be_between_rule": {
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                },
                "parameter_builders": [
                    {
                        "name": "min_range_estimator",
                        "class_name": "NumericMetricRangeMultiBatchParameterBuilder",
                        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
                        "metric_name": "column.min",
                        "metric_domain_kwargs": "$domain.domain_kwargs",
                        "metric_value_kwargs": None,
                        "enforce_numeric_metric": True,
                        "replace_nan_with_zero": True,
                        "reduce_scalar_metric": True,
                        "false_positive_rate": "$variables.false_positive_rate",
                        "sampling_method": "$variables.sampling_method",
                        "num_bootstrap_samples": "$variables.num_bootstrap_samples",
                        "bootstrap_random_seed": "$variables.bootstrap_random_seed",
                        "round_decimals": "$variables.round_decimals",
                        "truncate_values": "$variables.truncate_values",
                        "json_serialize": True,
                    },
                ],
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_min_to_be_between",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        "column": "$domain.domain_kwargs.column",
                        "min_value": "$parameter.min_range_estimator.value.value_range[0]",
                        "max_value": "$parameter.min_range_estimator.value.value_range[1]",
                        "strict_min": "$variables.strict_min",
                        "strict_max": "$variables.strict_max",
                        "meta": {
                            "profiler_details": "$parameter.min_range_estimator.details",
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
    args_keys = (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
    ) -> bool:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """
        super().validate_configuration(configuration=configuration)
        self.validate_metric_value_between_configuration(configuration=configuration)

        return True

    @classmethod
    def _atomic_prescriptive_template(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
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
        params_with_json_schema = {
            "column": {"schema": {"type": "string"}, "value": params.get("column")},
            "min_value": {
                "schema": {"type": "number"},
                "value": params.get("min_value"),
            },
            "max_value": {
                "schema": {"type": "number"},
                "value": params.get("max_value"),
            },
            "parse_strings_as_datetimes": {
                "schema": {"type": "boolean"},
                "value": params.get("parse_strings_as_datetimes"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
            "strict_min": {
                "schema": {"type": "boolean"},
                "value": params.get("strict_min"),
            },
            "strict_max": {
                "schema": {"type": "boolean"},
                "value": params.get("strict_max"),
            },
        }

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "minimum value may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"minimum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"minimum value must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"minimum value must be {at_least_str} $min_value."
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
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = f"{conditional_template_str}, then {template_str}"
            params_with_json_schema.update(conditional_params)

        return (template_str, params_with_json_schema, styling)

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
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
            template_str = "minimum value may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"minimum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"minimum value must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"minimum value must be {at_least_str} $min_value."
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
    @renderer(renderer_type="renderer.descriptive.stats_table.min_row")
    def _descriptive_stats_table_min_row_renderer(
        cls,
        configuration=None,
        result=None,
        language=None,
        runtime_configuration=None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        return [
            {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Minimum",
                    "tooltip": {"content": "expect_column_min_to_be_between"},
                },
            },
            f"{result.result['observed_value']:.2f}",
        ]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.min",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )
