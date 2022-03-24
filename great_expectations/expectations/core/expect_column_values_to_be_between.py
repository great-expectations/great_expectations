from typing import List, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    handle_strict_min_max,
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.rule_based_profiler.types import (
    DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
    FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER,
    VARIABLES_KEY,
)


class ExpectColumnValuesToBeBetween(ColumnMapExpectation):
    """Expect column entries to be between a minimum value and a maximum value (inclusive).

    expect_column_values_to_be_between is a \
    :func:`column_map_expectation <great_expectations.execution_engine.execution_engine.MetaExecutionEngine
    .column_map_expectation>`.

    Args:
        column (str): \
            The column name.
        min_value (comparable type or None): The minimum value for a column entry.
        max_value (comparable type or None): The maximum value for a column entry.
        strict_min (boolean):
            If True, values must be strictly larger than min_value, default=False
        strict_max (boolean):
            If True, values must be strictly smaller than max_value, default=False

    Keyword Args:
        allow_cross_type_comparisons (boolean or None) : If True, allow comparisons between types (e.g. integer and\
            string). Otherwise, attempting such comparisons will raise an exception.
        parse_strings_as_datetimes (boolean or None) : If True, parse min_value, max_value, and all non-null column\
            values to datetimes before making comparisons.
        output_strftime_format (str or None): \
            A valid strfime format for datetime output. Only used if parse_strings_as_datetimes=True.
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

    Notes:
        * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
        * If min_value is None, then max_value is treated as an upper bound, and there is no minimum value checked.
        * If max_value is None, then min_value is treated as a lower bound, and there is no maximum value checked.

    See Also:
        :func:`expect_column_value_lengths_to_be_between \
        <great_expectations.execution_engine.execution_engine.ExecutionEngine
        .expect_column_value_lengths_to_be_between>`

    """

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": ["core expectation", "column map expectation"],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_values.between"
    success_keys = (
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "allow_cross_type_comparisons",
        "mostly",
        "parse_strings_as_datetimes",
        "auto",
        "profiler_config",
    )

    min_estimato_parameter_builder_config: dict = {
        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
        "class_name": "MetricMultiBatchParameterBuilder",
        "name": "min_estimator",
        "metric_name": "column.min",
        "metric_domain_kwargs": DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        "metric_value_kwargs": None,
        "enforce_numeric_metric": True,
        "replace_nan_with_zero": True,
        "reduce_scalar_metric": True,
        "evaluation_parameter_builder_configs": None,
        "json_serialize": True,
    }
    max_estimato_parameter_builder_config: dict = {
        "module_name": "great_expectations.rule_based_profiler.parameter_builder",
        "class_name": "MetricMultiBatchParameterBuilder",
        "name": "max_estimator",
        "metric_name": "column.max",
        "metric_domain_kwargs": DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        "metric_value_kwargs": None,
        "enforce_numeric_metric": True,
        "replace_nan_with_zero": True,
        "reduce_scalar_metric": True,
        "evaluation_parameter_builder_configs": None,
        "json_serialize": True,
    }
    validation_parameter_builder_configs: List[dict] = [
        min_estimato_parameter_builder_config,
        max_estimato_parameter_builder_config,
    ]
    default_profiler_config: RuleBasedProfilerConfig = RuleBasedProfilerConfig(
        name="expect_column_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        class_name="RuleBasedProfilerConfig",
        module_name="great_expectations.rule_based_profiler",
        variables={
            "mostly": 1.0,
            "strict_min": False,
            "strict_max": False,
        },
        rules={
            "default_expect_column_values_to_be_between_rule": {
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                },
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_values_to_be_between",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        "validation_parameter_builder_configs": validation_parameter_builder_configs,
                        "column": f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
                        "min_value": "$parameter.min_estimator.value[-1]",
                        "max_value": "$parameter.max_estimator.value[-1]",
                        "mostly": f"{VARIABLES_KEY}mostly",
                        "strict_min": f"{VARIABLES_KEY}strict_min",
                        "strict_max": f"{VARIABLES_KEY}strict_max",
                        "meta": {
                            "profiler_details": {
                                "min_estimator": "$parameter.min_estimator.details",
                                "max_estimator": "$parameter.max_estimator.details",
                            },
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
        "min_value": None,
        "max_value": None,
        "strict_min": False,
        "strict_max": False,  # tolerance=1e-9,
        "parse_strings_as_datetimes": False,
        "allow_cross_type_comparisons": None,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
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

        # Setting up a configuration
        super().validate_configuration(configuration)

        min_val = None
        max_val = None
        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]
        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]
        assert (
            min_val is not None or max_val is not None
        ), "min_value and max_value cannot both be None"

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
                "mostly",
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
            "strict_min": {
                "schema": {"type": "boolean"},
                "value": params.get("strict_min"),
            },
            "strict_max": {
                "schema": {"type": "boolean"},
                "value": params.get("strict_max"),
            },
        }

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str += "may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            mostly_str = ""
            if params["mostly"] is not None:
                params_with_json_schema["mostly_pct"]["value"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                mostly_str = ", at least $mostly_pct % of the time"

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str += f"values must be {at_least_str} $min_value and {at_most_str} $max_value{mostly_str}."

            elif params["min_value"] is None:
                template_str += f"values must be {at_most_str} $max_value{mostly_str}."

            elif params["max_value"] is None:
                template_str += f"values must be {at_least_str} $min_value{mostly_str}."

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

    # NOTE: This method is a pretty good example of good usage of `params`.
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
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str += "may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            mostly_str = ""
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                mostly_str = ", at least $mostly_pct % of the time"

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str += f"values must be {at_least_str} $min_value and {at_most_str} $max_value{mostly_str}."

            elif params["min_value"] is None:
                template_str += f"values must be {at_most_str} $max_value{mostly_str}."

            elif params["max_value"] is None:
                template_str += f"values must be {at_least_str} $min_value{mostly_str}."

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
