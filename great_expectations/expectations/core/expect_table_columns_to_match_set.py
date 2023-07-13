from typing import TYPE_CHECKING, Dict, List, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    BatchExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import substitute_none_for_missing
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

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectTableColumnsToMatchSet(BatchExpectation):
    """Expect the columns to match an unordered set.

    expect_table_columns_to_match_set is a \
    [Table Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_table_expectations).

    Args:
        column_set (list of str): \
            The column names, in any order.
        exact_match (boolean): \
            Whether the list of columns must exactly match the observed columns.

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
    success_keys = (
        "column_set",
        "exact_match",
        "auto",
        "profiler_config",
    )

    mean_table_columns_set_match_multi_batch_parameter_builder_config = (
        ParameterBuilderConfig(
            module_name="great_expectations.rule_based_profiler.parameter_builder",
            class_name="MeanTableColumnsSetMatchMultiBatchParameterBuilder",
            name="column_names_set_estimator",
            metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
            metric_value_kwargs=None,
            evaluation_parameter_builder_configs=None,
        )
    )
    validation_parameter_builder_configs: List[ParameterBuilderConfig] = [
        mean_table_columns_set_match_multi_batch_parameter_builder_config,
    ]
    default_profiler_config = RuleBasedProfilerConfig(
        name="expect_table_columns_to_match_set",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        variables={},
        rules={
            "expect_table_columns_to_match_set": {
                "variables": {
                    "exact_match": None,
                    "success_ratio": 1.0,
                },
                "domain_builder": {
                    "class_name": "TableDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                },
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_table_columns_to_match_set",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        "validation_parameter_builder_configs": validation_parameter_builder_configs,
                        "condition": f"{PARAMETER_KEY}{mean_table_columns_set_match_multi_batch_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}success_ratio >= {VARIABLES_KEY}success_ratio",
                        "column_set": f"{PARAMETER_KEY}{mean_table_columns_set_match_multi_batch_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
                        "exact_match": f"{VARIABLES_KEY}exact_match",
                        "meta": {
                            "profiler_details": f"{PARAMETER_KEY}{mean_table_columns_set_match_multi_batch_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                        },
                    },
                ],
            },
        },
    )

    default_kwarg_values = {
        "column_set": None,
        "exact_match": True,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
        "auto": False,
        "profiler_config": default_profiler_config,
    }
    args_keys = (
        "column_set",
        "exact_match",
    )

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration of an Expectation.

        For this expectation it is required that:

        - `column_set` has been provided.
        - `column_set` is one of the following types: `list`, `set`, or `None`
        - If `column_set` is a `dict`, it is assumed to be an Evaluation Parameter, and therefore the
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
            assert "column_set" in configuration.kwargs, "column_set is required"
            assert (
                isinstance(configuration.kwargs["column_set"], (list, set, dict))
                or configuration.kwargs["column_set"] is None
            ), "column_set must be a list, set, or None"
            if isinstance(configuration.kwargs["column_set"], dict):
                assert (
                    "$PARAMETER" in configuration.kwargs["column_set"]
                ), 'Evaluation Parameter dict for column_set kwarg must have "$PARAMETER" key.'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column_set", RendererValueType.ARRAY),
            ("exact_match", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.column_set:
            template_str = "Must specify a set or list of columns."
        else:
            array_param_name = "column_set"
            param_prefix = "column_set_"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            column_set_str: str = cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

            exact_match_str = (
                "exactly"
                if params.exact_match and params.exact_match.value is True
                else "at least"
            )

            template_str = f"Must have {exact_match_str} these columns (in any order): {column_set_str}"

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
            configuration.kwargs, ["column_set", "exact_match"]
        )

        if params["column_set"] is None:
            template_str = "Must specify a set or list of columns."

        else:
            # standardize order of the set for output
            params["column_list"] = list(params["column_set"])

            column_list_template_str = ", ".join(
                [f"$column_list_{idx}" for idx in range(len(params["column_list"]))]
            )

            exact_match_str = "exactly" if params["exact_match"] is True else "at least"

            template_str = f"Must have {exact_match_str} these columns (in any order): {column_list_template_str}"

            for idx in range(len(params["column_list"])):
                params[f"column_list_{str(idx)}"] = params["column_list"][idx]

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
        expected_column_set = self.get_success_kwargs(configuration).get("column_set")
        expected_column_set = (
            set(expected_column_set) if expected_column_set is not None else set()
        )
        actual_column_list = metrics.get("table.columns")
        actual_column_set = set(actual_column_list)
        exact_match = self.get_success_kwargs(configuration).get("exact_match")

        if (
            (expected_column_set is None) and (exact_match is not True)
        ) or actual_column_set == expected_column_set:
            return {"success": True, "result": {"observed_value": actual_column_list}}
        else:
            # Convert to lists and sort to lock order for testing and output rendering
            # unexpected_list contains items from the dataset columns that are not in expected_column_set
            unexpected_list = sorted(list(actual_column_set - expected_column_set))
            # missing_list contains items from expected_column_set that are not in the dataset columns
            missing_list = sorted(list(expected_column_set - actual_column_set))
            # observed_value contains items that are in the dataset columns
            observed_value = sorted(actual_column_list)

            mismatched = {}
            if len(unexpected_list) > 0:
                mismatched["unexpected"] = unexpected_list
            if len(missing_list) > 0:
                mismatched["missing"] = missing_list

            result = {
                "observed_value": observed_value,
                "details": {"mismatched": mismatched},
            }

            return_success = {
                "success": True,
                "result": result,
            }
            return_failed = {
                "success": False,
                "result": result,
            }

            if exact_match:
                return return_failed
            else:
                # Failed if there are items in the missing list (but OK to have unexpected_list)
                if len(missing_list) > 0:  # noqa: PLR5501
                    return return_failed
                # Passed if there are no items in the missing list
                else:
                    return return_success
