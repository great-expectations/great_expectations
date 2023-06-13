from numbers import Number
from typing import TYPE_CHECKING, Dict, List, Optional, Union

import numpy as np

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.render import (
    AtomicDiagnosticRendererType,
    AtomicPrescriptiveRendererType,
    LegacyDescriptiveRendererType,
    LegacyDiagnosticRendererType,
    LegacyRendererType,
    RenderedAtomicContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    renderedAtomicValueSchema,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererSchema,
    RendererTableValue,
    RendererValueType,
)
from great_expectations.render.util import (
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
from great_expectations.util import isclose
from great_expectations.validator.validator import (
    ValidationDependencies,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnQuantileValuesToBeBetween(ColumnAggregateExpectation):
    # noinspection PyUnresolvedReferences
    """Expect the specific provided column quantiles to be between a minimum value and a maximum value.

    expect_column_quantile_values_to_be_between is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    For example:
    ::

        # my_df.my_col = [1,2,2,3,3,3,4]
        >>> my_df.expect_column_quantile_values_to_be_between(
            "my_col",
            {
                "quantiles": [0., 0.333, 0.6667, 1.],
                "value_ranges": [[0,1], [2,3], [3,4], [4,5]]
            }
        )
        {
          "success": True,
            "result": {
              "observed_value": {
                "quantiles: [0., 0.333, 0.6667, 1.],
                "values": [1, 2, 3, 4],
              }
              "element_count": 7,
              "missing_count": 0,
              "missing_percent": 0.0,
              "details": {
                "success_details": [true, true, true, true]
              }
            }
          }
        }

    expect_column_quantile_values_to_be_between can be computationally intensive for large datasets.

    Args:
        column (str): \
            The column name.
        quantile_ranges (dictionary with keys 'quantiles' and 'value_ranges'): \
            Key 'quantiles' is an increasingly ordered list of desired quantile values (floats). \
            Key 'value_ranges' is a list of 2-value lists that specify a lower and upper bound (inclusive) \
            for the corresponding quantile (with [min, max] ordering). The length of the 'quantiles' list \
            and the 'value_ranges' list must be equal.
        allow_relative_error (boolean or string): \
            Whether to allow relative error in quantile communications on backends that support or require it.

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
        * min_value and max_value are both inclusive.
        * If min_value is None, then max_value is treated as an upper bound only
        * If max_value is None, then min_value is treated as a lower bound only
        * details.success_details field in the result object is customized for this expectation

    See Also:
        [expect_column_min_to_be_between](https://greatexpectations.io/expectations/expect_column_min_to_be_between)
        [expect_column_max_to_be_between](https://greatexpectations.io/expectations/expect_column_max_to_be_between)
        [expect_column_median_to_be_between](https://greatexpectations.io/expectations/expect_column_median_to_be_between)
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

    metric_dependencies = ("column.quantile_values",)
    success_keys = (
        "quantile_ranges",
        "allow_relative_error",
        "auto",
        "profiler_config",
    )

    quantile_value_ranges_estimator_parameter_builder_config = ParameterBuilderConfig(
        module_name="great_expectations.rule_based_profiler.parameter_builder",
        class_name="NumericMetricRangeMultiBatchParameterBuilder",
        name="quantile_value_ranges_estimator",
        metric_name="column.quantile_values",
        metric_multi_batch_parameter_builder_name=None,
        metric_domain_kwargs=DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME,
        metric_value_kwargs={
            "quantiles": f"{VARIABLES_KEY}quantiles",
            "allow_relative_error": f"{VARIABLES_KEY}allow_relative_error",
        },
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
        quantile_value_ranges_estimator_parameter_builder_config,
    ]
    default_profiler_config = RuleBasedProfilerConfig(
        name="expect_column_quantile_values_to_be_between",  # Convention: use "expectation_type" as profiler name.
        config_version=1.0,
        variables={},
        rules={
            "default_expect_column_quantile_values_to_be_between_rule": {
                "variables": {
                    "quantiles": [
                        0.25,
                        0.5,
                        0.75,
                    ],
                    "allow_relative_error": "linear",
                    "estimator": "exact",
                    "include_estimator_samples_histogram_in_details": False,
                    "truncate_values": {
                        "lower_bound": None,
                        "upper_bound": None,
                    },
                    "round_decimals": None,
                },
                "domain_builder": {
                    "class_name": "ColumnDomainBuilder",
                    "module_name": "great_expectations.rule_based_profiler.domain_builder",
                },
                "expectation_configuration_builders": [
                    {
                        "expectation_type": "expect_column_quantile_values_to_be_between",
                        "class_name": "DefaultExpectationConfigurationBuilder",
                        "module_name": "great_expectations.rule_based_profiler.expectation_configuration_builder",
                        "validation_parameter_builder_configs": validation_parameter_builder_configs,
                        "column": f"{DOMAIN_KWARGS_PARAMETER_FULLY_QUALIFIED_NAME}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}column",
                        "quantile_ranges": {
                            "quantiles": f"{VARIABLES_KEY}quantiles",
                            "value_ranges": f"{PARAMETER_KEY}{quantile_value_ranges_estimator_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_VALUE_KEY}",
                        },
                        "allow_relative_error": f"{VARIABLES_KEY}allow_relative_error",
                        "meta": {
                            "profiler_details": f"{PARAMETER_KEY}{quantile_value_ranges_estimator_parameter_builder_config.name}{FULLY_QUALIFIED_PARAMETER_NAME_SEPARATOR_CHARACTER}{FULLY_QUALIFIED_PARAMETER_NAME_METADATA_KEY}",
                        },
                    }
                ],
            },
        },
    )

    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "quantile_ranges": None,
        "result_format": "BASIC",
        "allow_relative_error": False,
        "include_config": True,
        "catch_exceptions": False,
        "meta": None,
        "auto": False,
        "profiler_config": default_profiler_config,
    }
    args_keys = (
        "column",
        "quantile_ranges",
        "allow_relative_error",
    )

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates the configuration of an Expectation.

        For `expect_column_quantile_values_to_be_between` it is required that the `configuration.kwargs` contain a
        `quantile_ranges` key that is a `dict`. Also, `quantile_ranges` must contain a `value_ranges` key that is a
        list of ordered pairs, as well as a `quantiles` key that is a list of the same length as `values_ranges`.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: An `ExpectationConfiguration` to validate. If no configuration is provided, it will be pulled
                from the configuration attribute of the Expectation instance.

        Raises:
            InvalidExpectationConfigurationError: The configuration does not contain the values required by the
                Expectation.
            ValueError: `value_ranges` and `quantiles` are not the same length.
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        try:
            assert (
                "quantile_ranges" in configuration.kwargs
            ), "quantile_ranges must be provided"
            assert isinstance(
                configuration.kwargs["quantile_ranges"], dict
            ), "quantile_ranges should be a dictionary"

            assert all(
                True if None in x or x == sorted(x) else False
                for x in configuration.kwargs["quantile_ranges"]["value_ranges"]
            ), "quantile_ranges must consist of ordered pairs"

        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

        # Ensuring actual quantiles and their value ranges match up
        quantile_ranges = configuration.kwargs["quantile_ranges"]
        quantiles = quantile_ranges["quantiles"]
        quantile_value_ranges = quantile_ranges["value_ranges"]

        if len(quantiles) != len(quantile_value_ranges):
            raise ValueError(
                "quantile_values and quantiles must have the same number of elements"
            )

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        template_str = "quantiles must be within the following value ranges."

        if renderer_configuration.include_column_name:
            template_str = f"$column {template_str}"

        renderer_configuration.template_str = template_str

        quantiles: list = renderer_configuration.kwargs.get("quantile_ranges", {}).get(
            "quantiles", []
        )
        value_ranges: list = renderer_configuration.kwargs.get(
            "quantile_ranges", {}
        ).get("value_ranges", [])

        header_row = [
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Quantile"
            ),
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Min Value"
            ),
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Max Value"
            ),
        ]

        renderer_configuration.header_row = header_row

        table = []
        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}
        for quantile, value_range in zip(quantiles, value_ranges):
            quantile_string = quantile_strings.get(quantile, f"{quantile:3.2f}")
            value_range_lower: Union[Number, str] = (
                value_range[0] if value_range[0] else "Any"
            )
            value_rage_lower_type = (
                RendererValueType.NUMBER if value_range[0] else RendererValueType.STRING
            )
            value_range_upper: Union[Number, str] = (
                value_range[1] if value_range[1] else "Any"
            )
            value_range_upper_type = (
                RendererValueType.NUMBER if value_range[0] else RendererValueType.STRING
            )
            table.append(
                [
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.STRING),
                        value=quantile_string,
                    ),
                    RendererTableValue(
                        schema=RendererSchema(type=value_rage_lower_type),
                        value=value_range_lower,
                    ),
                    RendererTableValue(
                        schema=RendererSchema(type=value_range_upper_type),
                        value=value_range_upper,
                    ),
                ]
            )

        renderer_configuration.table = table

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=AtomicPrescriptiveRendererType.SUMMARY)
    @render_evaluation_parameter_string
    def _prescriptive_summary(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> RenderedAtomicContent:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        renderer_configuration = cls._prescriptive_template(
            renderer_configuration=renderer_configuration
        )
        header_row = [value.dict() for value in renderer_configuration.header_row]
        table = []
        for row in renderer_configuration.table:
            table.append([value.dict() for value in row])
        value_obj = renderedAtomicValueSchema.load(
            {
                "header": {
                    "schema": {"type": "StringValueType"},
                    "value": {
                        "template": renderer_configuration.template_str,
                        "params": renderer_configuration.params.dict(),
                    },
                },
                "header_row": header_row,
                "table": table,
                "meta_notes": renderer_configuration.meta_notes,
                "schema": {"type": "TableType"},
            }
        )
        return RenderedAtomicContent(
            name=AtomicPrescriptiveRendererType.SUMMARY,
            value=value_obj,
            value_type="TableType",
        )

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
        _ = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration["kwargs"],
            ["column", "quantile_ranges", "row_condition", "condition_parser"],
        )
        template_str = "quantiles must be within the following value ranges."

        if include_column_name:
            template_str = f"$column {template_str}"

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

        expectation_string_obj = {
            "content_block_type": "string_template",
            "string_template": {"template": template_str, "params": params},
        }

        quantiles = params["quantile_ranges"]["quantiles"]
        value_ranges = params["quantile_ranges"]["value_ranges"]

        table_header_row = ["Quantile", "Min Value", "Max Value"]
        table_rows = []

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for quantile, value_range in zip(quantiles, value_ranges):
            quantile_string = quantile_strings.get(quantile, f"{quantile:3.2f}")
            table_rows.append(
                [
                    quantile_string,
                    str(value_range[0]) if value_range[0] is not None else "Any",
                    str(value_range[1]) if value_range[1] is not None else "Any",
                ]
            )

        quantile_range_table = RenderedTableContent(
            **{
                "content_block_type": "table",
                "header_row": table_header_row,
                "table": table_rows,
                "styling": {
                    "body": {
                        "classes": [
                            "table",
                            "table-sm",
                            "table-unbordered",
                            "col-4",
                            "mt-2",
                        ],
                    },
                    "parent": {"styles": {"list-style-type": "none"}},
                },
            }
        )

        return [expectation_string_obj, quantile_range_table]

    @classmethod
    @renderer(renderer_type=LegacyDiagnosticRendererType.OBSERVED_VALUE)
    def _diagnostic_observed_value_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        if result.result is None or result.result.get("observed_value") is None:
            return "--"

        quantiles = result.result.get("observed_value", {}).get("quantiles", [])
        value_ranges = result.result.get("observed_value", {}).get("values", [])

        table_header_row = ["Quantile", "Value"]
        table_rows = []

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile)
            table_rows.append(
                [
                    quantile_string if quantile_string else f"{quantile:3.2f}",
                    str(value_ranges[idx]),
                ]
            )

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header_row": table_header_row,
                "table": table_rows,
                "styling": {
                    "body": {
                        "classes": ["table", "table-sm", "table-unbordered", "col-4"],
                    }
                },
            }
        )

    @classmethod
    def _atomic_diagnostic_observed_value_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ):
        if (
            renderer_configuration.result.result is None
            or renderer_configuration.result.result.get("observed_value") is None
        ):
            renderer_configuration.template_str = "--"
            return renderer_configuration

        quantiles = renderer_configuration.result.result.get("observed_value", {}).get(
            "quantiles", []
        )
        value_ranges = renderer_configuration.result.result.get(
            "observed_value", {}
        ).get("values", [])

        header_row = [
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Quantile"
            ),
            RendererTableValue(
                schema=RendererSchema(type=RendererValueType.STRING), value="Value"
            ),
        ]

        table = []
        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}
        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile) or f"{quantile:3.2f}"
            table.append(
                [
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.STRING),
                        value=quantile_string,
                    ),
                    RendererTableValue(
                        schema=RendererSchema(type=RendererValueType.NUMBER),
                        value=value_ranges[idx],
                    ),
                ]
            )

        renderer_configuration.header_row = header_row
        renderer_configuration.table = table

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=AtomicDiagnosticRendererType.OBSERVED_VALUE)
    def _atomic_diagnostic_observed_value(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ):
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        renderer_configuration = cls._atomic_diagnostic_observed_value_template(
            renderer_configuration=renderer_configuration,
        )
        if renderer_configuration.template_str:
            value_obj = renderedAtomicValueSchema.load(
                {
                    "template": renderer_configuration.template_str,
                    "params": {},
                    "schema": {"type": "StringValueType"},
                }
            )
            return RenderedAtomicContent(
                name="atomic.diagnostic.observed_value",
                value=value_obj,
                value_type="StringValueType",
            )
        else:
            header_row = [value.dict() for value in renderer_configuration.header_row]
            table = []
            for row in renderer_configuration.table:
                table.append([value.dict() for value in row])
            value_obj = renderedAtomicValueSchema.load(
                {
                    "header_row": header_row,
                    "table": table,
                    "schema": {"type": "TableType"},
                }
            )
            return RenderedAtomicContent(
                name=AtomicDiagnosticRendererType.OBSERVED_VALUE,
                value=value_obj,
                value_type="TableType",
            )

    @classmethod
    @renderer(renderer_type=LegacyDescriptiveRendererType.QUANTILE_TABLE)
    def _descriptive_quantile_table_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        table_rows = []
        quantiles = result.result["observed_value"]["quantiles"]
        quantile_ranges = result.result["observed_value"]["values"]

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for idx, quantile in enumerate(quantiles):
            quantile_string = quantile_strings.get(quantile)
            table_rows.append(
                [
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": quantile_string
                            if quantile_string
                            else f"{quantile:3.2f}",
                            "tooltip": {
                                "content": "expect_column_quantile_values_to_be_between \n expect_column_median_to_be_between"
                                if quantile == 0.50  # noqa: PLR2004
                                else "expect_column_quantile_values_to_be_between"
                            },
                        },
                    },
                    quantile_ranges[idx],
                ]
            )

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {"template": "Quantiles", "tag": "h6"},
                    }
                ),
                "table": table_rows,
                "styling": {
                    "classes": ["col-3", "mt-1", "pl-1", "pr-1"],
                    "body": {
                        "classes": ["table", "table-sm", "table-unbordered"],
                    },
                },
            }
        )

    def get_validation_dependencies(
        self,
        configuration: Optional[ExpectationConfiguration] = None,
        execution_engine: Optional[ExecutionEngine] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> ValidationDependencies:
        validation_dependencies: ValidationDependencies = (
            super().get_validation_dependencies(
                configuration, execution_engine, runtime_configuration
            )
        )
        # column.quantile_values expects a "quantiles" key
        validation_dependencies.get_metric_configuration(
            metric_name="column.quantile_values"
        ).metric_value_kwargs["quantiles"] = configuration.kwargs["quantile_ranges"][
            "quantiles"
        ]
        return validation_dependencies

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        quantile_vals = metrics.get("column.quantile_values")
        quantile_ranges = configuration.kwargs.get("quantile_ranges")
        quantiles = quantile_ranges["quantiles"]
        quantile_value_ranges = quantile_ranges["value_ranges"]

        # We explicitly allow "None" to be interpreted as +/- infinity
        comparison_quantile_ranges = [
            [
                -np.inf if lower_bound is None else lower_bound,
                np.inf if upper_bound is None else upper_bound,
            ]
            for (lower_bound, upper_bound) in quantile_value_ranges
        ]
        success_details = [
            isclose(
                operand_a=quantile_vals[idx],
                operand_b=range_[0],
                rtol=1.0e-4,
            )
            or isclose(
                operand_a=quantile_vals[idx],
                operand_b=range_[1],
                rtol=1.0e-4,
            )
            or range_[0] <= quantile_vals[idx] <= range_[1]
            for idx, range_ in enumerate(comparison_quantile_ranges)
        ]

        return {
            "success": np.all(success_details),
            "result": {
                "observed_value": {"quantiles": quantiles, "values": quantile_vals},
                "details": {"success_details": success_details},
            },
        }
