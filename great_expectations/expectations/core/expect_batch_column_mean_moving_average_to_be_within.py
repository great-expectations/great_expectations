# TODO: <Alex>ALEX -- Remove unused imports after final implementation (including configuration validation and rendering) is completed.</Alex>
import itertools
from typing import Dict, List, Optional

import numpy as np

from great_expectations.core import (
    ExpectationConfiguration,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnExpectation,
)


class ExpectBatchColumnMeanMovingAverageToBeWithin(ColumnExpectation):
    """Expect the column mean moving average over specified window of Batch samples to be within given number of column standard deviations (inclusive).

    expect_batch_column_mean_moving_average_to_be_within is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    Args:
        column (str): \
            The column name.
        window_size (int): \
            The window size in units of Batch samples over which to compute moving average.
        num_std_devs (int): \
            The number of column standard deviations determining the boundaries within which column mean moving average must lie.

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
        * observed_value field in the result object is customized for this expectation to be a list of float values \
            representing the true mean of the column for each Batch sample

    # TODO: <Alex>ALEX</Alex>
    # See Also:
    #     [expect_column_median_to_be_between](https://greatexpectations.io/expectations/expect_column_median_to_be_between)
    #     [expect_column_stdev_to_be_between](https://greatexpectations.io/expectations/expect_column_stdev_to_be_between)
    # TODO: <Alex>ALEX</Alex>
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
    metric_dependencies = ("column.mean", "column.standard_deviation")
    success_keys = (
        "window_size",
        "num_std_devs",
        "mode",
    )

    # Default values
    default_kwarg_values = {
        "window_size": None,
        "num_std_devs": None,
        "mode": "valid",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column",
        "window_size",
        "num_std_devs",
        "valid",
    )

    kwargs_json_schema_base_properties = {
        "result_format": {
            "oneOf": [
                {"type": "null"},
                {
                    "type": "string",
                    "enum": ["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"],
                },
                {
                    "type": "object",
                    "properties": {
                        "result_format": {
                            "type": "string",
                            "enum": ["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"],
                        },
                        "partial_unexpected_count": {"type": "number"},
                    },
                },
            ],
            "default": "BASIC",
        },
        "include_config": {
            "oneOf": [{"type": "null"}, {"type": "boolean"}],
            "default": "true",
        },
        "catch_exceptions": {
            "oneOf": [{"type": "null"}, {"type": "boolean"}],
            "default": "false",
        },
        "meta": {"type": "object"},
    }

    kwargs_json_schema = {
        "type": "object",
        "properties": {
            **kwargs_json_schema_base_properties,
            "column": {"type": "string"},
            "window_size": {"oneOf": [{"type": "null"}, {"type": "number"}]},
            "num_std_devs": {"oneOf": [{"type": "null"}, {"type": "number"}]},
            "valid": {"oneOf": [{"type": "null"}, {"type": "string"}]},
        },
        "required": ["column"],
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.

        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """
        super().validate_configuration(configuration)
        # TODO: <Alex>ALEX</Alex>
        # self.validate_metric_value_between_configuration(configuration=configuration)
        # TODO: <Alex>ALEX</Alex>

    # TODO: <Alex>ALEX</Alex>
    # @classmethod
    # def _prescriptive_template(
    #     cls,
    #     renderer_configuration: RendererConfiguration,
    # ) -> RendererConfiguration:
    #     add_param_args = (
    #         ("column", RendererSchemaType.STRING),
    #         ("min_value", RendererSchemaType.NUMBER),
    #         ("max_value", RendererSchemaType.NUMBER),
    #         ("row_condition", RendererSchemaType.STRING),
    #         ("condition_parser", RendererSchemaType.STRING),
    #         ("strict_min", RendererSchemaType.BOOLEAN),
    #         ("strict_max", RendererSchemaType.BOOLEAN),
    #     )
    #     for name, schema_type in add_param_args:
    #         renderer_configuration.add_param(name=name, schema_type=schema_type)
    #
    #     params: RendererParams = renderer_configuration.params
    #
    #     if not params.min_value and not params.max_value:
    #         template_str = "mean may have any numerical value."
    #     else:
    #         at_least_str = "greater than or equal to"
    #         if params.strict_min:
    #             at_least_str: str = cls._get_strict_min_string(
    #                 renderer_configuration=renderer_configuration
    #             )
    #         at_most_str = "less than or equal to"
    #         if params.strict_max:
    #             at_most_str: str = cls._get_strict_max_string(
    #                 renderer_configuration=renderer_configuration
    #             )
    #
    #         if params.min_value and params.max_value:
    #             template_str = f"mean must be {at_least_str} $min_value and {at_most_str} $max_value."
    #         elif not params.min_value:
    #             template_str = f"mean must be {at_most_str} $max_value."
    #         else:
    #             template_str = f"mean must be {at_least_str} $min_value."
    #
    #     if renderer_configuration.include_column_name:
    #         template_str = f"$column {template_str}"
    #
    #     if params.row_condition:
    #         renderer_configuration = cls._add_row_condition_params(
    #             renderer_configuration=renderer_configuration
    #         )
    #         row_condition_str: str = cls._get_row_condition_string(
    #             renderer_configuration=renderer_configuration
    #         )
    #         template_str = f"{row_condition_str}, then {template_str}"
    #
    #     renderer_configuration.template_str = template_str
    #
    #     return renderer_configuration
    #
    # @classmethod
    # @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    # @render_evaluation_parameter_string
    # def _prescriptive_renderer(
    #     cls,
    #     configuration: Optional[ExpectationConfiguration] = None,
    #     result: Optional[ExpectationValidationResult] = None,
    #     runtime_configuration: Optional[dict] = None,
    #     **kwargs,
    # ):
    #
    #     runtime_configuration = runtime_configuration or {}
    #     include_column_name = (
    #         False if runtime_configuration.get("include_column_name") is False else True
    #     )
    #     styling = runtime_configuration.get("styling")
    #     params = substitute_none_for_missing(
    #         configuration.kwargs,
    #         [
    #             "column",
    #             "min_value",
    #             "max_value",
    #             "row_condition",
    #             "condition_parser",
    #             "strict_min",
    #             "strict_max",
    #         ],
    #     )
    #
    #     template_str = ""
    #     if (params["min_value"] is None) and (params["max_value"] is None):
    #         template_str = "mean may have any numerical value."
    #     else:
    #         at_least_str, at_most_str = handle_strict_min_max(params)
    #
    #         if params["min_value"] is not None and params["max_value"] is not None:
    #             template_str = f"mean must be {at_least_str} $min_value and {at_most_str} $max_value."
    #         elif params["min_value"] is None:
    #             template_str = f"mean must be {at_most_str} $max_value."
    #         elif params["max_value"] is None:
    #             template_str = f"mean must be {at_least_str} $min_value."
    #
    #     if include_column_name:
    #         template_str = f"$column {template_str}"
    #
    #     if params["row_condition"] is not None:
    #         (
    #             conditional_template_str,
    #             conditional_params,
    #         ) = parse_row_condition_string_pandas_engine(params["row_condition"])
    #         template_str = f"{conditional_template_str}, then {template_str}"
    #         params.update(conditional_params)
    #
    #     return [
    #         RenderedStringTemplateContent(
    #             **{
    #                 "content_block_type": "string_template",
    #                 "string_template": {
    #                     "template": template_str,
    #                     "params": params,
    #                     "styling": styling,
    #                 },
    #             }
    #         )
    #     ]
    #
    # @classmethod
    # @renderer(renderer_type=LegacyDescriptiveRendererType.STATS_TABLE_MEAN_ROW)
    # def _descriptive_stats_table_mean_row_renderer(
    #     cls,
    #     configuration: Optional[ExpectationConfiguration] = None,
    #     result: Optional[ExpectationValidationResult] = None,
    #     runtime_configuration: Optional[dict] = None,
    #     **kwargs,
    # ):
    #     assert result, "Must pass in result."
    #     return [
    #         {
    #             "content_block_type": "string_template",
    #             "string_template": {
    #                 "template": "Mean",
    #                 "tooltip": {"content": "expect_column_mean_to_be_between"},
    #             },
    #         },
    #         f"{result.result['observed_value']:.2f}",
    #     ]
    # TODO: <Alex>ALEX</Alex>

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        window_size: int = self.get_success_kwargs().get("window_size")
        num_std_devs: int = self.get_success_kwargs().get("num_std_devs")
        mode: str = self.get_success_kwargs().get("mode")

        multi_batch_column_mean_values: List[float] = [
            element[0] for element in metrics.get("column.mean").values()
        ]
        multi_batch_column_standard_deviation_values: List[float] = [
            element[0] for element in metrics.get("column.standard_deviation").values()
        ]
        max_amplitude: float = (
            num_std_devs * multi_batch_column_standard_deviation_values[-1]
        )

        moving_average_values: np.ndarray = (
            np.convolve(
                np.asarray(multi_batch_column_mean_values), np.ones(window_size), mode
            )
            / window_size
        )

        a: float
        b: float
        deltas: np.ndarray = np.asarray(
            [a - b for a, b in itertools.combinations(moving_average_values, 2)]
        )
        success: bool = np.logical_and(
            deltas >= (-max_amplitude), deltas <= max_amplitude
        ).all()

        return {
            "success": success,
            "result": {"observed_value": multi_batch_column_mean_values},
            "meta": {"column.standard_deviation": max_amplitude},
        }
