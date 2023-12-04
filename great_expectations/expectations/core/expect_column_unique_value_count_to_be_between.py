from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Dict, Optional, Union

from great_expectations.core.evaluation_parameters import (
    EvaluationParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.render import (
    LegacyDescriptiveRendererType,
    LegacyRendererType,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    handle_strict_min_max,
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.core import (
        ExpectationConfiguration,
        ExpectationValidationResult,
    )
    from great_expectations.execution_engine import ExecutionEngine
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnUniqueValueCountToBeBetween(ColumnAggregateExpectation):
    """Expect the number of unique values to be between a minimum value and a maximum value.

    expect_column_unique_value_count_to_be_between is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    Args:
        column (str): \
            The column name.
        min_value (int or None): \
            The minimum number of unique values allowed.
        max_value (int or None): \
            The maximum number of unique values allowed.

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
        * If min_value is None, then max_value is treated as an upper bound
        * If max_value is None, then min_value is treated as a lower bound
        * observed_value field in the result object is customized for this expectation to be an int \
          representing the number of unique values the column

    See Also:
        [expect_column_proportion_of_unique_values_to_be_between](https://greatexpectations.io/expectations/expect_column_proportion_of_unique_values_to_be_between)
    """

    min_value: Union[float, EvaluationParameterDict, datetime, None] = None
    max_value: Union[float, EvaluationParameterDict, datetime, None] = None

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
    metric_dependencies = ("column.distinct_values.count",)
    success_keys = (
        "min_value",
        "max_value",
    )

    # Default values
    default_kwarg_values = {
        "row_condition": None,
        "condition_parser": None,
        "min_value": None,
        "max_value": None,
        "result_format": "BASIC",
        "catch_exceptions": False,
    }
    args_keys = (
        "column",
        "min_value",
        "max_value",
    )

    """ A Column Aggregate Metric Decorator for the Unique Value Count"""

    @classmethod
    def _prescriptive_template(  # noqa: PLR0912
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("min_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("max_value", [RendererValueType.NUMBER, RendererValueType.DATETIME]),
            ("mostly", RendererValueType.NUMBER),
            ("strict_min", RendererValueType.BOOLEAN),
            ("strict_max", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.min_value and not params.max_value:
            template_str = "may have any number of unique values."
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

            if params.mostly and params.mostly.value < 1.0:  # noqa: PLR2004
                renderer_configuration = cls._add_mostly_pct_param(
                    renderer_configuration=renderer_configuration
                )
                if not params.min_value:
                    template_str = f"must have {at_most_str} $max_value unique values, at least $mostly_pct % of the time."
                elif not params.max_value:
                    template_str = f"must have {at_least_str} $min_value unique values, at least $mostly_pct % of the time."
                else:
                    template_str = f"must have {at_least_str} $min_value and {at_most_str} $max_value unique values, at least $mostly_pct % of the time."
            else:  # noqa: PLR5501
                if not params.min_value:
                    template_str = f"must have {at_most_str} $max_value unique values."
                elif not params.max_value:
                    template_str = f"must have {at_least_str} $min_value unique values."
                else:
                    template_str = f"must have {at_least_str} $min_value and {at_most_str} $max_value unique values."

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
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        at_least_str, at_most_str = handle_strict_min_max(params)

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "may have any number of unique values."
        else:  # noqa: PLR5501
            if params["mostly"] is not None and params["mostly"] < 1.0:  # noqa: PLR2004
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                if params["min_value"] is None:
                    template_str = f"must have {at_most_str} $max_value unique values, at least $mostly_pct % of the time."
                elif params["max_value"] is None:
                    template_str = f"must have {at_least_str} $min_value unique values, at least $mostly_pct % of the time."
                else:
                    template_str = f"must have {at_least_str} $min_value and {at_most_str} $max_value unique values, at least $mostly_pct % of the time."
            else:  # noqa: PLR5501
                if params["min_value"] is None:
                    template_str = f"must have {at_most_str} $max_value unique values."
                elif params["max_value"] is None:
                    template_str = f"must have {at_least_str} $min_value unique values."
                else:
                    template_str = f"must have {at_least_str} $min_value and {at_most_str} $max_value unique values."

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
    @renderer(
        renderer_type=LegacyDescriptiveRendererType.COLUMN_PROPERTIES_TABLE_DISTINCT_COUNT_ROW
    )
    def _descriptive_column_properties_table_distinct_count_row_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
        **kwargs,
    ):
        assert result, "Must pass in result."
        observed_value = result.result["observed_value"]
        template_string_object = RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "Distinct (n)",
                    "tooltip": {
                        "content": "expect_column_unique_value_count_to_be_between"
                    },
                },
            }
        )
        if not observed_value:
            return [template_string_object, "--"]
        else:
            return [template_string_object, observed_value]

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        return self._validate_metric_value_between(
            metric_name="column.distinct_values.count",
            configuration=configuration,
            metrics=metrics,
            runtime_configuration=runtime_configuration,
            execution_engine=execution_engine,
        )
