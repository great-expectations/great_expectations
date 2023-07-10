from typing import TYPE_CHECKING, Dict, List, Optional

import altair as alt
import pandas as pd

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnAggregateExpectation,
    InvalidExpectationConfigurationError,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics.util import parse_value_set
from great_expectations.render import (
    LegacyDescriptiveRendererType,
    LegacyRendererType,
    RenderedGraphContent,
    RenderedStringTemplateContent,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.renderer_configuration import (
    RendererConfiguration,
    RendererValueType,
)
from great_expectations.render.util import (
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnDistinctValuesToBeInSet(ColumnAggregateExpectation):
    """Expect the set of distinct column values to be contained by a given set.

    expect_column_distinct_values_to_be_in_set is a \
    [Column Aggregate Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations).

    The success value for this expectation will match that of \
    [expect_column_values_to_be_in_set](https://greatexpectations.io/expectations/expect_column_values_to_be_in_set).

    For example:
    ::

        # my_df.my_col = [1,2,2,3,3,3]
        >>> my_df.expect_column_distinct_values_to_be_in_set(
                "my_col",
                [2, 3, 4]
            )
        {
            "success": false
            "result": {
                "observed_value": [1,2,3],
                "details": {
                    "value_counts": [
                        {
                            "value": 1,
                            "count": 1
                        },
                        {
                            "value": 2,
                            "count": 1
                        },
                        {
                            "value": 3,
                            "count": 1
                        }
                    ]
                }
            }
        }

    Args:
        column (str): \
            The column name.
        value_set (set-like): \
            A set of objects used for comparison.

    Keyword Args:
        parse_strings_as_datetimes (boolean or None): If True values provided in value_set will be parsed \
        as datetimes before making comparisons.

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
        [expect_column_distinct_values_to_contain_set](https://greatexpectations.io/expectations/expect_column_distinct_values_to_contain_set)
        [expect_column_distinct_values_to_equal_set](https://greatexpectations.io/expectations/expect_column_distinct_values_to_equal_set)
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
    metric_dependencies = ("column.value_counts",)
    success_keys = (
        "value_set",
        "parse_strings_as_datetimes",
    )

    # Default values
    default_kwarg_values = {
        "value_set": None,
        "parse_strings_as_datetimes": False,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = (
        "column",
        "value_set",
    )

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column", RendererValueType.STRING),
            ("value_set", RendererValueType.ARRAY),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if not params.value_set or len(params.value_set.value) == 0:
            if renderer_configuration.include_column_name:
                template_str = "$column distinct values must belong to this set: [ ]"
            else:
                template_str = "distinct values must belong to a set, but that set is not specified."
        else:
            array_param_name = "value_set"
            param_prefix = "v__"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            value_set_str: str = cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )

            if renderer_configuration.include_column_name:
                template_str = (
                    f"$column distinct values must belong to this set: {value_set_str}."
                )
            else:
                template_str = (
                    f"distinct values must belong to this set: {value_set_str}."
                )

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
    ) -> List[RenderedStringTemplateContent]:
        renderer_configuration: RendererConfiguration = RendererConfiguration(
            configuration=configuration,
            result=result,
            runtime_configuration=runtime_configuration,
        )
        params = substitute_none_for_missing(
            renderer_configuration.kwargs,
            ["column", "value_set", "row_condition", "condition_parser"],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            if renderer_configuration.include_column_name:
                template_str = "$column distinct values must belong to this set: [ ]"
            else:
                template_str = "distinct values must belong to a set, but that set is not specified."

        else:
            for i, v in enumerate(params["value_set"]):
                params[f"v__{str(i)}"] = v
            values_string = " ".join(
                [f"$v__{str(i)}" for i, v in enumerate(params["value_set"])]
            )

            if renderer_configuration.include_column_name:
                template_str = (
                    f"$column distinct values must belong to this set: {values_string}."
                )
            else:
                template_str = (
                    f"distinct values must belong to this set: {values_string}."
                )

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = f"{conditional_template_str}, then {template_str}"
            params.update(conditional_params)

        styling = (
            runtime_configuration.get("styling", {}) if runtime_configuration else {}
        )

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
    @renderer(renderer_type=LegacyDescriptiveRendererType.VALUE_COUNTS_BAR_CHART)
    def _descriptive_value_counts_bar_chart_renderer(
        cls,
        configuration: Optional[ExpectationConfiguration] = None,
        result: Optional[ExpectationValidationResult] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> Optional[RenderedGraphContent]:
        assert result, "Must pass in result."
        value_count_dicts = result.result["details"]["value_counts"]
        if isinstance(value_count_dicts, pd.Series):
            values = value_count_dicts.index.tolist()
            counts = value_count_dicts.tolist()
        else:
            values = [
                value_count_dict["value"] for value_count_dict in value_count_dicts
            ]
            counts = [
                value_count_dict["count"] for value_count_dict in value_count_dicts
            ]

        df = pd.DataFrame(
            {
                "value": values,
                "count": counts,
            }
        )

        if len(values) > 60:  # noqa: PLR2004
            return None
        else:
            chart_pixel_width = (len(values) / 60.0) * 500
            if chart_pixel_width < 250:  # noqa: PLR2004
                chart_pixel_width = 250
            chart_container_col_width = round((len(values) / 60.0) * 6)
            if chart_container_col_width < 4:  # noqa: PLR2004
                chart_container_col_width = 4
            elif chart_container_col_width >= 5:  # noqa: PLR2004
                chart_container_col_width = 6
            elif chart_container_col_width >= 4:  # noqa: PLR2004
                chart_container_col_width = 5

        mark_bar_args = {}
        if len(values) == 1:
            mark_bar_args["size"] = 20

        bars = (
            alt.Chart(df)
            .mark_bar(**mark_bar_args)
            .encode(y="count:Q", x="value:O", tooltip=["value", "count"])
            .properties(height=400, width=chart_pixel_width, autosize="fit")
        )

        chart = bars.to_json()

        new_block = RenderedGraphContent(
            **{
                "content_block_type": "graph",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Value Counts",
                            "tooltip": {
                                "content": "expect_column_distinct_values_to_be_in_set"
                            },
                            "tag": "h6",
                        },
                    }
                ),
                "graph": chart,
                "styling": {
                    "classes": [f"col-{str(chart_container_col_width)}", "mt-1"],
                },
            }
        )

        return new_block

    @public_api
    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration] = None
    ) -> None:
        """Validates configuration for the Expectation.

        For `expect_column_distinct_values_to_be_in_set` we require that the `configuraton.kwargs` contain
        a `value_set` key that is either a `list`, `set`, or `dict`.

        The configuration will also be validated using each of the `validate_configuration` methods in its Expectation
        superclass hierarchy.

        Args:
            configuration: The ExpectationConfiguration to be validated.

        Raises:
            InvalidExpectationConfigurationError: The configuration does not contain the values required by the
                Expectation.
        """
        super().validate_configuration(configuration)
        configuration = configuration or self.configuration
        try:
            assert "value_set" in configuration.kwargs, "value_set is required"
            assert (
                isinstance(configuration.kwargs["value_set"], (list, set, dict))
                or configuration.kwargs["value_set"] is None
            ), "value_set must be a list, set, or None"
            if isinstance(configuration.kwargs["value_set"], dict):
                assert (
                    "$PARAMETER" in configuration.kwargs["value_set"]
                ), 'Evaluation Parameter dict for value_set kwarg must have "$PARAMETER" key'
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: Optional[dict] = None,
        execution_engine: Optional[ExecutionEngine] = None,
    ):
        parse_strings_as_datetimes = self.get_success_kwargs(configuration).get(
            "parse_strings_as_datetimes"
        )
        observed_value_counts = metrics.get("column.value_counts")
        observed_value_set = set(observed_value_counts.index)
        value_set = self.get_success_kwargs(configuration).get("value_set") or []

        if parse_strings_as_datetimes:
            parsed_value_set = parse_value_set(value_set)
        else:
            parsed_value_set = value_set

        expected_value_set = set(parsed_value_set)

        if not expected_value_set:
            success = True
        else:
            success = observed_value_set.issubset(expected_value_set)

        return {
            "success": success,
            "result": {
                "observed_value": sorted(list(observed_value_set)),
                "details": {"value_counts": observed_value_counts},
            },
        }
