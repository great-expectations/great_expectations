from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Optional, Union

from great_expectations.compatibility.pydantic import root_validator
from great_expectations.core.evaluation_parameters import (
    EvaluationParameterDict,  # noqa: TCH001
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.render import LegacyRendererType, RenderedStringTemplateContent
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
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnValuesToBeBetween(ColumnMapExpectation):
    """Expect the column entries to be between a minimum value and a maximum value (inclusive).

    expect_column_values_to_be_between is a \
    [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations)

    Args:
        column (str): \
            The column name.
        min_value (comparable type or None): The minimum value for a column entry.
        max_value (comparable type or None): The maximum value for a column entry.
        strict_min (boolean): \
            If True, values must be strictly larger than min_value, default=False
        strict_max (boolean): \
            If True, values must be strictly smaller than max_value, default=False

    Keyword Args:
        mostly (None or a float between 0 and 1): \
            Successful if at least mostly fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly).

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
            modification. For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    Notes:
        * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
        * If min_value is None, then max_value is treated as an upper bound, and there is no minimum value checked.
        * If max_value is None, then min_value is treated as a lower bound, and there is no maximum value checked.

    See Also:
        [expect_column_value_lengths_to_be_between](https://greatexpectations.io/expectations/expect_column_value_lengths_to_be_between)
    """

    min_value: Union[int, float, EvaluationParameterDict, datetime, None] = None
    max_value: Union[int, float, EvaluationParameterDict, datetime, None] = None
    strict_min: bool = False
    strict_max: bool = False

    @classmethod
    @root_validator(pre=True)
    def check_min_val_or_max_val(cls, values: dict) -> dict:
        min_val = values.get("min_val")
        max_val = values.get("max_val")

        if min_val is None and max_val is None:
            raise ValueError("min_value and max_value cannot both be None")

        return values

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
        "mostly",
    )

    args_keys = (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
    )

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ):
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

        template_str = ""
        if not params.min_value and not params.max_value:
            template_str += "may have any numerical value."
        else:
            at_least_str = "greater than or equal to"
            if params.strict_min:
                at_least_str: str = cls._get_strict_min_string(
                    renderer_configuration=renderer_configuration
                )
            at_most_str = "less than or equal to"
            if params.strict_max:
                at_most_str: str = cls._get_strict_max_string(
                    renderer_configuration=renderer_configuration
                )

            if params.min_value and params.max_value:
                template_str += f"values must be {at_least_str} $min_value and {at_most_str} $max_value"
            elif not params.min_value:
                template_str += f"values must be {at_most_str} $max_value"
            else:
                template_str += f"values must be {at_least_str} $min_value"

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

    # NOTE: This method is a pretty good example of good usage of `params`.
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

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str += "may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            mostly_str = ""
            if params["mostly"] is not None and params["mostly"] < 1.0:  # noqa: PLR2004
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, no_scientific=True
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
