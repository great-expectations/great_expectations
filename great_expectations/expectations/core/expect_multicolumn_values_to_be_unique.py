from typing import TYPE_CHECKING, Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
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
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)

if TYPE_CHECKING:
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectMulticolumnValuesToBeUnique(ColumnMapExpectation):
    """Expect that the columns are unique together (e.g. a multi-column primary key)

     Note that all instances of any duplicates are considered failed

     expect_multicolumnvalues_to_be_unique is a \
     [Column Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations).

     For example:
     ::

         A B C
         1 1 2 Fail
         1 2 3 Pass
         1 1 2 Fail
         2 2 2 Pass
         3 2 3 Pass

     Args:
         column_list (tuple or list): The column names to evaluate

     Keyword Args:
         ignore_row_if (str): "all_values_are_missing", "any_value_is_missing", "never"

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
        "tags": [
            "core expectation",
            "multi-column expectation",
            "column map expectation",
        ],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    metric_dependencies = tuple()
    success_keys = (
        "column_list",
        "ignore_row_if",
        "mostly",
    )
    default_kwarg_values = {
        "column_list": None,
        "ignore_row_if": "all_values_are_missing",
        "mostly": 1,
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }
    args_keys = ("column_list",)

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column_list", RendererValueType.ARRAY),
            ("mostly", RendererValueType.NUMBER),
            ("ignore_row_if", RendererValueType.STRING),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params

        if params.mostly and params.mostly.value < 1.0:  # noqa: PLR2004
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            template_str = "Values must be unique across columns, at least $mostly_pct % of the time: "
        else:
            template_str = "Values must always be unique across columns: "

        if params.column_list:
            array_param_name = "column_list"
            param_prefix = "column_list_"
            renderer_configuration = cls._add_array_params(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
            )
            template_str += cls._get_array_string(
                array_param_name=array_param_name,
                param_prefix=param_prefix,
                renderer_configuration=renderer_configuration,
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
        **kwargs,
    ):
        runtime_configuration = runtime_configuration or {}
        _ = False if runtime_configuration.get("include_column_name") is False else True
        styling = runtime_configuration.get("styling")

        # NOTE: This expectation is deprecated, please use
        # expect_select_column_values_to_be_unique_within_record instead.

        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )

        template_str = f"Values must always be unique across columns{mostly_str}: "
        for idx in range(len(params["column_list"]) - 1):
            template_str += f"$column_list_{str(idx)}, "
            params[f"column_list_{str(idx)}"] = params["column_list"][idx]

        last_idx = len(params["column_list"]) - 1
        template_str += f"$column_list_{str(last_idx)}"
        params[f"column_list_{str(last_idx)}"] = params["column_list"][last_idx]

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
