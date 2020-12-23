from great_expectations.expectations.expectation import TableExpectation
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ExpectCompoundColumnsToBeUnique(TableExpectation):
    metric_dependencies = tuple()
    success_keys = (
        "column_list",
        "ignore_row_if",
    )
    default_kwarg_values = {
        "column_list": None,
        "ignore_row_if": "all_values_are_missing",
        "result_format": "BASIC",
        "include_config": True,
        "catch_exceptions": False,
    }

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

        template_str = (
            f"Values for given compound columns must be unique together{mostly_str}: "
        )
        for idx in range(len(params["column_list"]) - 1):
            template_str += "$column_list_" + str(idx) + ", "
            params["column_list_" + str(idx)] = params["column_list"][idx]

        last_idx = len(params["column_list"]) - 1
        template_str += "$column_list_" + str(last_idx)
        params["column_list_" + str(last_idx)] = params["column_list"][last_idx]

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
