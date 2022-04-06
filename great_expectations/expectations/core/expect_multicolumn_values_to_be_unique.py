from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.util import (
    add_values_with_json_schema_from_list_in_params,
    render_evaluation_parameter_string,
)
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import (
    num_to_str,
    parse_row_condition_string_pandas_engine,
    substitute_none_for_missing,
)


class ExpectMulticolumnValuesToBeUnique(ColumnMapExpectation):
    """
     Expect that the columns are unique together, e.g. a multi-column primary key
     Note that all instances of any duplicates are considered failed

     For example::

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
             Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
         include_config (boolean): \
             If True, then include the expectation config as part of the result object. \
         catch_exceptions (boolean or None): \
             If True, then catch exceptions and include them as part of the result object. \
         meta (dict or None): \
             A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification.

     Returns:
         An ExpectationSuiteValidationResult
    """

    library_metadata = {
        "maturity": "production",
        "package": "great_expectations",
        "tags": ["core expectation", "multi-column expectation"],
        "contributors": [
            "@great_expectations",
        ],
        "requirements": [],
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
        params_with_json_schema = {
            "column_list": {
                "schema": {"type": "array"},
                "value": params.get("column_list"),
            },
            "ignore_row_if": {
                "schema": {"type": "string"},
                "value": params.get("ignore_row_if"),
            },
            "row_condition": {
                "schema": {"type": "string"},
                "value": params.get("row_condition"),
            },
            "condition_parser": {
                "schema": {"type": "string"},
                "value": params.get("condition_parser"),
            },
            "mostly": {
                "schema": {"type": "number"},
                "value": params.get("mostly"),
            },
            "mostly_pct": {
                "schema": {"type": "string"},
                "value": params.get("mostly_pct"),
            },
        }

        if params["mostly"] is not None and params["mostly"] < 1.0:
            params_with_json_schema["mostly_pct"]["value"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            template_str = (
                f"Values must be unique across columns, at least $mostly_pct % of the time: "
            )
        else:
            template_str = (
                f"Values must always be unique across columns: "
            )
        
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
            ) = parse_row_condition_string_pandas_engine(
                params["row_condition"], with_schema=True
            )
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params_with_json_schema.update(conditional_params)

        params_with_json_schema = add_values_with_json_schema_from_list_in_params(
            params=params,
            params_with_json_schema=params_with_json_schema,
            param_key_with_list="column_list",
        )
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
