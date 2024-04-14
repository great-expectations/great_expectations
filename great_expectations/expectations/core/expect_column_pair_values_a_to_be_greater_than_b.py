from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

from great_expectations.expectations.expectation import (
    ColumnPairMapExpectation,
    render_suite_parameter_string,
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
    from great_expectations.core import (
        ExpectationValidationResult,
    )
    from great_expectations.expectations.expectation_configuration import (
        ExpectationConfiguration,
    )
    from great_expectations.render.renderer_configuration import AddParamArgs


class ExpectColumnPairValuesAToBeGreaterThanB(ColumnPairMapExpectation):
    """Expect the values in column A to be greater than column B.

    expect_column_pair_values_a_to_be_greater_than_b is a \
    [Column Pair Map Expectation](https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_pair_map_expectations).

    Column Pair Map Expectations are evaluated for a pair of columns and ask a yes/no question about the row-wise relationship between those two columns.
    Based on the result, they then calculate the percentage of rows that gave a positive answer.
    If the percentage is high enough, the Expectation considers that data valid.

    Args:
        column_A (str): The first column name
        column_B (str): The second column name
        or_equal (boolean or None): If True, then values can be equal, not strictly greater

    Other Parameters:
        ignore_row_if (str): \
            "both_values_are_missing", "either_value_is_missing", "neither" \
            If specified, sets the condition on which a given row is to be ignored. Default "neither".
        mostly (None or a float between 0 and 1): \
            Successful if at least `mostly` fraction of values match the expectation. \
            For more detail, see [mostly](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#mostly). Default 1.
        result_format (str or None): \
            Which output mode to use: BOOLEAN_ONLY, BASIC, COMPLETE, or SUMMARY. \
            For more detail, see [result_format](https://docs.greatexpectations.io/docs/reference/expectations/result_format).
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see [catch_exceptions](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#catch_exceptions).
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without modification. \
            For more detail, see [meta](https://docs.greatexpectations.io/docs/reference/expectations/standard_arguments/#meta).

    Returns:
        An [ExpectationSuiteValidationResult](https://docs.greatexpectations.io/docs/terms/validation_result)

        Exact fields vary depending on the values passed to result_format, catch_exceptions, and meta.

    Supported Datasources:
        [Snowflake](https://docs.greatexpectations.io/docs/application_integration_support/)
        [PostgreSQL](https://docs.greatexpectations.io/docs/application_integration_support/)

    Data Quality Category:
        Distribution

    Example Data:
                test 	test2
            0 	2       1
            1 	2       2
            2 	4   	4

    Code Examples:
        Passing Case:
            Input:
                ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="test",
                    column_B="test2",
                    or_equal=True
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 3,
                    "unexpected_count": 0,
                    "unexpected_percent": 0.0,
                    "partial_unexpected_list": [],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 0.0,
                    "unexpected_percent_nonmissing": 0.0
                  },
                  "meta": {},
                  "success": true
                }

        Failing Case:
            Input:
                ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="test2",
                    column_B="test"
            )

            Output:
                {
                  "exception_info": {
                    "raised_exception": false,
                    "exception_traceback": null,
                    "exception_message": null
                  },
                  "result": {
                    "element_count": 3,
                    "unexpected_count": 3,
                    "unexpected_percent": 100.0,
                    "partial_unexpected_list": [
                      [
                        1,
                        2
                      ],
                      [
                        2,
                        2
                      ],
                      [
                        4,
                        4
                      ]
                    ],
                    "missing_count": 0,
                    "missing_percent": 0.0,
                    "unexpected_percent_total": 100.0,
                    "unexpected_percent_nonmissing": 100.0
                  },
                  "meta": {},
                  "success": false
                }
    """  # noqa: E501

    or_equal: Union[bool, None] = None
    ignore_row_if: Literal["both_values_are_missing", "either_value_is_missing", "neither"] = (
        "both_values_are_missing"
    )

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "production",
        "tags": [
            "core expectation",
            "column pair map expectation",
        ],
        "contributors": ["@great_expectations"],
        "requirements": [],
        "has_full_test_suite": True,
        "manually_reviewed_code": True,
    }

    map_metric = "column_pair_values.a_greater_than_b"
    success_keys = (
        "column_A",
        "column_B",
        "ignore_row_if",
        "or_equal",
        "mostly",
    )
    args_keys = (
        "column_A",
        "column_B",
        "or_equal",
    )

    @classmethod
    def _prescriptive_template(
        cls,
        renderer_configuration: RendererConfiguration,
    ) -> RendererConfiguration:
        add_param_args: AddParamArgs = (
            ("column_A", RendererValueType.STRING),
            ("column_B", RendererValueType.STRING),
            ("ignore_row_if", RendererValueType.STRING),
            ("mostly", RendererValueType.NUMBER),
            ("or_equal", RendererValueType.BOOLEAN),
        )
        for name, param_type in add_param_args:
            renderer_configuration.add_param(name=name, param_type=param_type)

        params = renderer_configuration.params
        template_str = ""

        if not params.column_A or not params.column_B:
            template_str += "$column has a bogus `expect_column_pair_values_A_to_be_greater_than_B` expectation. "  # noqa: E501

        if not params.mostly or params.mostly.value == 1.0:
            if not params.or_equal:
                template_str += (
                    "Values in $column_A must always be greater than those in $column_B."
                )
            else:
                template_str += "Values in $column_A must always be greater than or equal to those in $column_B."  # noqa: E501
        else:
            renderer_configuration = cls._add_mostly_pct_param(
                renderer_configuration=renderer_configuration
            )
            if not params.or_equal:
                template_str = "Values in $column_A must be greater than those in $column_B, at least $mostly_pct % of the time."  # noqa: E501
            else:
                template_str = "Values in $column_A must be greater than or equal to those in $column_B, at least $mostly_pct % of the time."  # noqa: E501

        renderer_configuration.template_str = template_str

        return renderer_configuration

    @classmethod
    @renderer(renderer_type=LegacyRendererType.PRESCRIPTIVE)
    @render_suite_parameter_string
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
            configuration.kwargs,
            [
                "column_A",
                "column_B",
                "ignore_row_if",
                "mostly",
                "or_equal",
                "row_condition",
                "condition_parser",
            ],
        )

        if (params["column_A"] is None) or (params["column_B"] is None):
            template_str = "$column has a bogus `expect_column_pair_values_A_to_be_greater_than_B` expectation."  # noqa: E501
            params["row_condition"] = None

        if params["mostly"] is None or params["mostly"] == 1.0:
            if params["or_equal"] in [None, False]:
                template_str = "Values in $column_A must always be greater than those in $column_B."
            else:
                template_str = "Values in $column_A must always be greater than or equal to those in $column_B."  # noqa: E501
        else:
            params["mostly_pct"] = num_to_str(params["mostly"] * 100, no_scientific=True)
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            if params["or_equal"] in [None, False]:
                template_str = "Values in $column_A must be greater than those in $column_B, at least $mostly_pct % of the time."  # noqa: E501
            else:
                template_str = "Values in $column_A must be greater than or equal to those in $column_B, at least $mostly_pct % of the time."  # noqa: E501

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str + ", then " + template_str[0].lower() + template_str[1:]
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
