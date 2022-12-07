from typing import Optional

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationValidationResult,
)
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import num_to_str, substitute_none_for_missing


class ColumnValuesEqualThree(ColumnMapMetricProvider):
    condition_metric_name = "column_values.equal_three"
    # condition_value_keys = {}
    # default_kwarg_values = {}

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        return column == 3


class ExpectColumnValuesToEqualThree(ColumnMapExpectation):

    map_metric = "column_values.equal_three"
    success_keys = ("mostly",)
    # default_kwarg_values = ColumnMapExpectation.default_kwarg_values


class ExpectColumnValuesToEqualThree__SecondIteration(ExpectColumnValuesToEqualThree):
    """Expect values in this column to equal the number three."""

    examples = [
        {
            "data": {
                "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.6},
                    "include_in_gallery": True,
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "negative_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.9},
                    "include_in_gallery": False,
                    "out": {
                        "success": False,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "other_negative_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.9},
                    # "include_in_gallery": False, #This key is omitted, so the example shouldn't show up in the gallery
                    "out": {
                        "success": False,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
            ],
        }
    ]

    library_metadata = {
        "maturity": "EXPERIMENTAL",
        "tags": ["tag", "other_tag"],
        "contributors": [
            "@abegong",
        ],
    }


class ExpectColumnValuesToEqualThree__ThirdIteration(
    ExpectColumnValuesToEqualThree__SecondIteration
):
    @classmethod
    @renderer(renderer_type="renderer.question")
    def _question_renderer(cls, configuration, result=None, runtime_configuration=None):
        column = configuration.kwargs.get("column")
        mostly = configuration.kwargs.get("mostly")

        if mostly:
            return (
                f'Do at least {mostly * 100}% of values in column "{column}" equal 3?'
            )
        else:
            return f'Do all the values in column "{column}" equal 3?'

    @classmethod
    @renderer(renderer_type="renderer.answer")
    def _answer_renderer(
        cls, configuration=None, result=None, runtime_configuration=None
    ):
        column = result.expectation_config.kwargs.get("column")
        mostly = result.expectation_config.kwargs.get("mostly")

        if mostly:
            if result.success:
                return (
                    f'At least {mostly * 100}% of values in column "{column}" equal 3.'
                )
            else:
                return (
                    f'Less than {mostly * 100}% of values in column "{column}" equal 3.'
                )
        else:
            if result.success:
                return f'All of the values in column "{column}" equal 3.'
            else:
                return f'Not all of the values in column "{column}" equal 3.'

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
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
            ["column", "regex", "mostly", "row_condition", "condition_parser"],
        )

        template_str = "values must be equal to 3"
        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
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


class ExpectColumnValuesToEqualThree__BrokenIteration(
    ExpectColumnValuesToEqualThree__SecondIteration
):

    examples = [
        {
            "data": {
                "mostly_threes": [3, 3, 3, 3, 3, 3, 2, -1, None, None],
                "broken_column": [3, 3, 3, 3, 3, 3, 2, -1, "b", "b"],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.6},
                    "include_in_gallery": True,
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "negative_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.9},
                    "include_in_gallery": False,
                    "out": {
                        "success": False,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "other_negative_test_with_mostly",
                    "exact_match_out": False,
                    "in": {"column": "mostly_threes", "mostly": 0.9},
                    # "include_in_gallery": False, #This key is omitted, so the example shouldn't show up in the gallery
                    "out": {
                        "success": False,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "test_that_will_error_out",
                    "exact_match_out": False,
                    "in": {"column": "column_that_doesnt_exist"},
                    "include_in_gallery": True,
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
                {
                    "title": "another_test_that_will_error_out",
                    "exact_match_out": False,
                    "in": {"column": "broken_column"},
                    "include_in_gallery": True,
                    "out": {
                        "success": True,
                        "unexpected_index_list": [6, 7],
                        "unexpected_list": [2, -1],
                    },
                },
            ],
        }
    ]
