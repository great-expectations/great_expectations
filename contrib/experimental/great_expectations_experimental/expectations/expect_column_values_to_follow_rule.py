import json

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    Expectation,
    ExpectationConfiguration,
    render_evaluation_parameter_string,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.render import RenderedStringTemplateContent
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validator import Validator


class ColumnRuleFollowers(ColumnMapMetricProvider):
    """
    Checks to see if all rows  satisfy a given rule.
    A rule is defined as a JSON object (interpreted as a Python dict) with the following fields:

        - ranges (dict): a nonempty dictionary that contains nested lists.
            +Expected Values: Nested lists must be length 2, holding ints in increasing order, or length 0.
            +Special Cases: [] will pass in the entire string contained in the row.
            Some examples. Suppose the row is the string "1234".
            {"a": []}        - The variable a in the expression is set to "1234".
            {"x": [0, 1]}    - The variable x in the expression is set to "1".
            {"x1": [0, 1],   - The variable x1 in the expression is set to "1", and x2 is set to "4".
                "x2": [3, 4]}

        - expr (string): The expression. This is a string in the form of a Python expression. Python functions are
            available, as it is parsed using exec(). Each variable is passed in as a string.
            +Expected Value: A well-formed python expression that evaluates to a boolean value.
            +Special Cases: "True" and "False" will evaluate to True and False, respectively.
    """

    condition_metric_name = "column_values.expect_column_values_to_follow_rule"
    condition_value_keys = ("rule",)

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, rule, **kwargs):

        if rule["ranges"] is {}:
            raise ValueError("Ranges must contain at least 1 variable!")

        return column.apply(lambda x: ColumnRuleFollowers._helper(x, rule))

    @staticmethod
    def _helper(x, rule):
        """Helper function since Python doesn't like multiline functions"""
        strings = {}
        ldict = {}
        names = ""
        if x is None:
            x = ""
        if not isinstance(x, str):
            raise TypeError(
                "Column values must be strings in order to use 'expect_column_values_to_follow_rule'"
            )
        for name, rnge in rule["ranges"].items():
            if rnge[0] < rnge[1]:
                strings[name] = str(x[rnge[0] : rnge[1]])
                names += name + ","
            else:
                raise ValueError(
                    "Unexpected range. Ensure that the second number in your range is larger than the first."
                )

        exec("expr = lambda " + names + ":" + rule["expr"], None, ldict)
        func = ldict["expr"]
        return func(**strings)


# This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
# TODO: SQL Alchemy
#     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#     def _sqlalchemy(cls, column, _dialect, **kwargs):
#         return column.in_([3])

# TODO: Pyspark
# This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
#     @column_condition_partial(engine=SparkDFExecutionEngine)
#     def _spark(cls, column, **kwargs):
#         return column.isin([3])


class ExpectColumnValuesToFollowRule(ColumnMapExpectation):
    """Expect that all rows of a column satisfy a given input rule that specifies ranges and expressions.

    Args:
        column (str): The column name

    Keyword Args:
        rule (dict): Dict with keys "ranges" and "expr". \
            ranges is a dict whos keys are variables in the expression (expr) and whos values are \
            an empty list or two-item list of indicies. \
            expr is a string in the form of a Python expression that uses the variable names in the \
            ranges dict and will be parsed with exec(); each variable is passed in as a string
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "a_column": ["12345", "1abdfadfasf", "1234", "555555", "124214"],
                "or_column": ["1200", "1234", "9812", "1212", "9912"],
            },
            "tests": [
                {
                    "title": "positive_mostly_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "a_column",
                        "rule": {
                            "ranges": {"x": [0, 2], "y": [2, 5]},
                            "expr": "x == '12' and int(y) > 100",
                        },
                        "mostly": 0.4,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [1, 2, 3],
                        "unexpected_list": ["1abdfadfasf", "1234", "555555"],
                    },
                },
                {
                    "title": "positive_or_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "or_column",
                        "rule": {
                            "ranges": {"x": [0, 2], "y": [2, 4]},
                            "expr": "x == '12' or y == '12'",
                        },
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "or_column",
                        "rule": {
                            "ranges": {"x": [0, 2], "y": [2, 4]},
                            "expr": "x == '12'",
                        },
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2, 4],
                        "unexpected_list": ["9812", "9912"],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": [
            "experimental",
            "hackathon-20210206",
        ],  # Tags for this Expectation in the gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@firenoo",
            "@rldejournett",
            "@talagluck",
        ],
    }

    map_metric = "column_values.expect_column_values_to_follow_rule"

    success_keys = (
        "rule",
        "mostly",
    )

    # This method defines a question Renderer
    # For more info on Renderers, see
    # https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_docs/how_to_create_renderers_for_custom_expectations.html
    #!!! This example renderer should render RenderedStringTemplateContent, not just a string


#     @classmethod
#     @renderer(renderer_type="renderer.question")
#     def _question_renderer(
#         cls, configuration, result=None, runtime_configuration=None
#     ):
#         column = configuration.kwargs.get("column")
#         mostly = configuration.kwargs.get("mostly")

#         return f'Do at least {mostly * 100}% of values in column "{column}" equal 3?'

# This method defines an answer Renderer
#!!! This example renderer should render RenderedStringTemplateContent, not just a string
#     @classmethod
#     @renderer(renderer_type="renderer.answer")
#     def _answer_renderer(
#         cls, configuration=None, result=None, runtime_configuration=None
#     ):
#         column = result.expectation_config.kwargs.get("column")
#         mostly = result.expectation_config.kwargs.get("mostly")
#         regex = result.expectation_config.kwargs.get("regex")
#         if result.success:
#             return f'At least {mostly * 100}% of values in column "{column}" equal 3.'
#         else:
#             return f'Less than {mostly * 100}% of values in column "{column}" equal 3.'

# This method defines a prescriptive Renderer
#     @classmethod
#     @renderer(renderer_type="renderer.prescriptive")
#     @render_evaluation_parameter_string
#     def _prescriptive_renderer(
#         cls,
#         configuration=None,
#         result=None,
#         runtime_configuration=None,
#         **kwargs,
#     ):
#!!! This example renderer should be shorter
#         runtime_configuration = runtime_configuration or {}
#         include_column_name = False if runtime_configuration.get("include_column_name") is False else True
#         styling = runtime_configuration.get("styling")
#         params = substitute_none_for_missing(
#             configuration.kwargs,
#             ["column", "regex", "mostly", "row_condition", "condition_parser"],
#         )

#         template_str = "values must be equal to 3"
#         if params["mostly"] is not None:
#             params["mostly_pct"] = num_to_str(
#                 params["mostly"] * 100, precision=15, no_scientific=True
#             )
#             # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
#             template_str += ", at least $mostly_pct % of the time."
#         else:
#             template_str += "."

#         if include_column_name:
#             template_str = "$column " + template_str

#         if params["row_condition"] is not None:
#             (
#                 conditional_template_str,
#                 conditional_params,
#             ) = parse_row_condition_string_pandas_engine(params["row_condition"])
#             template_str = conditional_template_str + ", then " + template_str
#             params.update(conditional_params)

#         return [
#             RenderedStringTemplateContent(
#                 **{
#                     "content_block_type": "string_template",
#                     "string_template": {
#                         "template": template_str,
#                         "params": params,
#                         "styling": styling,
#                     },
#                 }
#             )
#         ]

if __name__ == "__main__":
    ExpectColumnValuesToFollowRule().print_diagnostic_checklist()
