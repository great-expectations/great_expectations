import json

#!!! This giant block of imports should be something simpler, such as:
# from great_expectations.helpers.expectation_creation import *
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


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesToNotContainCharacter(ColumnMapMetricProvider):
    """
    Determines whether column values do not contain a specific character. Column values pass if they do NOT contain the
    character
    """

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.not_contain_character"

    condition_value_keys = ("character",)

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, character, **kwargs):
        return column.apply(lambda val: str(character) not in str(val))


# This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
#     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#     def _sqlalchemy(cls, column, _dialect, **kwargs):
#         return column.in_([3])

# This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
#     @column_condition_partial(engine=SparkDFExecutionEngine)
#     def _spark(cls, column, **kwargs):
#         return column.isin([3])


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectColumnValuesToNotContainCharacter(ColumnMapExpectation):
    """Expect the set of column values to not contain a given character.

           expect_column_values_to_not_contain_character is a \
           :func:`column_map_expectation
   <great_expectations.execution_engine.MetaExecutionEngine.column_map_expectation>`.

           Args:
               column (str): \
                   The provided column name
               character (str): \
                   A character to test for the nonexistence of

           Other Parameters:
               result_format (str or None): \
                   Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
                   For more detail, see :ref:`result_format <result_format>`.
               include_config (boolean): \
                   If True, then include the expectation config as part of the result object. \
                   For more detail, see :ref:`include_config`.
               catch_exceptions (boolean or None): \
                   If True, then catch exceptions and include them as part of the result object. \
                   For more detail, see :ref:`catch_exceptions`.
               meta (dict or None): \
                   A JSON-serializable dictionary (nesting allowed) that will be included in the output without \
                   modification. For more detail, see :ref:`meta`.

           Returns:
               An ExpectationSuiteValidationResult

               Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
               :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.
           """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "mostly_non_spaced": [
                    "hello",
                    "snake_case_words_h",
                    "this has spaces",
                    "@@@-somh?-stuff",
                    None,
                    3.1415965,
                ],
                "mostly_none": [None, None, None, "@@@-somh?-stuff", None, 3.14159265],
            },
            "tests": [
                {
                    "title": "test_for_spaces",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_non_spaced",
                        "character": " ",
                        "mostly": 0.7,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [2],
                        "unexpected_list": ["this has spaces"],
                    },
                },
                {
                    "title": "test_for_at_symbol",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_non_spaced",
                        "character": "@",
                        "mostly": 0.7,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [3],
                        "unexpected_list": ["@@@-som?-stuff"],
                    },
                },
                {
                    "title": "test_for_letter",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_non_spaced",
                        "character": "h",
                        "mostly": 0.7,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2, 3],
                        "unexpected_list": [
                            "hello",
                            "snake_case_words_h",
                            "this has spaces",
                            "@@@-somh?-stuff",
                        ],
                    },
                },
                {
                    "title": "test_column_with_mostly_nones",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_none", "character": " ", "mostly": 1.0},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            "experimental",
            "hackathon-20200123",
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            #         "@your_name_here", # Don't forget to add your github handle here!
            "@jsteinberg4",
            "@vraimondi04",
            "@talagluck",
        ],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.not_contain_character"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = (
        "character",
        "mostly",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see {some doc}
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
    ExpectColumnValuesToNotContainCharacter().print_diagnostic_checklist()
