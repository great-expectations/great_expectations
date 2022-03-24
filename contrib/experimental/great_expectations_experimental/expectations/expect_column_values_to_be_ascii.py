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
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes
from great_expectations.expectations.registry import (
    _registered_expectations,
    _registered_metrics,
    _registered_renderers,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.render.util import num_to_str, substitute_none_for_missing
from great_expectations.validator.validator import Validator


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesAreAscii(ColumnMapMetricProvider):
    """
    Determines whether column values consist only of ascii characters. If value consists of any non-ascii character
    then that value will not pass.
    """

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.are_ascii"

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def check_if_ascii(x):
            return str(x).isascii()

        column_ascii_check = column.apply(check_if_ascii)
        return column_ascii_check

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    #     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    #     def _sqlalchemy(cls, column, _dialect, **kwargs):
    #         return column.in_([3])

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        def is_ascii(val):
            return str(val).isascii()

        is_ascii_udf = F.udf(is_ascii, sparktypes.BooleanType())

        return is_ascii_udf(column)


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectColumnValuesToBeAscii(ColumnMapExpectation):
    """Expect the set of column values to be ASCII characters

           expect_column_values_to_not_contain_character is a \
           :func:`column_map_expectation
   <great_expectations.execution_engine.MetaExecutionEngine.column_map_expectation>`.

           Args:
               column (str): \
                   The provided column name

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
                "mostly_ascii": [1, 12.34, "a;lskdjfji", "””", "#$%^&*(", None, None],
                "mostly_numbers": [42, 404, 858, 8675309, 62, 48, 17],
                "mostly_characters": [
                    "Lantsberger",
                    "Gil Pasternak",
                    "Vincent",
                    "J@sse",
                    "R!ck R00l",
                    "A_",
                    "B+",
                ],
                "not_ascii": [
                    "ဟယ်လို",
                    " שלום",
                    "नमस्ते",
                    "رحبا",
                    "ନମସ୍କାର",
                    "สวัสดี",
                    "ಹಲೋ ",
                ],
            },
            "schemas": {
                "spark": {
                    "mostly_ascii": "StringType",
                    "mostly_numbers": "StringType",
                    "mostly_characters": "StringType",
                    "not_ascii": "StringType",
                }
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly_ascii",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_ascii", "mostly": 0.6},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [3],
                        "unexpected_list": ["””"],
                    },
                },
                {
                    "title": "positive_test_with_mostly_numbers",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_numbers", "mostly": 1.0},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "positive_test_with_mostly_ascii",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mostly_characters", "mostly": 1.0},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_non_ascii",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "not_ascii", "mostly": 1.0},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2, 3, 4, 5, 6],
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
            "@jsteinberg4",
            "@vraimondi04",
            "@talagluck",
            "@lodeous",
            "@rexboyce",
            "@bragleg",
        ],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.are_ascii"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see {some doc}
    #!!! This example renderer should render RenderedStringTemplateContent, not just a string


#     @classmethod
#     @renderer(renderer_type="renderer.question")
#     def _question_renderer(
#         cls, configuration, result=None, language=None, runtime_configuration=None
#     ):
#         column = configuration.kwargs.get("column")
#         mostly = configuration.kwargs.get("mostly")

#         return f'Do at least {mostly * 100}% of values in column "{column}" equal 3?'

# This method defines an answer Renderer
#!!! This example renderer should render RenderedStringTemplateContent, not just a string
#     @classmethod
#     @renderer(renderer_type="renderer.answer")
#     def _answer_renderer(
#         cls, configuration=None, result=None, language=None, runtime_configuration=None
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
#         language=None,
#         runtime_configuration=None,
#         **kwargs,
#     ):
#!!! This example renderer should be shorter
#         runtime_configuration = runtime_configuration or {}
#         include_column_name = runtime_configuration.get("include_column_name", True)
#         include_column_name = (
#             include_column_name if include_column_name is not None else True
#         )
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
    ExpectColumnValuesToBeAscii().print_diagnostic_checklist()
