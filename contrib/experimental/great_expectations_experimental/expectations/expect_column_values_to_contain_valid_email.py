import json
import re  # regular expressions

# !!! This giant block of imports should be something simpler, such as:
from great_expectations import *
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

EMAIL_REGEX = r"[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,7}$"


class ColumnValuesContainValidEmail(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.valid_email"
    condition_value_keys = ()

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def matches_email_regex(x):
            if re.match(EMAIL_REGEX, str(x)):
                return True
            return False

        return column.apply(lambda x: matches_email_regex(x) if x else False)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    #     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    #     def _sqlalchemy(cls, column, _dialect, **kwargs):
    #         return column.in_([3])

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        return column.rlike(EMAIL_REGEX)


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectColumnValuesToContainValidEmail(ColumnMapExpectation):
    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "fail_case_1": ["a123@something", "a123@something.", "a123."],
                "fail_case_2": ["aaaa.a123.co", "aaaa.a123.", "aaaa.a123.com"],
                "fail_case_3": ["aaaa@a123.e", "aaaa@a123.a", "aaaa@a123.d"],
                "fail_case_4": ["@a123.com", "@a123.io", "@a123.eu"],
                "pass_case_1": [
                    "a123@something.com",
                    "vinod.km@something.au",
                    "this@better.work",
                ],
                "pass_case_2": [
                    "example@website.dom",
                    "ex.ample@example.ex",
                    "great@expectations.email",
                ],
                "valid_emails": [
                    "Janedoe@company.org",
                    "someone123@stuff.net",
                    "mycompany@mycompany.com",
                ],
                "bad_emails": ["Hello, world!", "Sophia", "this should fail"],
            },
            "tests": [
                {
                    "title": "negative_test_for_no_domain_name",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_1"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "a123@something",
                            "a123@something.",
                            "a123.",
                        ],
                    },
                },
                {
                    "title": "negative_test_for_no_at_symbol",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_2"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "aaaa.a123.co",
                            "aaaa.a123.",
                            "aaaa.a123.com",
                        ],
                    },
                },
                {
                    "title": "negative_test_for_ending_with_one_character",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_3"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "aaaa@a123.e",
                            "aaaa@a123.a",
                            "aaaa@a123.d",
                        ],
                    },
                },
                {
                    "title": "negative_test_for_emails_with_no_leading_string",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "fail_case_4"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "aaaa@a123.e",
                            "aaaa@a123.a",
                            "aaaa@a123.d",
                        ],
                    },
                },
                {
                    "title": "pass_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "pass_case_1"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "pass_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "pass_case_2"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "valid_emails",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "valid_emails"},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "invalid_emails",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "bad_emails"},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2],
                        "unexpected_list": [
                            "Hello, world!",
                            "Sophia",
                            "this should fail",
                        ],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",
        "tags": ["experimental", "column map expectation"],
        "contributors": [  # Github
            "@aworld1",
            "@enagola",
            "@spencerhardwick",
            "@vinodkri1",
            "@degulati",
            "@ljohnston931",
            "@rexboyce",
            "@lodeous",
            "@sophiarawlings",
            "@vtdangg",
        ],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.valid_email"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ()

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see {some doc}
    # !!! This example renderer should render RenderedStringTemplateContent, not just a string


#     @classmethod
#     @renderer(renderer_type="renderer.question")
#     def _question_renderer(
#         cls, configuration, result=None, language=None, runtime_configuration=None
#     ):
#         column = configuration.kwargs.get("column")
#         mostly = configuration.kwargs.get("mostly")

#         return f'Do at least {mostly * 100}% of values in column "{column}" equal 3?'

# This method defines an answer Renderer
# !!! This example renderer should render RenderedStringTemplateContent, not just a string
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
# !!! This example renderer should be shorter
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
    ExpectColumnValuesToContainValidEmail().print_diagnostic_checklist()
