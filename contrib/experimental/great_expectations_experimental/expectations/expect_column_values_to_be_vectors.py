import json
import re

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
# To learn about the relationship between Metrics and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics.
class ColumnValuesIsVector(ColumnMapMetricProvider):
    VECTOR_REGEX = r"\[\d+\.*\d*,\s*\d+\.*\d*(,\s*\d+\.*\d*)*]"

    # This is the id string that will be used to reference your metric.
    # Please see https://docs.greatexpectations.io/en/latest/reference/core_concepts/metrics.html#metrics
    # for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.is_vector"

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        def matches_vector(x):
            """Checks if the row is a list containing only numbers with length
            greater than 1. If a string uses regular expression to check
            Returns true for such rows"""
            if isinstance(x, str) and re.match(
                ColumnValuesIsVector.VECTOR_REGEX, str(x)
            ):
                return True
            elif isinstance(x, list) and len(x) > 1:
                return all(isinstance(listobject, (int, float)) for listobject in x)
            else:
                return False

        return column.apply(lambda x: matches_vector(x))


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
class ExpectColumnValuesToBeVectors(ColumnMapExpectation):
    """Expect column values to be vectors"""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "mostly_vectors_and_numbers_strings_scalars": [
                    [1.1, 4, 5],
                    "[2, 3.4,6]",
                    [3, 9, 7],
                    [2, 2, 2],
                    [6, 7, 9],
                    6,
                    [9, 4],
                    "five",
                    [0],
                    None,
                ],
                "all_valid_vectors": [
                    [2.1, 3, 4],
                    [9, 5, 4],
                    "[0, 0, 2]",
                    [9, 1, 4],
                    [8, 7, 8],
                    [2, 6, 0],
                    [1, 2, 9],
                    [8, 7, 4],
                    [2, 3, 6],
                    [6, 7, 2],
                ],
            },
            "tests": [
                {
                    "title": "vectors_and_nonvectors",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_vectors_and_numbers_strings_scalars",
                        "mostly": 0.6,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [5, 7, 8],
                        "unexpected_list": [6, "five", [0]],
                    },
                },
                {
                    "title": "valid_vectors",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "all_valid_vectors", "mostly": 1},
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
        "tags": ["experimental", "datatypes", "column map expectation"],
        "contributors": ["@manyshapes"],
        "requirements": [],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.is_vector"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see https://docs.greatexpectations.io/en/latest/reference/core_concepts/expectations/expectations.html#expectation-concepts-domain-and-success-keys
    # for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see
    # https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_docs/how_to_create_renderers_for_custom_expectations.html
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
    ExpectColumnValuesToBeVectors().print_diagnostic_checklist()
