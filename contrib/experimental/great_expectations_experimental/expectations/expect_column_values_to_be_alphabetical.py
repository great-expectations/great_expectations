import json
import operator
from typing import Any, Dict, Optional, Tuple

import pandas

from great_expectations.core import ExpectationConfiguration

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.execution_engine import (
    MetricDomainTypes,
    MetricPartialFunctionTypes,
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
from great_expectations.expectations.metrics.import_manager import F, Window, sparktypes
from great_expectations.expectations.metrics.map_metric import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.metrics.metric_provider import (
    metric_partial,
    metric_value,
)
from great_expectations.expectations.metrics.table_metrics.table_column_types import (
    ColumnTypes,
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
from great_expectations.validator.validation_graph import MetricConfiguration
from great_expectations.validator.validator import Validator


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesAreAlphabetical(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.are_alphabetical"
    condition_value_keys = ("reverse",)

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, reverse=False, **kwargs):

        # lowercase the whole column to avoid issues with capitalization
        # (since every capital letter is "before" the lowercase letters)
        column_lower = column.map(str.lower)

        column_length = column.size

        # choose the operator to use for comparison of consecutive items
        # could be easily adapted for other comparisons, perhaps of custom objects
        if reverse:
            compare_function = operator.ge
        else:
            compare_function = operator.le

        output = [True]  # first value is automatically in order
        for i in range(1, column_length):
            if (
                column_lower[i] and column_lower[i - 1]
            ):  # make sure we aren't comparing Nones
                output.append(compare_function(column_lower[i - 1], column_lower[i]))
            else:
                output.append(None)

        return pandas.Series(output)


# This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
#     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#     def _sqlalchemy(cls, column, _dialect, **kwargs):
#         return column.in_([3])
#
#
#
#
#
#
#

# This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
#     @column_condition_partial(engine=SparkDFExecutionEngine)
#     def _spark(cls, column, **kwargs):
#         return column.isin([3])

######
#   The Spark implementation is based on the expect_column_values_to_be_decreasing Expectation but currently doesn't
#   work.
######


# @metric_partial(
#     engine=SparkDFExecutionEngine,
#     partial_fn_type=MetricPartialFunctionTypes.WINDOW_CONDITION_FN,
#     domain_type=MetricDomainTypes.COLUMN,
# )
# def _spark(
#     cls,
#     execution_engine: SparkDFExecutionEngine,
#     metric_domain_kwargs: Dict,
#     metric_value_kwargs: Dict,
#     metrics: Dict[Tuple, Any],
#     runtime_configuration: Dict,
# ):
#     # check if column is any type that could have na (numeric types)
#     column_name = metric_domain_kwargs["column"]
#     # table_columns = metrics["table.column_types"]
#     # column_metadata = [col for col in table_columns if col["name"] == column_name][
#     #     0
#     # ]
#     # if isinstance(
#     #     column_metadata["type"],
#     #     (
#     #         sparktypes.LongType,
#     #         sparktypes.DoubleType,
#     #         sparktypes.IntegerType,
#     #     ),
#     # ):
#     #     # if column is any type that could have NA values, remove them (not filtered by .isNotNull())
#     #     compute_domain_kwargs = execution_engine.add_column_row_condition(
#     #         metric_domain_kwargs,
#     #         filter_null=cls.filter_column_isnull,
#     #         filter_nan=True,
#     #     )
#     # else:
#     compute_domain_kwargs = metric_domain_kwargs
#     (
#         df,
#         compute_domain_kwargs,
#         accessor_domain_kwargs,
#     ) = execution_engine.get_compute_domain(
#         compute_domain_kwargs, MetricDomainTypes.COLUMN
#     )
#
#     # # NOTE: 20201105 - parse_strings_as_datetimes is not supported here;
#     # # instead detect types naturally
#     column = F.col(column_name)
#     column = F.lower(column)
#     # if isinstance(
#     #     column_metadata["type"], (sparktypes.TimestampType, sparktypes.DateType)
#     # ):
#     #     diff = F.datediff(
#     #         column, F.lag(column).over(Window.orderBy(F.lit("constant")))
#     #     )
#     # else:
#     diff = F.lag(column, default="a").over(Window.orderBy(F.lit("constant")))
#     diff = F.when(column > diff, True).otherwise(False)
#
#     # NOTE: because in spark we are implementing the window function directly,
#     # we have to return the *unexpected* condition
#     # if metric_value_kwargs["strictly"]:
#     #     return (
#     #         F.when(diff >= 0, F.lit(True)).otherwise(F.lit(False)),
#     #         compute_domain_kwargs,
#     #         accessor_domain_kwargs,
#     #     )
#     # # If we expect values to be flat or decreasing then unexpected values are those
#     # # that are decreasing
#     # else:
#     return (
#         diff,
#         compute_domain_kwargs,
#         accessor_domain_kwargs,
#     )


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectColumnValuesToBeAlphabetical(ColumnMapExpectation):
    """
    Given a list of string values, check if the list is alphabetical, either forwards or backwards (specified with the
    `reverse` parameter). Comparison is case-insensitive. Using `mostly` will give you how many items are alphabetical
    relative to the immediately previous item in the list.

    conditions:
        reverse: Checks for Z to A alphabetical if True, otherwise checks A to Z
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "is_alphabetical_lowercase": [
                    "apple",
                    "banana",
                    "coconut",
                    "donut",
                    "eggplant",
                    "flour",
                    "grapes",
                    "jellybean",
                    None,
                    None,
                    None,
                ],
                "is_alphabetical_lowercase_reversed": [
                    "moon",
                    "monster",
                    "messy",
                    "mellow",
                    "marble",
                    "maple",
                    "malted",
                    "machine",
                    None,
                    None,
                    None,
                ],
                "is_alphabetical_mixedcase": [
                    "Atlanta",
                    "bonnet",
                    "Delaware",
                    "gymnasium",
                    "igloo",
                    "Montreal",
                    "Tennessee",
                    "toast",
                    "Washington",
                    "xylophone",
                    "zebra",
                ],
                "out_of_order": [
                    "Right",
                    "wrong",
                    "up",
                    "down",
                    "Opposite",
                    "Same",
                    "west",
                    "east",
                    None,
                    None,
                    None,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_all_values_alphabetical_lowercase",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "is_alphabetical_lowercase",
                        "reverse": False,
                        "mostly": 1.0,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_all_values_alphabetical_lowercase_reversed",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "is_alphabetical_lowercase_reversed",
                        "reverse": False,
                        "mostly": 1.0,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [1, 2, 3, 4, 5, 6, 7],
                        "unexpected_list": [
                            "monster",
                            "messy",
                            "mellow",
                            "marble",
                            "maple",
                            "malted",
                            "machine",
                        ],
                    },
                },
                {
                    "title": "positive_test_with_all_values_alphabetical_lowercase_reversed",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "is_alphabetical_lowercase_reversed",
                        "reverse": True,
                        "mostly": 1.0,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "positive_test_with_all_values_alphabetical_mixedcase",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "is_alphabetical_mixedcase", "mostly": 1.0},
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_out_of_order_mixedcase",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "out_of_order", "mostly": 1.0},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2, 3, 7],
                        "unexpected_list": ["up", "down", "east"],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": ["experimental"],  # Tags for this Expectation in the gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@sethdmay",
            "@maximetokman",
            "@Harriee02",  # Don't forget to add your github handle here!
        ],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.are_alphabetical"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly", "reverse")

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
    ExpectColumnValuesToBeAlphabetical().print_diagnostic_checklist()
