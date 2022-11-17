import json
from typing import Optional

import pandas as pd

#!!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import PandasExecutionEngine
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
class ForeignKeysInColumnAExistInColumnB(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.foreign_key_in_other_col"
    condition_value_keys = ("df", "column_B")
    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, df, column_B, **kwargs):
        if type(df) == list:
            df = pd.DataFrame(df)
        value_set = set(df[column_B])
        return column.isin(value_set)
        # return True


# This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
#     @column_condition_partial(engine=SqlAlchemyExecutionEngine)
#     def _sqlalchemy(cls, column, _dialect, **kwargs):
#         return column.in_([3])


# This class defines the Expectation itself
# The main business logic for calculation lives here.
class ExpectForeignKeysInColumnAToExistInColumnB(ColumnMapExpectation):
    """Ensure that values in the column of interest (ColumnA) are in a valueset provided as a dataframe (df parameter) + column (column_B parameter) or as a list of elements supported by pandas.DataFrame() (e.g. list of dicts [{"col_name": value},], list of tuples [(value, value), (value, value)]. This is a very experimental implementation to describe the functionality, but this expectation should be revisited once cross-table expectation templates are available."""

    examples = [
        {
            # "expectation_type": "expect_column_values_to_be_in_set",
            "data": {
                "x": [1, 2, 4],
                "y": [1.1, 2.2, 5.5],
                "z": ["hello", "jello", "mello"],
            },
            "tests": [
                {
                    "title": "basic_positive_test_case_number_set",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "df": [{"fk_col": 1}, {"fk_col": 2}, {"fk_col": 4}],
                        "column_B": "fk_col",
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test_case_number_set",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "x",
                        "df": [{"fk_col": 1}, {"fk_col": 2}, {"fk_col": 7}],
                        "column_B": "fk_col",
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [
            "experimental",
            "help_wanted",
        ],  # Tags for this Expectation in the gallery
        "contributors": ["@robertparker"],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.foreign_key_in_other_col"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("df", "column_B")

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

# def get_validation_dependencies(
#     self,
#     configuration: Optional[ExpectationConfiguration] = None,
#     execution_engine: Optional[ExecutionEngine] = None,
#     runtime_configuration: Optional[dict] = None,
# ):
#     dependencies = super().get_validation_dependencies(
#         configuration, execution_engine, runtime_configuration
#     )
#     # get other_table_name kwarg
#     # get the column_B kwarg
#     # get metric expect_column_values_to_be_unique


#     other_table_name = configuration.kwargs.get("other_table_name")
#     # create copy of table.row_count metric and modify "table" metric domain kwarg to be other table name
#     table_row_count_metric_config_other = deepcopy(
#         dependencies["metrics"]["table.row_count"]
#     )
#     table_row_count_metric_config_other.metric_domain_kwargs[
#         "table"
#     ] = other_table_name
#     # rename original "table.row_count" metric to "table.row_count.self"
#     dependencies["metrics"]["table.row_count.self"] = dependencies["metrics"].pop(
#         "table.row_count"
#     )
#     # add a new metric dependency named "table.row_count.other" with modified metric config
#     dependencies["metrics"][
#         "table.row_count.other"
#     ] = table_row_count_metric_config_other

#     return dependencies

# def _validate(
#     self,
#     configuration: ExpectationConfiguration,
#     metrics: Dict,
#     runtime_configuration: dict = None,
#     execution_engine: ExecutionEngine = None,
# ):
#     table_row_count_self = metrics["table.row_count.self"]
#     table_row_count_other = metrics["table.row_count.other"]

#     return {
#         "success": table_row_count_self == table_row_count_other,
#         "result": {
#             "observed_value": {
#                 "self": table_row_count_self,
#                 "other": table_row_count_other,
#             }
#         },
#     }

if __name__ == "__main__":
    ExpectForeignKeysInColumnAToExistInColumnB().print_diagnostic_checklist()
