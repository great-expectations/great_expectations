import json

from scipy import stats

# !!! This giant block of imports should be something simpler, such as:
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


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit
# https://docs.greatexpectations.io/en/latest/reference/core_concepts.html#expectations-and-metrics.
class ColumnValuesNotOutliers(ColumnMapMetricProvider):
    # This is the id string that will be used to reference your metric.
    # Please see https://docs.greatexpectations.io/en/latest/reference/core_concepts/metrics.html#metrics
    # for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.not_outliers"
    condition_value_keys = ("method", "multiplier")

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, method="iqr", multiplier=1.5, **kwargs):
        if method == "iqr":
            iqr = stats.iqr(column)
            median = column.median()
            return (column - median).abs() < multiplier * iqr
        elif method == "std":
            std = column.std()
            mean = column.mean()
            return (column - mean).abs() < multiplier * std
        else:
            raise NotImplementedError(f"method {method} has not been implemented")


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
class ExpectColumnValuesToNotBeOutliers(ColumnMapExpectation):
    """
    Expect Column Values to not be outliers. User is asked to specify the column, method and multiplier. Currently
    standard deviation (std) and inter-quantile range (iqr) are supported.
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "a": [
                    0.88546138,
                    1.3061609,
                    -0.32247349,
                    0.97258135,
                    0.98273209,
                    -0.10502805,
                    0.63429027,
                    -0.520042,
                    -0.15674414,
                    0.94144714,
                    -0.88228603,
                    -0.60380027,
                    -0.11121819,
                    0.74895147,
                    0.42992403,
                    0.65493905,
                    1.35901276,
                    0.49965162,
                    2.0,
                    3.0,
                ],  # drawn from Normal(0,1)
                "b": [
                    1.46104728,
                    1.33568658,
                    1.39303305,
                    1.34369635,
                    2.07627429,
                    3.22523841,
                    1.2514533,
                    2.44427933,
                    2.12703316,
                    3.29557985,
                    1.04298411,
                    1.3659108,
                    4.18867559,
                    2.85009897,
                    1.58180929,
                    1.47433799,
                    1.10678471,
                    4.73338285,
                    5.0,
                    10.0,
                ],  # drawn from Gamma(1,1)
                "c": [
                    78.09208927,
                    79.08947083,
                    78.15403075,
                    91.01199697,
                    86.87351353,
                    93.31079309,
                    92.41605866,
                    85.95186289,
                    85.57633936,
                    82.9214903,
                    78.67996655,
                    83.65076874,
                    76.51547517,
                    75.95991938,
                    73.56762212,
                    98.82595865,
                    88.0945241,
                    75.38697834,
                    115.0,
                    0.0,
                ],  # drawn from Beta(11, 2)
                "d": [
                    0.15131528,
                    -0.32290392,
                    0.33894553,
                    0.41806171,
                    0.09906698,
                    0.32659221,
                    -0.07283207,
                    0.72584037,
                    0.07496465,
                    -0.28889126,
                    3.57416451,
                    3.44258958,
                    3.11353884,
                    2.82008269,
                    3.68115642,
                    3.23682442,
                    2.70231677,
                    3.21949992,
                    4.06638354,
                    4.77655811,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_std",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "method": "std", "multiplier": 3},
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test_iqr",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "a", "method": "iqr", "multiplier": 1.5},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [19],
                        "unexpected_list": [3],
                    },
                },
                {
                    "title": "negative_test_iqr_mostly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "b",
                        "mostly": 0.9,
                        "method": "iqr",
                        "multiplier": 1.5,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [17, 18, 19],
                        "unexpected_list": [4.73338285, 5.0, 10.0],
                    },
                },
                {
                    "title": "positive_test_std_mostly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "b",
                        "mostly": 0.9,
                        "method": "std",
                        "multiplier": 3,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test_std",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "c", "method": "std", "multiplier": 3},
                    "out": {
                        "success": False,
                        "unexpected_index_list": [19],
                        "unexpected_list": [0],
                    },
                },
                {
                    "title": "positive_test_iqr",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "d", "method": "iqr", "multiplier": 1.5},
                    "out": {
                        "success": True,
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [  # Tags for this Expectation in the gallery
            #         "experimental"
        ],
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@rexboyce",
            "@lodeous",
            "@bragleg",
        ],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.not_outliers"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see https://docs.greatexpectations.io/en/latest/reference/core_concepts/expectations/expectations.html#expectation-concepts-domain-and-success-keys
    # for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly", "method", "multiplier")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This method defines a question Renderer
    # For more info on Renderers, see
    # https://docs.greatexpectations.io/en/latest/guides/how_to_guides/configuring_data_docs/how_to_create_renderers_for_custom_expectations.html
    # !!! This example renderer should render RenderedStringTemplateContent, not just a string

    # @classmethod
    # @renderer(renderer_type="renderer.question")
    # def _question_renderer(
    #     cls, configuration, result=None, language=None, runtime_configuration=None
    # ):
    #     column = configuration.kwargs.get("column")
    #     mostly = configuration.kwargs.get("mostly")
    #
    #     return f'Do at least {mostly * 100}% of values in column "{column}" equal 3?'


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
# # !!! This example renderer should be shorter
#         runtime_configuration = runtime_configuration or {}
#         include_column_name = False if runtime_configuration.get("include_column_name") is False else True
#         styling = runtime_configuration.get("styling")
#         params = substitute_none_for_missing(
#             configuration.kwargs,
#             ["column", "regex", "mostly", "row_condition", "condition_parser"],
#         )
#
#         template_str = "values must be equal to 3"
#         if params["mostly"] is not None:
#             params["mostly_pct"] = num_to_str(
#                 params["mostly"] * 100, precision=15, no_scientific=True
#             )
#             # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
#             template_str += ", at least $mostly_pct % of the time."
#         else:
#             template_str += "."
#
#         if include_column_name:
#             template_str = "$column " + template_str
#
#         if params["row_condition"] is not None:
#             (
#                 conditional_template_str,
#                 conditional_params,
#             ) = parse_row_condition_string_pandas_engine(params["row_condition"])
#             template_str = conditional_template_str + ", then " + template_str
#             params.update(conditional_params)
#
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
    ExpectColumnValuesToNotBeOutliers().print_diagnostic_checklist()
