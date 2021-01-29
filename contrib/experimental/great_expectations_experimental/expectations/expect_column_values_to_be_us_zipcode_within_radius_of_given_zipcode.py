import json

import uszipcode

# !!! This giant block of imports should be something simpler, such as:
# from great_exepectations.helpers.expectation_creation import *
from great_expectations.execution_engine import (  # SparkDFExecutionEngine,
    PandasExecutionEngine,
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
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesAreUSZipcodeWithinMileRadiusOfGivenZipcode(ColumnMapMetricProvider):
    """
    Determines whether a US zip code is within a the given radius in miles of another given zip code.
    requirements: uszipcode
    """

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.us_zipcode_within_radius_of_given_zipcode"

    condition_value_keys = ("central_zip", "radius_in_miles")

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, central_zip=None, radius_in_miles=10, **kwargs):
        search = uszipcode.SearchEngine()
        center_zipcode_object = search.by_zipcode(central_zip)

        def _find_distance_between_zipcodes(
            center_lat, center_long, zipcode: int, search: uszipcode.search.SearchEngine
        ):
            zipcode_object = search.by_zipcode(zipcode)
            return zipcode_object.dist_from(lat=center_lat, lng=center_long)

        return column.apply(
            lambda loc: _find_distance_between_zipcodes(
                center_lat=center_zipcode_object.lat,
                center_long=center_zipcode_object.lng,
                zipcode=int(loc),
                search=search,
            )
            <= radius_in_miles
        )


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
class ExpectColumnValuesToBeUSZipcodeWithinMileRadiusOfGivenZipcode(
    ColumnMapExpectation
):
    """
    Given a zipcode and a radius, this expectation checks that all zipcodes in a column of a table are within a specified
    radius, in miles, of the given zipcode.
    """

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "all_zip_codes_within_15mi_radius_of_60201": [
                    60202,
                    60203,
                    60076,
                    60091,
                    60043,
                    60093,
                    60201,
                    None,
                    None,
                    None,
                    None,
                ],
                "all_zip_codes_within_20mi_radius_of_30338": [
                    30360,
                    30350,
                    30328,
                    30346,
                    30319,
                    30342,
                    30341,
                    None,
                    None,
                    None,
                    None,
                ],
                "all_zip_codes_within_50mi_radius_of_92130": [
                    91911,
                    92029,
                    92071,
                    92091,
                    92101,
                    92102,
                    92109,
                    92111,
                    92131,
                    92140,
                    None,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_all_zipcodes_in_15mi_radius_of_60201",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_zip_codes_within_15mi_radius_of_60201",
                        "mostly": 1.0,
                        "central_zip": 60201,
                        "radius_in_miles": 15,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "positive_test_with_all_zipcodes_in_20mi_radius_of_30338",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_zip_codes_within_20mi_radius_of_30338",
                        "mostly": 1.0,
                        "central_zip": 30338,
                        "radius_in_miles": 20,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_all_zipcodes_in_20mi_radius_of_30338",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_zip_codes_within_20mi_radius_of_30338",
                        "mostly": 1.0,
                        "central_zip": 37409,
                        "radius_in_miles": 20,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [0, 1, 2, 3, 4, 5, 6],
                        "unexpected_list": [
                            30360,
                            30350,
                            30328,
                            30346,
                            30319,
                            30342,
                            30341,
                        ],
                    },
                },
                {
                    "title": "positive_test_with_all_zipcodes_in_25mi_radius_of_92130",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_zip_codes_within_50mi_radius_of_92130",
                        "mostly": 1.0,
                        "central_zip": 92130,
                        "radius_in_miles": 50,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [],
                        "unexpected_list": [],
                    },
                },
                {
                    "title": "negative_test_with_some_zipcodes_in_5mi_radius_of_30338",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "all_zip_codes_within_20mi_radius_of_30338",
                        "mostly": 1.0,
                        "central_zip": 30338,
                        "radius_in_miles": 5,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [4, 5],
                        "unexpected_list": [
                            30319,
                            30342,
                        ],
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [
            "experimental",
            "hackathon-20200123",
        ],  # Tags for this Expectation in the gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@sethdmay",
            "@maximetokman",
            "@talagluck",
        ],
        "package": "experimental_expectations",
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.us_zipcode_within_radius_of_given_zipcode"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = ("mostly", "central_zip", "radius_in_miles")

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
    diagnostics_report = (
        ExpectColumnValuesToBeUSZipcodeWithinMileRadiusOfGivenZipcode().run_diagnostics()
    )
print(json.dumps(diagnostics_report, indent=2))
