import json

import geopandas
from shapely.geometry import Point

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
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesPointWithinGeoRegion(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.point_within_geo_region"
    condition_value_keys = ("country_iso_a3",)
    world = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, country_iso_a3, **kwargs):
        # Fetches polygon points from Geopandas library for the iso code
        country_shapes = cls.world[["geometry", "iso_a3"]]
        country_shapes = country_shapes[country_shapes["iso_a3"] == country_iso_a3]
        country_shapes.reset_index(drop=True, inplace=True)
        if country_shapes.empty:
            raise Exception("This ISO country code is not supported.")
        country_polygon = country_shapes["geometry"][0]

        points = geopandas.GeoSeries(column.apply(Point))
        return points.within(country_polygon)


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
class ExpectColumnValuesPointWithinGeoRegion(ColumnMapExpectation):
    """This expectation will check a (longitude, latitude) tuple to see if it falls within a country input by the
    user. To do this geo calculation, it leverages the Geopandas library. So for now it only supports the countries
    that are in the Geopandas world database. Importantly, countries are defined by their iso_a3 country code, not their
    full name."""

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "mostly_points_within_geo_region_PER": [
                    (-77.0428, -12.0464),
                    (-72.545128, -13.163068),
                    (-75.01515, -9.18997),
                    (-3.435973, 55.378051),
                    None,
                ],
                "mostly_points_within_geo_region_GBR": [
                    (-77.0428, -12.0464),
                    (-72.545128, -13.163068),
                    (2.2426, 53.4808),
                    (-3.435973, 55.378051),
                    None,
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_mostly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_points_within_geo_region_PER",
                        "country_iso_a3": "PER",
                        "mostly": 0.5,
                    },
                    "out": {
                        "success": True,
                        "unexpected_index_list": [3],
                        "unexpected_list": [(-3.435973, 55.378051)],
                    },
                },
                {
                    "title": "negative_test_with_mostly",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_points_within_geo_region_GBR",
                        "country_iso_a3": "PER",
                        "mostly": 0.9,
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [2, 3],
                        "unexpected_list": [(2.2426, 53.4808), (-3.435973, 55.378051)],
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
            "@DXcarlos",
            "@Rxmeez",
            "@ryanlindeborg",  # Don't forget to add your github handle here!
        ],
        "package": "experimental_expectations",
        "requirements": ["geopandas"],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.point_within_geo_region"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = (
        "country_iso_a3",
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
    diagnostics_report = ExpectColumnValuesPointWithinGeoRegion().run_diagnostics()
    print(json.dumps(diagnostics_report, indent=2))
