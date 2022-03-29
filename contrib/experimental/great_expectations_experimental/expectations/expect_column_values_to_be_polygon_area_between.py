import geopandas
from shapely.geometry import mapping, shape

from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesPolygonArea(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.polygon_area"
    condition_value_keys = (
        "min_area",
        "max_area",
        "crs",
    )

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, min_area, max_area, crs="epsg:4326", **kwargs):
        # Convert from json/dict to polygon/multipolygon
        column = column.apply(shape)
        column = geopandas.GeoSeries(column)
        # Set crs so geopandas knows how the data is represented
        column = column.set_crs(crs)
        # Convert from current representation to an equal area representation
        column = column.to_crs({"proj": "cea"})
        # Divide to get area in squared kilometers
        column_array = column.area / 10**6

        return (column_array >= min_area) & (column_array <= max_area)


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
class ExpectColumnValuesToBePolygonAreaBetween(ColumnMapExpectation):
    """This expectation will compute the area of each polygon/multipolygon in square kilometers and check if it's between two values."""

    world = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
    # Index by name to make example data shorter
    world_indexed_by_name = world.set_index("name")
    # We're only concerned with the polygons for the data
    world_geometries = world_indexed_by_name.geometry

    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                # low-res polygons of each country
                # Mapping function must be used for the polygons to become json serializable
                "country_polygons_less_than_20000_km2": [
                    mapping(world_geometries.loc["Luxembourg"]),
                    mapping(world_geometries.loc["Cyprus"]),
                    mapping(world_geometries.loc["Puerto Rico"]),
                    mapping(world_geometries.loc["Bahamas"]),
                    mapping(world_geometries.loc["Kuwait"]),
                ],
                "country_polygons_between_500000_and_600000_km2": [
                    mapping(world_geometries.loc["Thailand"]),
                    mapping(world_geometries.loc["Ukraine"]),
                    mapping(world_geometries.loc["Madagascar"]),
                    mapping(world_geometries.loc["Kenya"]),
                    mapping(world_geometries.loc["Morocco"]),
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_less_than_20000_km2",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "country_polygons_less_than_20000_km2",
                        "min_area": 0,
                        "max_area": 20000,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test_with_between_20000_and_30000_km2",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "country_polygons_less_than_20000_km2",
                        "min_area": 20000,
                        "max_area": 30000,
                    },
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "positive_test_with_between_500000_and_600000_km2",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "country_polygons_between_500000_and_600000_km2",
                        "min_area": 500000,
                        "max_area": 600000,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test_with_between_300000_and_400000_km2",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "country_polygons_between_500000_and_600000_km2",
                        "min_area": 300000,
                        "max_area": 400000,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This dictionary contains metadata for display in the public gallery
    library_metadata = {
        "maturity": "experimental",  # "experimental", "beta", or "production"
        "tags": [
            "geospatial",
            "hackathon-22",
        ],  # Tags for this Expectation in the gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@mmi333",
        ],
        "requirements": ["geopandas", "shapely"],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.polygon_area"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = (
        "mostly",
        "min_area",
        "max_area",
        "crs",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "min_area": 0,
        "max_area": 0,
        "crs": "epsg:4326",
        "mostly": 1.0,
    }


if __name__ == "__main__":
    ExpectColumnValuesToBePolygonAreaBetween().print_diagnostic_checklist()
