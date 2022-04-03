import json
from typing import Optional
import geopandas
from shapely.geometry import LineString

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnMapExpectation
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)


# This class defines a Metric to support your Expectation
# For most Expectations, the main business logic for calculation will live here.
# To learn about the relationship between Metrics and Expectations, please visit {some doc}.
class ColumnValuesLinestringKMDistanceBetween(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    # Please see {some doc} for information on how to choose an id string for your Metric.
    condition_metric_name = "column_values.linestring_distance"
    condition_value_keys = (
        "min_distance",
        "max_distance",
    )

    # This method defines the business logic for evaluating your metric when using a PandasExecutionEngine

    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, min_distance, max_distance, **kwargs):
        column = geopandas.GeoSeries(column)
        # Set crs to meters
        column = column.to_crs({"proj": "cea"})
        #access the length of the column
        col_len = column.length
        col_len
        return (col_len >= min_distance) & (col_len <= max_distance)


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
class ExpectColumnValuesToBeLinestringKMDistanceBetween(ColumnMapExpectation):
    """This expectation will compute the distance of Linestring
    in kilometers and check if it's between two values."""


    # These examples will be shown in the public gallery, and also executed as unit tests for your Expectation
    examples = [
        {
            "data": {
                "linestring_less_than_1000_km": [
                    "LINESTRING (0 0, 111319.490793 110568.8124, 0 110568.8124)",
                    "LINESTRING (0 0, 111319.490793 110568.8124, 111319.490793 0, 0 110568.8124)",
                    "LINESTRING (0 0, 222638.981587 221104.845779, 222638.981587 0)"
                ],
                "linestring_between_1000_and_2000_km": [
                    "LINESTRING (222638.981587 552188.640112, 111319.490793 772147.013102, 1113194.907933 1209055.279421)",
                    "LINESTRING (111319.490793 881798.964757, 1001875.417139 221104.845779, 111319.490793 0, 556597.453966 1317466.085138)",
                    "LINESTRING (556597.453966 552188.640112, 1224514.398726 1209055.279421, 779236.435553 881798.964757)"
                ],
            },
            "tests": [
                {
                    "title": "linestring_less_than_1000_km",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "",
                        "min_distance": 0,
                        "max_distance": 1000,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "linestring_between_1000_and_2000_km",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "",
                        "min_distance": 1000,
                        "max_distance": 2000,
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
            "@luismdiaz01","@derekma73"
        ],
    }

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.linestring_distance"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    # Please see {some doc} for more information about domain and success keys, and other arguments to Expectations
    success_keys = (
        "mostly",
        "min_distance",
        "max_distance",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "min_distance": 0,
        "max_distance": 0,
        "mostly": 1.0,
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeLinestringKMDistanceBetween().print_diagnostic_checklist()