"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

import json

from shapely.geometry import Point, Polygon

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


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesCoordinatesDegreeDecimal(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.coordinates.degree_decimal"

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        bounds = Polygon.from_bounds(xmin=-180.0, ymin=-90.0, xmax=180.0, ymax=90.0)
        return column.apply(lambda x, y=bounds: cls._point_in_bounds(x, y))

    @staticmethod
    def _point_in_bounds(point, bounds):
        try:
            result = bounds.intersects(
                Point(float(eval(point)[1]), float(eval(point)[0]))
            )
        except TypeError:
            try:
                result = bounds.intersects(Point(float(point[1]), float(point[0])))
            except TypeError:
                return False

        return result

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesToBeValidDegreeDecimalCoordinates(ColumnMapExpectation):
    """Expect column values to contain degree-decimal, lat/lon coordinates."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "tuple_float": [
                    (62.75955799999999, -164.483752),
                    (62.7673475, -164.4996625),
                    (62.7698675, -164.5034575),
                    (62.76901333333333, -164.50339),
                    (62.76906333333334, -164.50353333333337),
                ],
                "mixed_types": [
                    "[62.75955799999999, -164.483752]",
                    [62.7673475, -164.4996625],
                    (62.7698675, -164.5034575),
                    "(62.76901333333333, -164.50339)",
                    True,
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "tuple_float"},
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {"column": "mixed_types", "mostly": 1},
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.coordinates.degree_decimal"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly",)

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-22",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@austiezr",  # Don't forget to add your github handle here!
        ],
        "requirements": ["shapely"],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeValidDegreeDecimalCoordinates().print_diagnostic_checklist()
