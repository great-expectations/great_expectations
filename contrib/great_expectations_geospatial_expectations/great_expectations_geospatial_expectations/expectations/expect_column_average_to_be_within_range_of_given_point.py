"""
This is a template for creating custom ColumnExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_aggregate_expectations
"""

from math import cos, sqrt
from statistics import mean
from typing import Dict, Optional

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.exceptions import InvalidExpectationConfigurationError
from great_expectations.execution_engine import (
    ExecutionEngine,
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)


# This class defines a Metric to support your Expectation.
# For most ColumnExpectations, the main business logic for calculation will live in this class.
class ColumnCoordinatesDistance(ColumnAggregateMetricProvider):

    # This is the id string that will be used to reference your Metric.
    metric_name = "column.coordinates.distance"
    value_keys = ("center_point",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        center_point = kwargs.get("center_point")

        avg_lat = mean([point[0] for point in column])
        avg_lon = mean([point[1] for point in column])

        distance = cls.fcc_projection((avg_lat, avg_lon), center_point)

        return distance

    @staticmethod
    def fcc_projection(loc1, loc2):
        lat1, lat2 = float(loc1[0]), float(loc2[0])
        lon1, lon2 = float(loc1[1]), float(loc2[1])

        mean_lat = (lat1 + lat2) / 2
        delta_lat = lat2 - lat1
        delta_lon = lon2 - lon1

        k1 = 111.13209 - (0.56605 * cos(2 * mean_lat)) + (0.0012 * cos(4 * mean_lat))
        k2 = (
            (111.41513 * cos(mean_lat))
            - (0.09455 * cos(3 * mean_lat))
            + (0.00012 * cos(5 * mean_lat))
        )

        distance = sqrt((k1 * delta_lat) ** 2 + (k2 * delta_lon) ** 2)

        return distance


# This class defines the Expectation itself
class ExpectColumnAverageToBeWithinRangeOfGivenPoint(ColumnExpectation):
    """Expect the average of a column of degree-decimal, lat/lon coordinates to be in range of a given point."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "lat_lon": [
                    (62.75955799999999, -164.483752),
                    (62.7673475, -164.4996625),
                    (62.7698675, -164.5034575),
                    (62.76901333333333, -164.50339),
                    (62.76906333333334, -164.50353333333337),
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon",
                        "center_point": (62.7597, -164.484),
                        "range": 8,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon",
                        "center_point": (72.7597, -161.484),
                        "range": 8,
                    },
                    "out": {"success": False},
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.coordinates.distance",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("range", "center_point")

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {}

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        distance = metrics.get("column.coordinates.distance")
        range = self.get_success_kwargs(configuration).get("range")

        success = distance <= range

        return {"success": success, "result": {"observed_value": distance}}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-22",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@austiezr",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnAverageToBeWithinRangeOfGivenPoint().print_diagnostic_checklist()
