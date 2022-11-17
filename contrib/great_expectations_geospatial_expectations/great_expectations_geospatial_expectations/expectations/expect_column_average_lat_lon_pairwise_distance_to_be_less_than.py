from math import radians
from typing import Dict

import numpy as np
from scipy.spatial.distance import pdist
from sklearn.metrics.pairwise import haversine_distances

from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.execution_engine import ExecutionEngine, PandasExecutionEngine
from great_expectations.expectations.expectation import ColumnExpectation
from great_expectations.expectations.metrics import (
    ColumnAggregateMetricProvider,
    column_aggregate_value,
)


# This class defines a Metric to support your Expectation.
# For most ColumnExpectations, the main business logic for calculation will live in this class.
class ColumnAverageLatLonPairwiseDistance(ColumnAggregateMetricProvider):

    metric_name = "column.average_lat_lon_pairwise_distance"
    value_keys = ()

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        # convert everything to arrays for pairwise distance computation
        column_array = np.array([np.array([point[0], point[1]]) for point in column])

        result = pdist(column_array, cls.haversine_adapted).mean()

        return result

    @staticmethod
    def haversine_adapted(point_1, point_2):
        # lat lon to radians for haversine
        point_1 = [radians(_) for _ in point_1]
        point_2 = [radians(_) for _ in point_2]

        result = haversine_distances([point_1, point_2])
        # convert to km
        result *= 6371000 / 1000
        # result is a 2d distance matrix,
        #  0, dist
        #  dist, 0
        return result[0][1]


# This class defines the Expectation itself
class ExpectColumnAverageLatLonPairwiseDistanceToBeLessThan(ColumnExpectation):
    """This expectation will compute the pairwise haversine distance between each (latitude, longitude) pair
    and test that the average is less than some value in km."""

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "mostly_points_within_geo_region_PER": [
                    (-12.0464, -77.0428),
                    (-13.163068, -72.545128),
                    (-9.18997, -75.01515),
                    (55.378051, -3.435973),
                ],
                "mostly_points_within_geo_region_GBR": [
                    (-12.0464, -77.0428),
                    (-13.163068, -72.545128),
                    (53.4808, 2.2426),
                    (55.378051, -3.435973),
                ],
                "mostly_points_within_geo_region_US": [
                    (33.570321, -116.884380),
                    (32.699316, -117.063457),
                    (32.699316, -117.063457),
                    (33.598757, -117.721397),
                ],
            },
            "tests": [
                {
                    "title": "positive_test_within_100km",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_points_within_geo_region_US",
                        "max_distance": 100,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test_within_50km",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_points_within_geo_region_US",
                        "max_distance": 50,
                    },
                    "out": {
                        "success": False,
                    },
                },
                {
                    "title": "positive_test_within_7000km",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_points_within_geo_region_GBR",
                        "max_distance": 7000,
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test_within_1000km",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mostly_points_within_geo_region_PER",
                        "max_distance": 1000,
                    },
                    "out": {
                        "success": False,
                    },
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.average_lat_lon_pairwise_distance",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = ("max_distance",)

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
        distance = metrics.get("column.average_lat_lon_pairwise_distance")
        max_distance = self.get_success_kwargs(configuration).get("max_distance")
        success = distance < max_distance
        return {"success": success, "result": {"observed_value": distance}}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-22",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@mmi333",  # Don't forget to add your github handle here!
        ],
        "requirements": ["scipy", "scikit-learn", "numpy"],
    }


if __name__ == "__main__":
    ExpectColumnAverageLatLonPairwiseDistanceToBeLessThan().print_diagnostic_checklist()
