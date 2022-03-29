"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

import json
import math
from math import cos, pi, sqrt
from typing import Optional

import numpy as np

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
)
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    MetricDomainTypes,
    MetricPartialFunctionTypes,
    column_condition_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sparktypes


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesAreLatLonCoordinatesInRange(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.coordinates.in_range"
    condition_value_keys = ("center_point", "range", "unit", "projection")

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        center_point = kwargs.get("center_point")
        unit = kwargs.get("unit")
        range = kwargs.get("range")
        projection = kwargs.get("projection")

        if projection == "fcc":
            if unit == "kilometers":
                distances = column.apply(lambda x, y=center_point: fcc_projection(x, y))
            elif unit == "miles":
                distances = column.apply(
                    lambda x, y=center_point: fcc_projection(x, y) * 1.609344
                )
                range = range * 1.609344

        elif projection == "pythagorean":
            if unit == "kilometers":
                distances = column.apply(
                    lambda x, y=center_point: pythagorean_projection(x, y)
                )
            elif unit == "miles":
                distances = column.apply(
                    lambda x, y=center_point: pythagorean_projection(x, y) * 1.609344
                )
                range = range * 1.609344

        return distances.le(range)

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    @column_condition_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, **kwargs):
        center_point = kwargs.get("center_point")
        unit = kwargs.get("unit")
        range = kwargs.get("range")
        projection = kwargs.get("projection")

        if projection == "fcc":
            if unit == "kilometers":
                distances = F.udf(
                    lambda x, y=center_point: fcc_projection(x, y),
                    sparktypes.FloatType(),
                )
            elif unit == "miles":
                distances = F.udf(
                    lambda x, y=center_point: fcc_projection(x, y) * 1.609344,
                    sparktypes.FloatType(),
                )
                range = range * 1.609344

            return F.when(distances(column) < range, F.lit(True)).otherwise(
                F.lit(False)
            )

        elif projection == "pythagorean":
            if unit == "kilometers":
                distances = F.udf(
                    lambda x, y=center_point: pythagorean_projection(x, y),
                    sparktypes.FloatType(),
                )
            elif unit == "miles":
                distances = F.udf(
                    lambda x, y=center_point: pythagorean_projection(x, y) * 1.609344,
                    sparktypes.FloatType(),
                )
                range = range * 1.609344

            return F.when(distances(column) < range, F.lit(True)).otherwise(
                F.lit(False)
            )


def fcc_projection(loc1, loc2):
    """
    Application of the Pythagorean theorem to calculate the distance in kilometers between two lat/lon points on an ellipsoidal earth
    projected to a plane, as prescribed by the FCC for distances not exceeding 475km/295mi.

    See https://www.govinfo.gov/content/pkg/CFR-2016-title47-vol4/pdf/CFR-2016-title47-vol4-sec73-208.pdf

    :param loc1: A tuple of lat/lon
    :param loc2: A tuple of lat/lon
    :return distance: Distance between points in km
    """

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


def pythagorean_projection(loc1, loc2):
    """
    Application of the pythagorean theorem to calculate the distance in kilometers between two lat/lon points on
    a spherical earth projected to a plane.

    :param loc1: A tuple of lat/lon
    :param loc2: A tuple of lat/lon
    :return distance: Distance between points in km
    """

    lat1, lat2 = float(loc1[0]), float(loc2[0])
    lon1, lon2 = float(loc1[1]), float(loc2[1])

    mean_lat = (lat1 + lat2) / 2
    delta_lat = (lat2 - lat1) * (pi / 180)  # converting from degree-decimal to radians
    delta_lon = (lon2 - lon1) * (pi / 180)  # converting from degree-decimal to radians

    radius = 6371.009

    distance = radius * sqrt((delta_lat**2) + (cos(mean_lat) * delta_lon) ** 2)

    return distance


# This class defines the Expectation itself
class ExpectColumnValuesToBeLatLonCoordinatesInRangeOfGivenPoint(ColumnMapExpectation):
    """Expect values in a column to be tuples of degree-decimal (latitude, longitude) within a specified range of a given degree-decimal (latitude, longitude) point.

    expect_column_values_to_be_lat_lon_coordinates_in_range_of_given_point is a :func:`column_map_expectation \
    <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.


    Keyword Args:
        center_point (tuple(float, float) or list(float, float)): \
            The point from which to measure to the column points.
            Must be a tuple or list of exactly two (2) floats.

        unit (str or None): \
            The unit of distance with which to measure.
            Must be one of: [miles, kilometers]
            Default: kilometers

        range (int or float): \
            The range in [miles, kilometers] from your specified center_point to measure.

        projection (str or None): \
            The method by which to calculate distance between points.
            Must be one of: [fcc, pythagorean]
            Default: pythagorean

    Other Parameters:
        result_format (str or None): \
            Which output mode to use: `BOOLEAN_ONLY`, `BASIC`, `COMPLETE`, or `SUMMARY`.
            For more detail, see :ref:`result_format <result_format>`.
        include_config (boolean): \
            If True, then include the expectation config as part of the result object. \
            For more detail, see :ref:`include_config`.
        catch_exceptions (boolean or None): \
            If True, then catch exceptions and include them as part of the result object. \
            For more detail, see :ref:`catch_exceptions`.
        meta (dict or None): \
            A JSON-serializable dictionary (nesting allowed) that will be included in the output without
            modification. For more detail, see :ref:`meta`.

    Returns:
        An ExpectationSuiteValidationResult

        Exact fields vary depending on the values passed to :ref:`result_format <result_format>` and
        :ref:`include_config`, :ref:`catch_exceptions`, and :ref:`meta`.

    Projections:
        fcc: \
            Calculates the distance in kilometers between two lat/lon points on an ellipsoidal earth
            projected to a plane. Prescribed by the FCC for distances up to and not exceeding 475km/295mi.

        pythagorean: \
            Calculates the distance in kilometers between two lat/lon points on
            a spherical earth projected to a plane. Very fast but error increases rapidly as distances increase.
    """

    def validate_configuration(
        cls, configuration: Optional[ExpectationConfiguration]
    ) -> None:
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.
        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            None. Raises InvalidExpectationConfigurationError if the config is not validated successfully
        """

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = cls.configuration

        center_point = configuration.kwargs["center_point"]
        range = configuration.kwargs["range"]
        unit = configuration.kwargs["unit"]
        projection = configuration.kwargs["projection"]

        try:
            assert (
                center_point is not None and range is not None
            ), "center_point and range must be specified"
            assert (
                isinstance(center_point, tuple) or isinstance(center_point, list)
            ) and all(
                isinstance(n, float) for n in center_point
            ), "center_point must be a tuple or list of lat/lon floats"
            assert (center_point[0] >= -90 and center_point[0] <= 90) and (
                center_point[1] >= -180 and center_point[1] <= 180
            ), "center_point must be a valid lat/lon pair"
            assert isinstance(range, (float, int)), "range must be a numeric value"
            assert isinstance(unit, str) and unit in [
                "miles",
                "kilometers",
            ], "unit must be a string specifying miles or kilometers"
            assert isinstance(projection, str) and projection in [
                "fcc",
                "pythagorean",
            ], "projection must be a string specifying fcc or pythagorean"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

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
                "mixed_types": [
                    (62.75955799999999, -164.483752),
                    ("62.7673475", "-164.4996625"),
                    True,
                    5,
                    "null",
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon",
                        "center_point": (62.7596, -164.484),
                        "range": 8,
                        "unit": "miles",
                        "projection": "fcc",
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon",
                        "range": 8,
                        "center_point": (61.7668, -162.578),
                        "unit": "kilometers",
                        "projection": "pythagorean",
                        "mostly": 0.2,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "bad_type_error_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "mixed_types",
                        "range": 8,
                        "center_point": (62.7596, -164.484),
                        "unit": "kilometers",
                        "projection": None,
                        "mostly": 1,
                        "catch_exceptions": True,
                    },
                    "out": {},
                    "error": {"traceback_substring": "bad operand type for unary ~:"},
                },
                {
                    "title": "bad_range_parameter_input",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon",
                        "range": "8",
                        "center_point": (62.7596, -164.484),
                        "unit": "kilometers",
                        "projection": "fcc",
                        "catch_exceptions": True,
                    },
                    "out": {},
                    "error": {"traceback_substring": "range must be a numeric value"},
                },
            ],
            "only_for": ["pandas"],
        },
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
                        "center_point": (62.7596, -164.484),
                        "range": 8,
                        "unit": "miles",
                        "projection": "fcc",
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon",
                        "range": 8,
                        "center_point": (61.7668, -162.578),
                        "unit": "kilometers",
                        "projection": "pythagorean",
                        "mostly": 0.2,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "bad_range_parameter_input",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon",
                        "range": "8",
                        "center_point": (62.7596, -164.484),
                        "unit": "kilometers",
                        "projection": "fcc",
                        "catch_exceptions": True,
                    },
                    "out": {},
                    "error": {"traceback_substring": "range must be a numeric value"},
                },
            ],
            "only_for": ["spark"],
        },
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.coordinates.in_range"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly", "center_point", "projection", "range", "unit")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "projection": "pythagorean",
        "unit": "kilometers",
        "mostly": 1,
    }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-2022",
            "distance projection",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@austiezr",  # Don't forget to add your github handle here!
        ],
    }


if __name__ == "__main__":
    ExpectColumnValuesToBeLatLonCoordinatesInRangeOfGivenPoint().print_diagnostic_checklist()
