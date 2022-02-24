"""
This is a template for creating custom ColumnMapExpectations.
For detailed instructions on how to use it, please see:
    https://docs.greatexpectations.io/docs/guides/expectations/creating_custom_expectations/how_to_create_custom_column_map_expectations
"""

import json
import math
from math import cos, pi, sqrt
from typing import Optional

from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
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
    metric_partial,
)
from great_expectations.expectations.metrics.import_manager import F, sa, sparktypes


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
                distances = column.apply(
                    lambda x, y=center_point: cls.fcc_projection(x, y)
                )
            elif unit == "miles":
                distances = column.apply(
                    lambda x, y=center_point: cls.fcc_projection(x, y) * 1.609344
                )
                range = range * 1.609344

        elif projection == "pythagorean":
            if unit == "kilometers":
                distances = column.apply(
                    lambda x, y=center_point: cls.pythagorean_projection(x, y)
                )
            elif unit == "miles":
                distances = column.apply(
                    lambda x, y=center_point: cls.pythagorean_projection(x, y)
                    * 1.609344
                )
                range = range * 1.609344

        return distances.le(range)

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    @metric_partial(
        engine=SqlAlchemyExecutionEngine,
        partial_fn_type=MetricPartialFunctionTypes.MAP_CONDITION_FN,
        domain_type=MetricDomainTypes.COLUMN,
    )
    def _sqlalchemy(
        cls,
        execution_engine: SqlAlchemyExecutionEngine,
        metric_domain_kwargs,
        metric_value_kwargs,
        metrics,
        runtime_configuration,
    ):
        (
            selectable,
            compute_domain_kwargs,
            accessor_domain_kwargs,
        ) = execution_engine.get_compute_domain(
            metric_domain_kwargs, MetricDomainTypes.COLUMN
        )

        column_name = accessor_domain_kwargs["column"]
        column = sa.column(column_name)
        center_point = accessor_domain_kwargs["center_point"]
        unit = accessor_domain_kwargs["unit"]
        range = accessor_domain_kwargs["range"]
        projection = accessor_domain_kwargs["projection"]

        engine = execution_engine.engine
        breakpoint()
        if projection == "fcc":
            if unit == "kilometers":
                points = engine.execute(
                    sa.select(column).select_from(selectable)
                ).collect()
                distances = F.udf(
                    lambda x, y=center_point: cls.fcc_projection(x, y),
                    sparktypes.FloatType(),
                )
            elif unit == "miles":
                distances = F.udf(
                    lambda x, y=center_point: cls.fcc_projection(x, y) * 1.609344,
                    sparktypes.FloatType(),
                )
                range = range * 1.609344

        return None

    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

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
                    lambda x, y=center_point: cls.fcc_projection(x, y),
                    sparktypes.FloatType(),
                )
            elif unit == "miles":
                distances = F.udf(
                    lambda x, y=center_point: cls.fcc_projection(x, y) * 1.609344,
                    sparktypes.FloatType(),
                )
                range = range * 1.609344

            return F.when(distances(column) < range, F.lit(True)).otherwise(
                F.lit(False)
            )

        elif projection == "pythagorean":
            if unit == "kilometers":
                distances = F.udf(
                    lambda x, y=center_point: cls.pythagorean_projection(x, y),
                    sparktypes.FloatType(),
                )
            elif unit == "miles":
                distances = F.udf(
                    lambda x, y=center_point: cls.pythagorean_projection(x, y)
                    * 1.609344,
                    sparktypes.FloatType(),
                )
                range = range * 1.609344

            return F.when(distances(column) < range, F.lit(True)).otherwise(
                F.lit(False)
            )

    @staticmethod
    def fcc_projection(loc1, loc2):
        """
        Calculates the distance in kilometers between two lat/lon points on an ellipsoidal earth
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

    @staticmethod
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
        delta_lat = (lat2 - lat1) * (pi / 180)
        delta_lon = (lon2 - lon1) * (pi / 180)

        radius = 6371.009

        distance = radius * sqrt((delta_lat**2) + (cos(mean_lat) * delta_lon) ** 2)

        return distance


# This class defines the Expectation itself
class ExpectColumnValuesToBeLatLonCoordinatesInRangeOfGivenPointOnSphere(
    ColumnMapExpectation
):
    """Expect values in a column to be tuples of (latitude, longitude) within a specified range of a given (latitude, longitude) point."""

    def validate_configuration(self, configuration: Optional[ExpectationConfiguration]):
        """
        Validates that a configuration has been set, and sets a configuration if it has yet to be set. Ensures that
        necessary configuration arguments have been provided for the validation of the expectation.
        Args:
            configuration (OPTIONAL[ExpectationConfiguration]): \
                An optional Expectation Configuration entry that will be used to configure the expectation
        Returns:
            True if the configuration has been validated successfully. Otherwise, raises an exception
        """

        # Setting up a configuration
        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

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
            ), "center_point must be a tuple of lat/lon floats"
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

        return True

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
            "test_backends": [
                {"backend": "pandas", "dialects": None},
                {"backend": "sqlalchemy", "dialects": ["sqlite", "postgresql"]},
            ],
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
            "only_for": "spark",
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
    ExpectColumnValuesToBeLatLonCoordinatesInRangeOfGivenPointOnSphere().print_diagnostic_checklist()
    # out = ExpectColumnValuesToBeLatLonCoordinatesInRangeOfGivenPointOnSphere().run_diagnostics()
    # breakpoint()
