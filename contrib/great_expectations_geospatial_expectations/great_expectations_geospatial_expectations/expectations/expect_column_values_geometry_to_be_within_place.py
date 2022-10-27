from typing import Optional

import geopy
import pandas as pd
import pygeos as geos

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
class ColumnValuesGeometryWithinPlace(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.geometry.within_place"
    condition_value_keys = (
        "column_shape_format",
        "place",
        "geocoder",
        "geocoder_config",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        column_shape_format = kwargs.get("column_shape_format")
        place = kwargs.get("place")
        geocoder = kwargs.get("geocoder")
        geocoder_config = kwargs.get("geocoder_config")

        if geocoder not in ["nominatim", "pickpoint", "openmapquest"]:
            raise NotImplementedError(
                "The geocoder is not implemented for this method."
            )

        # find the reference shape with the geocoder.
        if geocoder is not None:
            try:
                # Specify the default parameters for Nominatim and run query. User is responsible for config and query params otherwise.
                query_params = dict(exactly_one=True, geometry="wkt")
                location = cls.geocode(geocoder, geocoder_config, place, query_params)
            except:
                raise Exception(
                    "Geocoding configuration and query failed to produce a valid result."
                )
        else:
            raise Exception(
                "A valid geocoder must be provided for this method. See GeoPy for reference."
            )

        # This method only works with the default Nominatim params and wkt.
        # TODO: Other geocoders and methods need to be implemented.
        # TODO: Add a conversion from lat-long to a CRS. (geopandas)
        if location is not None:
            shape_ref = geos.from_wkt(location.raw.get("geotext"))
        else:
            raise Exception("Geocoding failed to return a result.")

        # Load the column into a pygeos Geometry vector from numpy array (Series not supported).
        if column_shape_format == "wkt":
            shape_test = geos.from_wkt(column.to_numpy(), on_invalid="ignore")
        elif column_shape_format == "wkb":
            shape_test = geos.from_wkb(column.to_numpy(), on_invalid="ignore")
        else:
            raise NotImplementedError("Column values shape format not implemented.")

        # Prepare the geometries
        geos.prepare(shape_ref)
        geos.prepare(shape_test)

        # Return whether the distance is below the tolerance.
        return pd.Series(geos.contains(shape_ref, shape_test))

    @staticmethod
    def geocode(geocoder, config, query, query_config):
        cls = geopy.geocoders.get_geocoder_for_service(geocoder)
        geolocator = cls(**config)
        location = geolocator.geocode(query, **query_config)
        return location

    # This method defines the business logic for evaluating your metric when using a SqlAlchemyExecutionEngine
    # @column_condition_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError

    # This method defines the business logic for evaluating your metric when using a SparkDFExecutionEngine
    # @column_condition_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnValuesGeometryToBeWithinPlace(ColumnMapExpectation):
    """
    Expect that column values as geometries are within a place that can be returned through geocoding (as a shape)

    expect_column_values_geometry_to_be_near_shape is a :func:`column_map_expectation <great_expectations.dataset.dataset.MetaDataset.column_map_expectation>`.

    Args:
        column (str): \
            The column name.
            Column values must be provided in WKT or WKB format, which are commom formats for GIS Database formats.
            WKT can be accessed thhrough the ST_AsText() or ST_AsBinary() functions in queries for PostGIS and MSSQL.
            Values must be in longitude - latitude format for this method to work.

    Keyword Args:
        place: str
            The country, place, address, etc. to query. Expect to return a geometry from OpenStreetMaps (Nominatim)

        column_shape_format: str
            Geometry format for 'column'. Column values must be provided in WKT or WKB format, which are commom formats for GIS Database formats.
            WKT can be accessed thhrough the ST_AsText() or ST_AsBinary() functions in queries for PostGIS and MSSQL.

        geocoder: str
            Geocoder from GeoPy to use to return the shape. While this is generic, the api is required to be available from GeoPy and must return a geometry.

        geocoder_config: dict(str)
            arguments to initialize the GeoPy geocoder. e.g. for paid services, an API_key is usually required. See GeoPy for reference.

    Returns:
        An ExpectationSuiteValidationResult

    Notes:
        The user is responsible to transform the column to a WKT or WKB format that is in the WGS84 coordianate system for earth.
        Other Coordinate Reference Systems are not yet supported.
        See Nominatim for appropriate use of the service as open-source. Requests are subject to timeout and blocking.
        Frequent queries should be cached locally and use (expect_column_value_geometries_to_be_within_shape)
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "point_array_michigan": [
                    "POINT(-83.05091885970701 42.33652958471724)",
                    "POINT(-85.67021291198213 42.96702932962047)",
                    "POINT(-85.95743932240455 45.02382817778478)",
                    "POINT(-85.13547392026877 45.84546038186437)",
                    "POINT(-87.66468654829194 47.41967043499885)",
                ],
                "point_array_lake_michigan": [
                    "POINT(-87.57235899196583 41.91932821084754)",
                    "POINT(-85.67021291198213 42.96702932962047)",
                    "POINT(-85.95743932240455 45.02382817778478)",
                    "POINT(-85.13547392026877 45.84546038186437)",
                    "POINT(-87.66468654829194 47.41967043499885)",
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_points_in_michigan",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "point_array_michigan",
                        "place": "Michigan",
                    },
                    "out": {
                        "success": True,
                    },
                },
                {
                    "title": "negative_test_with_points_in_lake_michigan",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "point_array_lake_michigan",
                        "place": "Lake Michigan",
                    },
                    "out": {"success": False, "unexpected_index_list": [1, 4]},
                },
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.geometry.within_place"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "mostly",
        "column_shape_format",
        "place",
        "geocoder",
        "geocoder_config",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "mostly": 1,
        "column_shape_format": "wkt",
        "geocoder": "nominatim",
        "geocoder_config": dict(user_agent="great_expectations.hacakthon-2022"),
    }

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

        super().validate_configuration(configuration)
        if configuration is None:
            configuration = self.configuration

        # # Check other things in configuration.kwargs and raise Exceptions if needed
        # try:
        #     assert (
        #         ...
        #     ), "message"
        #     assert (
        #         ...
        #     ), "message"
        # except AssertionError as e:
        #     raise InvalidExpectationConfigurationError(str(e))

        return True

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-2022",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@pjdobson",  # Don't forget to add your github handle here!
        ],
        "requirements": ["pygeos", "geopy"],
    }


if __name__ == "__main__":
    ExpectColumnValuesGeometryToBeWithinPlace().print_diagnostic_checklist()
