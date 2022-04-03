import json
from typing import Optional
import pygeos as geos
import geopy
import pandas as pd


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
class ColumnValuesGeometryDistanceToAddress(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.geometry.distance_to_address"
    condition_value_keys = ("column_shape_format", "place", "geocoder", "geocoder_config", "min_value", "max_value", "strict_min", "strict_max","units")

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs): 
        
        column_shape_format = kwargs.get("column_shape_format")
        place = kwargs.get("place")
        geocoder = kwargs.get("geocoder")
        geocoder_config = kwargs.get("geocoder_config")
        min_value = kwargs.get("min_value")
        max_value = kwargs.get("max_value")
        strict_min = kwargs.get("strict_min")
        strict_max = kwargs.get("strict_max")
        units = kwargs.get("units")
        
        if min_value is None and max_value is None:
            raise ValueError("min_value and max_value cannot both be None")
        if min_value is not None and max_value is not None and min_value > max_value:
            raise ValueError("min_value cannot be greater than max_value")
        
        
        if geocoder not in ['nominatim', 'pickpoint', 'openmapquest']:
            raise NotImplementedError("The geocoder is not implemented for this method.")
        
        #find the reference shape with the geocoder.
        if geocoder is not None:
            try:
                #Specify the default parameters for Nominatim and run query. User is responsible for config and query params otherwise.
                query_params = dict(exactly_one=True, geometry = 'wkt')
                location = cls.geocode(geocoder, geocoder_config, place, query_params)
            except:
                raise Exception("Geocoding configuration and query failed to produce a valid result.")
        else:
            raise Exception("A valid geocoder must be provided for this method. See GeoPy for reference.")

    
        # Load the column into a pygeos Geometry vector from numpy array (Series not supported).
        if column_shape_format == 'wkt':
            shape_test = geos.from_wkt(column.to_numpy(), on_invalid='ignore')
        elif column_shape_format == 'wkb':
            shape_test = geos.from_wkb(column.to_numpy(), on_invalid='ignore')
        else:
            raise NotImplementedError("Column values shape format not implemented.")
        
        
        #verify that all shapes are points and if not, convert to centroid point.
        points_test = pd.Series(shape_test)
        if not points_test.apply(lambda x: geos.get_type_id(x) == 0).all():
            points_test = points_test.map(geos.centroid)
            
        if location is None:
             raise Exception("Geocoding failed to return a result.")
        else:
            point_ref = geopy.distance.lonlat(location.longitude, location.latitude)
             
        #calculate the distance between the points using geopy
        #TODO: Implement unit conversion
        if units not in ['km', 'kilometers']:
            raise NotImplementedError("Unit conversion has not yet been implemented. Please use km.")
        
        column_dist = points_test.apply(lambda p: geopy.distance.distance(p,point_ref).km)
        
        
        #Evaluate the between statement (from column_values_between.py)
        if min_value is None:
            if strict_max:
                return column_dist < max_value
            else:
                return column_dist <= max_value

        elif max_value is None:
            if strict_min:
                return min_value < column_dist
            else:
                return min_value <= column_dist

        else:
            if strict_min and strict_max:
                return (min_value < column_dist) & (column_dist < max_value)
            elif strict_min:
                return (min_value < column_dist) & (column_dist <= max_value)
            elif strict_max:
                return (min_value <= column_dist) & (column_dist < max_value)
            else:
                return (min_value <= column_dist) & (column_dist <= max_value)


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
    Expect that column values as geometry points to be between a certain distance from a geocoded object. 
    
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
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "point_array_michigan": ['POINT(-83.05091885970701 42.33652958471724)', 'POINT(-85.67021291198213 42.96702932962047)',  'POINT(-85.95743932240455 45.02382817778478)', 'POINT(-85.13547392026877 45.84546038186437)',  'POINT(-87.66468654829194 47.41967043499885)'],
                "point_array_lake_michigan": ['POINT(-87.57235899196583 41.91932821084754)', 'POINT(-85.67021291198213 42.96702932962047)',  'POINT(-85.95743932240455 45.02382817778478)', 'POINT(-85.13547392026877 45.84546038186437)',  'POINT(-87.66468654829194 47.41967043499885)']
            },
            "tests": [
                {
                    "title": "positive_test_with_points_in_michigan",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "point_array_michigan",
                        "place": 'Michigan', 
                    },
                    "out": {
                        "success": True,
                    },
                },

                {
                    "title": "positive_test_with_points_in_lake_michigan",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "point_array_lake_michigan",
                        "place": 'Lake Michigan', 
                    },
                    "out": {
                        "success": False,
                        "unexpected_index_list": [1, 4]
                    },
                },
                
            ],
        }
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.geometry.distance_to_address"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = ("mostly", "column_shape_format", "place", "geocoder", "geocoder_config", "min_value", "max_value", "units")

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        'mostly': 1,
        'column_shape_format':'wkt',
        'geocoder': 'nominatim',
        'geocoder_config': dict(user_agent="great_expectations.hacakthon-2022"),
        'units':'km'
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

        min_val = None
        max_val = None
        if "min_value" in configuration.kwargs:
            min_val = configuration.kwargs["min_value"]
        if "max_value" in configuration.kwargs:
            max_val = configuration.kwargs["max_value"]
        assert (
            min_val is not None or max_val is not None
        ), "min_value and max_value cannot both be None"        

        return True

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": ["geospatial",
                 "hackathon-2022"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@pjdobson",  # Don't forget to add your github handle here!
        ],
        "requirements": ['pygeos', 'geopy']
    }


if __name__ == "__main__":
    ExpectColumnValuesGeometryToBeWithinPlace().print_diagnostic_checklist()
