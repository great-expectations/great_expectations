from typing import Dict, Optional

import pandas as pd
import pygeos as geos

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
class ColumnAggregateGeometryBoundingRadius(ColumnAggregateMetricProvider):

    # This is the id string that will be used to reference your Metric.
    metric_name = "column.geometry.minimum_bounding_radius"
    value_keys = (
        "column_shape_format",
        "diameter_flag",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):

        column_shape_format = kwargs.get("column_shape_format")

        # Load the column into a pygeos Geometry vector from numpy array (Series not supported).
        if column_shape_format == "wkt":
            shape_test = geos.from_wkt(column.to_numpy(), on_invalid="ignore")
        elif column_shape_format == "wkb":
            shape_test = geos.from_wkb(column.to_numpy(), on_invalid="ignore")
        elif column_shape_format == "xy":
            shape_df = pd.DataFrame(column.to_list(), columns=("x", "y"))
            shape_test = geos.points(shape_df.lon, y=shape_df.lat)
        else:
            raise NotImplementedError("Column values shape format not implemented.")

        shape_test = geos.union_all(shape_test)

        radius = geos.minimum_bounding_radius(shape_test)
        return radius

    # This method defines the business logic for evaluating your Metric when using a SqlAlchemyExecutionEngine
    # @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    # def _sqlalchemy(cls, column, _dialect, **kwargs):
    #     raise NotImplementedError
    #
    # This method defines the business logic for evaluating your Metric when using a SparkDFExecutionEngine
    # @column_aggregate_partial(engine=SparkDFExecutionEngine)
    # def _spark(cls, column, **kwargs):
    #     raise NotImplementedError


# This class defines the Expectation itself
class ExpectColumnMininumBoundingRadiusToBeBetween(ColumnExpectation):
    """
    Expect that column values as geometry points to be contained within a bounding circle with a given radius (or diameter).

    expect_column_values_minimum_bounding_radius_to_be_between is a :func:`column_expectation <great_expectations.dataset.dataset.MetaDataset.column_expectation>`.

    Args:
        column (str): \
            The column name.
            Column values must be provided in WKT or WKB format, which are commom formats for GIS Database formats.
            WKT can be accessed thhrough the ST_AsText() or ST_AsBinary() functions in queries for PostGIS and MSSQL.
            Column values can alternately be given in x,y tuple or list pairs.
            The user is responsible for the coordinate reference system and the units. e.g. values may be given in easting-northing pairs.
        min_value (float or None): \
            The minimum radius (or diameter) that bounds all geometries in the column
        max_value (float or None): \
            The maximum radius (or diameter) that bounds all geometries in the column
        strict_min (boolean): \
            If True, the minimal radius must be strictly larger than min_value,
            Default: False
        strict_max (boolean): \
            If True, the maximal radius must be strictly smaller than max_value,
            Default: False
    Keyword Args:
        column_shape_format: str
            Geometry format for 'column' (wkt, wkb, xy). Column values can be provided in WKT or WKB format, which are commom formats for GIS Database formats.
            xy also supports tuple pairs or list pairs for points only
            WKT can be accessed thhrough the ST_AsText() or ST_AsBinary() functions in queries for PostGIS and MSSQL.
            Must be one of: [wkt, wkb, xy]
            Default: wkt
        diameter_flag (boolean): \
            If True, the user can specify a diameter as opposed to a radius,
            Default: False

    Returns:
        An ExpectationSuiteValidationResult

    Notes:

        These fields in the result object are customized for this expectation:
        ::

            {
                "observed_value": (list) The actual bounding radius (or diameter)
            }


        * min_value and max_value are both inclusive unless strict_min or strict_max are set to True.
        * If min_value is None, then max_value is treated as an upper bound
        * If max_value is None, then min_value is treated as a lower bound
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "points_only": [
                    "POINT(1 1)",
                    "POINT(2 2)",
                    "POINT(6 4)",
                    "POINT(3 9)",
                    "POINT(5 5)",
                ],
                "points_and_lines": [
                    "POINT(1 1)",
                    "POINT(2 2)",
                    "POINT(6 4)",
                    "POINT(3 9)",
                    "LINESTRING(5 5, 8 10)",
                ],
            },
            "tests": [
                {
                    "title": "positive_test_with_points",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "points_only",
                        "column_shape_format": "wkt",
                        "min_value": None,
                        "max_value": 5,
                        "strict_min": False,
                        "strict_max": False,
                        "diameter_flag": False,
                    },
                    "out": {
                        "success": True,
                        # "result":{"observed_value":4.123105625617661}
                    },
                },
                {
                    "title": "positive_test_with_points_and_lines",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "points_and_lines",
                        "column_shape_format": "wkt",
                        "min_value": 5,
                        "max_value": 10,
                        "strict_min": True,
                        "strict_max": True,
                        "diameter_flag": False,
                    },
                    "out": {
                        "success": True,
                        # "result":{"observed_value":5.70087712549569}
                    },
                },
                {
                    "title": "negative positive_test_with_points_and_lines",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "points_and_lines",
                        "column_shape_format": "wkt",
                        "min_value": 1,
                        "max_value": 10,
                        "strict_min": False,
                        "strict_max": True,
                        "diameter_flag": True,
                    },
                    "out": {
                        "success": False,
                        # "result":{"observed_value":11.40175425099138}
                    },
                },
            ],
        }
    ]

    # This is a tuple consisting of all Metrics necessary to evaluate the Expectation.
    metric_dependencies = ("column.geometry.minimum_bounding_radius",)

    # This a tuple of parameter names that can affect whether the Expectation evaluates to True or False.
    success_keys = (
        "diameter_flag",
        "column_shape_format",
        "min_value",
        "strict_min",
        "max_value",
        "strict_max",
    )

    # This dictionary contains default values for any parameters that should have default values.
    default_kwarg_values = {
        "diameter_flag": False,
        "column_shape_format": "wkt",
    }

    def validate_configuration(
        self, configuration: Optional[ExpectationConfiguration]
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

    # This method performs a validation of your metrics against your success keys, returning a dict indicating the success or failure of the Expectation.
    def _validate(
        self,
        configuration: ExpectationConfiguration,
        metrics: Dict,
        runtime_configuration: dict = None,
        execution_engine: ExecutionEngine = None,
    ):
        radius = metrics.get("column.geometry.minimum_bounding_radius")
        diameter_flag = self.get_success_kwargs(configuration).get("diameter_flag")
        min_value = self.get_success_kwargs(configuration).get("min_value")
        max_value = self.get_success_kwargs(configuration).get("max_value")
        strict_min = self.get_success_kwargs(configuration).get("strict_min")
        strict_max = self.get_success_kwargs(configuration).get("strict_max")

        if diameter_flag:
            distance = radius * 2
        else:
            distance = radius

        # Evaluate the between statement (from column_values_between.py)
        if min_value is None:
            if strict_max:
                success = distance < max_value
            else:
                success = distance <= max_value

        elif max_value is None:
            if strict_min:
                success = min_value < distance
            else:
                success = min_value <= distance

        else:
            if strict_min and strict_max:
                success = (min_value < distance) & (distance < max_value)
            elif strict_min:
                success = (min_value < distance) & (distance <= max_value)
            elif strict_max:
                success = (min_value <= distance) & (distance < max_value)
            else:
                success = (min_value <= distance) & (distance <= max_value)

        return {"success": success, "result": {"observed_value": distance}}

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": ["hackathon-2022"],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@pjdobson",  # Don't forget to add your github handle here!
        ],
        "requirements": ["pygeos"],
    }


if __name__ == "__main__":
    ExpectColumnMininumBoundingRadiusToBeBetween().print_diagnostic_checklist()
