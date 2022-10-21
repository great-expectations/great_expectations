from typing import Any, List, Optional, Union

import geopandas
from shapely.geometry import Point

from great_expectations.core import ExpectationValidationResult
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationConfiguration,
    InvalidExpectationConfigurationError,
)
from great_expectations.expectations.metrics import (
    ColumnMapMetricProvider,
    column_condition_partial,
)
from great_expectations.expectations.util import render_evaluation_parameter_string
from great_expectations.render.renderer.renderer import renderer
from great_expectations.render.types import (
    RenderedBulletListContent,
    RenderedGraphContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
)
from great_expectations.render.util import num_to_str, substitute_none_for_missing


# This class defines a Metric to support your Expectation.
# For most ColumnMapExpectations, the main business logic for calculation will live in this class.
class ColumnValuesReverseGeocodedLatLonContain(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.reverse_geocoded_lat_lon_contain"
    condition_value_keys = (
        "word",
        "provider",
    )

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, word, provider=None, **kwargs):
        column = column.apply(Point)

        def reverse(point):
            # lat lon to lon lat for reverse_geocode
            return Point(point.y, point.x)

        column = column.apply(reverse)
        reverse_geocoded_column = column.apply(geopandas.tools.reverse_geocode)

        # check if lowercase reverse geocoded string contains word
        return reverse_geocoded_column.apply(
            lambda x: word in x["address"].values[0].lower()
        )


# This class defines the Expectation itself
class ExpectColumnValuesReverseGeocodedLatLonToContain(ColumnMapExpectation):
    """Expect values in a column to be tuples of degree-decimal (latitude, longitude) which contain a specific word when reverse geocoded.

    Args:
        column (str): \
            The column name.


    Keyword Args:
        word (str) : \
            The word to check if it's contained in the reverse geocoded string.
            Must be a lowercase string.

        provider (str or geopy.geocoder): \
            The reverse geocoding service provider.
            Default: photon
            More info here: https://geopandas.org/en/stable/docs/reference/api/geopandas.tools.reverse_geocode.html

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

        word = configuration.kwargs["word"]

        try:
            assert word is not None, "word must be provided"
            assert isinstance(word, str), "word must be a string"
            assert word.islower(), "word must be a lowercase string"
        except AssertionError as e:
            raise InvalidExpectationConfigurationError(str(e))

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "lat_lon_in_new_york": [
                    (40.583455700, -74.149604800),
                    (40.713507800, -73.828313200),
                    (40.652600600, -73.949721100),
                    (40.789623900, -73.959893900),
                    (40.846650800, -73.878593700),
                ],
            },
            "tests": [
                {
                    "title": "basic_positive_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_in_new_york",
                        "word": "new york",
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "basic_negative_test",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_in_new_york",
                        "word": "california",
                        "mostly": 0.2,
                    },
                    "out": {"success": False},
                },
            ],
        },
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.reverse_geocoded_lat_lon_contain"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "mostly",
        "word",
        "provider",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
        "provider": None,
        "mostly": 1,
    }

    # This object contains metadata for display in the public Gallery
    library_metadata = {
        "tags": [
            "geospatial",
            "hackathon-2022",
        ],  # Tags for this Expectation in the Gallery
        "contributors": [  # Github handles for all contributors to this Expectation.
            "@mmi333",  # Don't forget to add your github handle here!
        ],
        "requirements": ["geopandas"],
    }

    @classmethod
    @renderer(renderer_type="renderer.prescriptive")
    @render_evaluation_parameter_string
    def _prescriptive_renderer(
        cls,
        configuration: ExpectationConfiguration = None,
        result: ExpectationValidationResult = None,
        language: str = None,
        runtime_configuration: dict = None,
        **kwargs,
    ) -> List[
        Union[
            dict,
            str,
            RenderedStringTemplateContent,
            RenderedTableContent,
            RenderedBulletListContent,
            RenderedGraphContent,
            Any,
        ]
    ]:
        runtime_configuration = runtime_configuration or {}
        include_column_name = runtime_configuration.get("include_column_name", True)
        include_column_name = (
            include_column_name if include_column_name is not None else True
        )
        styling = runtime_configuration.get("styling")
        params = substitute_none_for_missing(
            configuration.kwargs,
            [
                "column",
                "word",
                "provider",
                "mostly",
            ],
        )

        if params["mostly"] is None:
            template_str = "values must be lat lon and contain $word when reverse geocoded by $provider"
        else:
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if include_column_name:
            template_str = f"$column {template_str}"

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]


if __name__ == "__main__":
    ExpectColumnValuesReverseGeocodedLatLonToContain().print_diagnostic_checklist()
