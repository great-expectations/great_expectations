from typing import Any, List, Union

from global_land_mask import globe

from great_expectations.core import ExpectationValidationResult
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.expectations.expectation import (
    ColumnMapExpectation,
    ExpectationConfiguration,
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
class ColumnValuesLatLonLandOrOcean(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.lat_lon_land_or_ocean"
    condition_value_keys = ("land_or_ocean",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, land_or_ocean="land", **kwargs):

        if land_or_ocean == "land":
            return column.apply(lambda point: globe.is_land(point[0], point[1]))
        elif land_or_ocean == "ocean":
            return column.apply(lambda point: globe.is_ocean(point[0], point[1]))
        else:
            raise ValueError("land_or_ocean must be 'land' or 'ocean'")


# This class defines the Expectation itself
class ExpectColumnValuesLatLonToBeLandOrOcean(ColumnMapExpectation):
    """Expect values in a column to be lat lon pairs that represent a point on land or in an ocean.

    Args:
        column (str): \
            The column name.

        land_or_ocean (str): \
            Either 'land' or 'ocean'. \
            represents whether to check if each point is on land or in an ocean.


    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "lat_lon_on_land": [
                    (32.699316, -117.063457),
                    (33.570321, -116.884380),
                    (33.598757, -117.721397),
                ],
                "lat_lon_in_ocean": [
                    (20.699316, -117.063457),
                    (50.699316, -45.063457),
                    (-3.699316, 45.063457),
                ],
            },
            "tests": [
                {
                    "title": "positive_for_lat_lon_on_land",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_on_land",
                        "land_or_ocean": "land",
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative_for_lat_lon_on_land",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_on_land",
                        "land_or_ocean": "ocean",
                        "mostly": 0.2,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "positive_for_lat_lon_in_ocean",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_in_ocean",
                        "land_or_ocean": "ocean",
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative_for_lat_lon_in_ocean",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "lat_lon_in_ocean",
                        "land_or_ocean": "land",
                        "mostly": 0.2,
                    },
                    "out": {"success": False},
                },
            ],
        },
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.lat_lon_land_or_ocean"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "mostly",
        "land_or_ocean",
    )

    # This dictionary contains default values for any parameters that should have default values
    default_kwarg_values = {
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
        "requirements": ["global-land-mask"],
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
                "mostly",
            ],
        )

        template_str = "values must be lat lon pairs that represent a point"

        if params["land_or_ocean"] == "land":
            template_str += " on land"
        elif params["land_or_ocean"] == "ocean":
            template_str += " in an ocean"

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
    ExpectColumnValuesLatLonToBeLandOrOcean().print_diagnostic_checklist()
