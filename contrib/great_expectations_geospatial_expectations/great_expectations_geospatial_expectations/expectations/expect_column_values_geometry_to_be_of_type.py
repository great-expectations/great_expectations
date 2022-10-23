from typing import Any, List, Union

from shapely.geometry import LineString, MultiPolygon, Point, Polygon, mapping, shape

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
class ColumnValuesGeometryOfType(ColumnMapMetricProvider):

    # This is the id string that will be used to reference your metric.
    condition_metric_name = "column_values.geometry_of_type"
    condition_value_keys = ("geom_types_list",)

    # This method implements the core logic for the PandasExecutionEngine
    @column_condition_partial(engine=PandasExecutionEngine)
    def _pandas(cls, column, geom_types_list, **kwargs):
        # We use the mapping and shape functions to serialize and deserialize the geometry objects
        column = column.apply(shape)
        return column.apply(lambda value: value.geom_type in geom_types_list)


# This class defines the Expectation itself
class ExpectColumnValuesGeometryToBeOfType(ColumnMapExpectation):
    """Expect values in a column to belong to one of the specified geometry types.
    Args:
        column (str): \
            The column name.
        geom_types_list (str): \
            List of shapely geometry types to match against. \
            e.g: Point, Polygon, LineString, MultiPoint, MultiPolygon, MultiLineString
    """

    # These examples will be shown in the public gallery.
    # They will also be executed as unit tests for your Expectation.
    examples = [
        {
            "data": {
                "point_and_polygon": [
                    mapping(Point(1, 2)),
                    mapping(Polygon([(1, 2), (3, 4), (5, 6)])),
                ],
                "multipolygon_and_linestring": [
                    mapping(LineString([(0, 0), (1, 1), (1, -1)])),
                    mapping(MultiPolygon([Polygon([(1, 2), (3, 4), (5, 6)])])),
                ],
            },
            "tests": [
                {
                    "title": "positive_for_point_and_polygon",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "point_and_polygon",
                        "geom_types_list": ["Point", "Polygon"],
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative_for_point_and_polygon",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "point_and_polygon",
                        "geom_types_list": ["Point", "MultiPolygon"],
                        "mostly": 1.0,
                    },
                    "out": {"success": False},
                },
                {
                    "title": "positive_for_multipolygon_and_linestring",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "multipolygon_and_linestring",
                        "geom_types_list": ["Point", "MultiPolygon", "LineString"],
                        "mostly": 1,
                    },
                    "out": {"success": True},
                },
                {
                    "title": "negative_for_multipolygon_and_linestring",
                    "exact_match_out": False,
                    "include_in_gallery": True,
                    "in": {
                        "column": "multipolygon_and_linestring",
                        "geom_types_list": ["Point", "Polygon"],
                        "mostly": 0.2,
                    },
                    "out": {"success": False},
                },
            ],
        },
    ]

    # This is the id string of the Metric used by this Expectation.
    # For most Expectations, it will be the same as the `condition_metric_name` defined in your Metric class above.
    map_metric = "column_values.geometry_of_type"

    # This is a list of parameter names that can affect whether the Expectation evaluates to True or False
    success_keys = (
        "mostly",
        "geom_types_list",
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
        "requirements": ["shapely"],
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
                "geom_types_list",
                "mostly",
            ],
        )

        template_str = "values must belong to one of the following geometry types: $geom_types_list"

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
    ExpectColumnValuesGeometryToBeOfType().print_diagnostic_checklist()
