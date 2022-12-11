import numpy as np
import pytest

from great_expectations.core.util import convert_to_json_serializable

try:
    from shapely.geometry import LineString, MultiPolygon, Point, Polygon
except ImportError:
    Point = None
    LineString = None
    Polygon = None
    MultiPolygon = None


@pytest.mark.skipif(Polygon is None, reason="Requires shapely.geometry.Polygon")
def test_serialization_of_shapely_polygon():
    polygon = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    assert (
        convert_to_json_serializable(polygon) == "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"
    )


@pytest.mark.skipif(
    any([MultiPolygon is None, Polygon is None]),
    reason="Requires shapely.geometry.Polygon and MultiPolygon",
)
def test_serialization_of_shapely_multipolygon():
    multi_polygon = MultiPolygon([Polygon([(1, 2), (3, 4), (5, 6)])])
    assert (
        convert_to_json_serializable(multi_polygon)
        == "MULTIPOLYGON (((1 2, 3 4, 5 6, 1 2)))"
    )


@pytest.mark.skipif(Point is None, reason="Requires shapely.geometry.Point")
def test_serialization_of_shapely_point():
    point = Point(1, 2)
    assert convert_to_json_serializable(point) == "POINT (1 2)"


@pytest.mark.skipif(LineString is None, reason="Requires shapely.geometry.LineString")
def test_serialization_of_shapely_linestring():
    point = LineString([(0, 0), (1, 1), (1, -1)])
    assert convert_to_json_serializable(point) == "LINESTRING (0 0, 1 1, 1 -1)"


def test_serialization_of_bytes():
    data = b"\xC0\xA8\x00\x01"
    assert convert_to_json_serializable(data) == "b'\\xc0\\xa8\\x00\\x01'"


def test_serialization_numpy_datetime():
    datetime_to_test = "2022-12-08T12:56:23.423"
    data = np.datetime64(datetime_to_test)
    assert convert_to_json_serializable(data) == datetime_to_test
