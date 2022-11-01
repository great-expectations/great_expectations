from great_expectations.core.util import convert_to_json_serializable

try:
    from shapely.geometry import Polygon
except ImportError:
    Polygon = None


def test_serialization_of_shapely_polygon():
    polygon = Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])
    assert convert_to_json_serializable(polygon) == "POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))"


def test_serialization_of_bytes():
    data = b"\xC0\xA8\x00\x01"
    assert convert_to_json_serializable(data) == "b'\\xc0\\xa8\\x00\\x01'"
