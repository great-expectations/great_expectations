import pytest

from great_expectations.expectations.expectation import _format_map_output

# module level markers
pytestmark = pytest.mark.unit


def test_format_map_output_with_numbers():
    success = False
    element_count = 5
    nonnull_count = 5
    unexpected_list = [
        {"foreign_key_1": 1, "foreign_key_2": 2},
        {"foreign_key_1": 1, "foreign_key_2": 2},
        {"foreign_key_1": 1, "foreign_key_2": 2},
    ]
    unexpected_index_list = [1, 2, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_list": [
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
            ],
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_counts": [{"value": (1, 2), "count": 3}],
            "partial_unexpected_index_list": [1, 2, 3],
            "unexpected_list": [
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
                {"foreign_key_1": 1, "foreign_key_2": 2},
            ],
            "unexpected_index_list": [1, 2, 3],
        },
    }


def test_format_map_output_with_numbers_without_values():
    success = False
    element_count = 5
    nonnull_count = 5
    unexpected_list = [
        {"foreign_key_1": 1, "foreign_key_2": 2},
        {"foreign_key_1": 1, "foreign_key_2": 2},
        {"foreign_key_1": 1, "foreign_key_2": 2},
    ]
    unexpected_index_list = [1, 2, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
            "exclude_unexpected_values": True,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_index_list": [1, 2, 3],
            "unexpected_index_list": [1, 2, 3],
        },
    }


def test_format_map_output_with_strings():
    success = False
    element_count = 5
    nonnull_count = 5
    unexpected_list = [
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
    ]
    unexpected_index_list = [1, 2, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
            ],
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_counts": [{"value": ("a", 2), "count": 3}],
            "partial_unexpected_index_list": [1, 2, 3],
            "unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
            ],
            "unexpected_index_list": [1, 2, 3],
        },
    }


def test_format_map_output_with_strings_without_values():
    success = False
    element_count = 5
    nonnull_count = 5
    unexpected_list = [
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
    ]
    unexpected_index_list = [1, 2, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
            "exclude_unexpected_values": True,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_index_list": [1, 2, 3],
            "unexpected_index_list": [1, 2, 3],
        },
    }


def test_format_map_output_with_strings_two_matches():
    success = False
    element_count = 5
    nonnull_count = 5
    unexpected_list = [
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "b", "foreign_key_2": 3},
    ]
    unexpected_index_list = [1, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "partial_unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "b", "foreign_key_2": 3},
            ],
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_counts": [
                {"value": ("a", 2), "count": 2},
                {"value": ("b", 3), "count": 1},
            ],
            "partial_unexpected_index_list": [1, 3],
            "unexpected_list": [
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "a", "foreign_key_2": 2},
                {"foreign_key_1": "b", "foreign_key_2": 3},
            ],
            "unexpected_index_list": [1, 3],
        },
    }


def test_format_map_output_with_strings_two_matches_without_values():
    success = False
    element_count = 5
    nonnull_count = 5
    unexpected_list = [
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "a", "foreign_key_2": 2},
        {"foreign_key_1": "b", "foreign_key_2": 3},
    ]
    unexpected_index_list = [1, 3]
    assert _format_map_output(
        result_format={
            "result_format": "COMPLETE",
            "partial_unexpected_count": 20,
            "include_unexpected_rows": False,
            "exclude_unexpected_values": True,
        },
        success=success,
        element_count=element_count,
        nonnull_count=nonnull_count,
        unexpected_count=len(unexpected_list),
        unexpected_list=unexpected_list,
        unexpected_index_list=unexpected_index_list,
    ) == {
        "success": False,
        "result": {
            "element_count": 5,
            "missing_count": 0,
            "missing_percent": 0.0,
            "unexpected_count": 3,
            "unexpected_percent": 60.0,
            "unexpected_percent_total": 60.0,
            "unexpected_percent_nonmissing": 60.0,
            "partial_unexpected_index_list": [1, 3],
            "unexpected_index_list": [1, 3],
        },
    }
