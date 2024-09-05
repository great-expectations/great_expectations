import pytest

from great_expectations.expectations import (
    ExpectColumnMaxToBeBetween,
    ExpectColumnValuesToNotBeNull,
    UnexpectedRowsExpectation,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    "expectation,expected_serialization",
    [
        (
            ExpectColumnMaxToBeBetween(
                column="test_column",
                min_value=1,
            ),
            (
                '{"id": null, "meta": null, "notes": null, "result_format": "BASIC", '
                '"description": null, "catch_exceptions": false, "rendered_content": null, '
                '"batch_id": null, "row_condition": null, "condition_parser": null, "column": '
                '"test_column", "min_value": 1.0, "max_value": null, "strict_min": false, '
                '"strict_max": false}'
            ),
        ),
        (
            ExpectColumnValuesToNotBeNull(
                column="test_column",
                mostly=0.82,
            ),
            (
                '{"id": null, "meta": null, "notes": null, "result_format": "BASIC", '
                '"description": null, "catch_exceptions": true, "rendered_content": null, '
                '"batch_id": null, "row_condition": null, "condition_parser": null, "column": '
                '"test_column", "mostly": 0.82}'
            ),
        ),
        (
            UnexpectedRowsExpectation(
                unexpected_rows_query="SELECT * FROM my_table WHERE data='bad'",
                description="Data shouldn't be bad.",
            ),
            (
                '{"id": null, "meta": null, "notes": null, "result_format": "BASIC", '
                '"description": "Data shouldn\'t be bad.", "catch_exceptions": false, '
                '"rendered_content": null, "batch_id": null, "row_condition": null, '
                '"condition_parser": null, "unexpected_rows_query": "SELECT * FROM my_table '
                "WHERE data='bad'\"}"
            ),
        ),
    ],
)
def test_expectation_serialization_snapshot(expectation, expected_serialization):
    assert expectation.json() == expected_serialization
