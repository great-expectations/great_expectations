import pytest

from great_expectations.expectations import (
    ExpectColumnMaxToBeBetween,
    ExpectColumnValuesToNotBeNull,
    UnexpectedRowsExpectation,
)
from great_expectations.expectations.window import Offset, Window


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
                '"windows": null, "batch_id": null, "column": "test_column", '
                '"row_condition": null, "condition_parser": null, "min_value": 1.0, '
                '"max_value": null, "strict_min": false, "strict_max": false}'
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
                '"windows": null, "batch_id": null, "column": "test_column", "mostly": 0.82, '
                '"row_condition": null, "condition_parser": null}'
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
                '"rendered_content": null, "windows": null, "batch_id": null, '
                '"unexpected_rows_query": "SELECT * FROM '
                "my_table WHERE data='bad'\"}"
            ),
        ),
        (
            ExpectColumnValuesToNotBeNull(
                column="test_column",
                mostly=0.82,
                windows=[
                    Window(
                        constraint_fn="a",
                        parameter_name="b",
                        range=5,
                        offset=Offset(positive=0.2, negative=0.2),
                    )
                ],
            ),
            (
                '{"id": null, "meta": null, "notes": null, "result_format": "BASIC", '
                '"description": null, "catch_exceptions": true, "rendered_content": null, '
                '"windows": [{"constraint_fn": "a", "parameter_name": "b", "range": 5, '
                '"offset": {"positive": 0.2, "negative": 0.2}}], "batch_id": null, '
                '"column": "test_column", "mostly": 0.82, '
                '"row_condition": null, "condition_parser": null}'
            ),
        ),
    ],
)
def test_expectation_serialization_snapshot(expectation, expected_serialization):
    assert expectation.json() == expected_serialization
