from timeit import timeit

import pytest
from great_expectations.data_asset.evaluation_parameters import (
    parse_evaluation_parameter,
)
from great_expectations.exceptions import EvaluationParameterError


def test_parse_evaluation_parameter():
    # Substitution alone is ok
    assert parse_evaluation_parameter("a", {"a": 1}) == 1
    assert (
        parse_evaluation_parameter(
            "urn:great_expectations:validations:blarg",
            {"urn:great_expectations:validations:blarg": 1},
        )
        == 1
    )

    # Very basic arithmetic is allowed as-is:
    assert parse_evaluation_parameter("1 + 1", {}) == 2

    # So is simple variable substitution:
    assert parse_evaluation_parameter("a + 1", {"a": 2}) == 3

    # URN syntax works
    assert (
        parse_evaluation_parameter(
            "urn:great_expectations:validations:source_patient_data.default"
            ":expect_table_row_count_to_equal.result.observed_value * 0.9",
            {
                "urn:great_expectations:validations:source_patient_data.default"
                ":expect_table_row_count_to_equal.result.observed_value": 10
            },
        )
        == 9
    )

    # We have basic operations (trunc)
    assert (
        parse_evaluation_parameter(
            "urn:great_expectations:validations:source_patient_data.default"
            ":expect_table_row_count_to_equal.result.observed_value * 0.9",
            {
                "urn:great_expectations:validations:source_patient_data.default"
                ":expect_table_row_count_to_equal.result.observed_value": 11
            },
        )
        != 9
    )

    assert (
        parse_evaluation_parameter(
            "trunc(urn:great_expectations:validations:source_patient_data.default"
            ":expect_table_row_count_to_equal.result.observed_value * 0.9)",
            {
                "urn:great_expectations:validations:source_patient_data.default"
                ":expect_table_row_count_to_equal.result.observed_value": 11
            },
        )
        == 9
    )

    # Non GE URN syntax fails
    with pytest.raises(EvaluationParameterError) as err:
        parse_evaluation_parameter("urn:ieee:not_ge * 10", {"urn:ieee:not_ge": 1})
    assert "Parse Failure" in str(err.value)

    # Valid variables but invalid expression is no good
    with pytest.raises(EvaluationParameterError) as err:
        parse_evaluation_parameter("1 / a", {"a": 0})
    assert (
        "Error while evaluating evaluation parameter expression: division by zero"
        in str(err.value)
    )

    # It is okay to *substitute* strings in the expression...
    assert parse_evaluation_parameter("foo", {"foo": "bar"}) == "bar"

    # ...and to have whitespace in substituted values...
    assert parse_evaluation_parameter("foo", {"foo": "bar "}) == "bar "

    # ...but whitespace is *not* preserved from the parameter name if we evaluate it
    assert parse_evaluation_parameter("foo ", {"foo": "bar"}) == "bar"  # NOT "bar "

    # We can use multiple parameters...
    assert parse_evaluation_parameter("foo * bar", {"foo": 2, "bar": 3}) == 6

    # ...but we cannot leave *partially* evaluated expressions (phew!)
    with pytest.raises(EvaluationParameterError) as e:
        parse_evaluation_parameter("foo + bar", {"foo": 2})
    assert (
        "Error while evaluating evaluation parameter expression: could not convert string to float"
        in str(e.value)
    )


def test_parser_timing():
    """We currently reuse the parser, clearing the stack between calls, which is about 10 times faster than not
    doing so. But these operations are really quick, so this may not be necessary."""
    assert (
        timeit(
            "parse_evaluation_parameter('x', {'x': 1})",
            setup="from great_expectations.data_asset.evaluation_parameters import parse_evaluation_parameter",
            number=100,
        )
        < 1
    )
