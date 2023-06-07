import pytest

import great_expectations.exceptions.exceptions as gx_exceptions
from great_expectations.expectations.expectation import Expectation


def test_expectation_is_expectation_self_initializing(capsys):
    with pytest.raises(gx_exceptions.ExpectationNotFoundError):
        Expectation.is_expectation_self_initializing(name="I_dont_exist")

    assert (
        Expectation.is_expectation_self_initializing(
            name="expect_column_distinct_values_to_be_in_set"
        )
        is False
    )
    captured = capsys.readouterr()
    assert (
        "The Expectation expect_column_distinct_values_to_be_in_set is not able to be self-initialized."
        in captured.out
    )

    assert (
        Expectation.is_expectation_self_initializing(
            name="expect_column_mean_to_be_between"
        )
        is True
    )
    captured = capsys.readouterr()
    assert (
        "The Expectation expect_column_mean_to_be_between is able to be self-initialized. Please run by using the auto=True parameter."
        in captured.out
    )
