import pytest
import json

from great_expectations.core import (
    ExpectationSuite,
    ExpectationConfiguration,
)

from .test_expectation_suite import (
    exp1,
    exp2,
    exp3,
    exp4,
    baseline_suite,
)

@pytest.fixture
def empty_suite():
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[],
        meta={
            "notes": "This is an expectation suite."
        }
    )

@pytest.fixture
def exp5():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "a",
        },
        meta={}
    )

def test_append_expectation(empty_suite, exp1, exp2):

    assert len(empty_suite.expectations) == 0

    empty_suite.append_expectation(exp1)
    assert len(empty_suite.expectations) == 1

    # Adding the same expectation again *does* add duplicates.
    empty_suite.append_expectation(exp1)
    assert len(empty_suite.expectations) == 2

    empty_suite.append_expectation(exp2)
    assert len(empty_suite.expectations) == 3

    # Turn this on once we're ready to enforce strict typing.
    # with pytest.raises(TypeError):
    #     empty_suite.append_expectation("not an expectation")

    # Turn this on once we're ready to enforce strict typing.
    # with pytest.raises(TypeError):
    #     empty_suite.append_expectation(exp1.to_json_dict())

def test_find_expectation_indexes(baseline_suite, exp5):
    
    # Passing no parameters "finds" all Expectations
    assert baseline_suite.find_expectation_indexes() == [0,1]

    # Match on single columns
    assert baseline_suite.find_expectation_indexes(
        column="a"
    ) == [0]
    assert baseline_suite.find_expectation_indexes(
        column="b"
    ) == [1]

    # Non-existent column returns no matches
    assert baseline_suite.find_expectation_indexes(
        column="z"
    ) == []

    # It can return multiple expectation_type matches
    assert baseline_suite.find_expectation_indexes(
        expectation_type="expect_column_values_to_be_in_set"
    ) == [0,1]

    # It can return multiple column matches
    baseline_suite.append_expectation(exp5)
    assert baseline_suite.find_expectation_indexes(
        column="a"
    ) == [0,2]

    # It can match a single expectation_type
    assert baseline_suite.find_expectation_indexes(
        expectation_type="expect_column_values_to_not_be_null"
    ) == [2]

    # expectation_kwargs can match full kwargs
    assert baseline_suite.find_expectation_indexes(
        expectation_kwargs={
            "column": "b",
            "value_set": [-1, -2, -3],
            "result_format": "BASIC",
        }
    ) == [1]

    # expectation_kwargs can match partial kwargs
    assert baseline_suite.find_expectation_indexes(
        expectation_kwargs={
            "column": "a"
        }
    ) == [0, 2]

    # expectation_type and expectation_kwargs work in conjunction
    assert baseline_suite.find_expectation_indexes(
        expectation_type="expect_column_values_to_not_be_null",
        expectation_kwargs={
            "column": "a"
        }
    ) == [2]

    # column and expectation_kwargs work in conjunction
    assert baseline_suite.find_expectation_indexes(
        column="a",
        expectation_kwargs={
            "result_format": "BASIC"
        }
    ) == [0]

    # column and expectation_type work in conjunction
    assert baseline_suite.find_expectation_indexes(
        column="a",
        expectation_type="expect_column_values_to_not_be_null",
    ) == [2]
    assert baseline_suite.find_expectation_indexes(
        column="a",
        expectation_type="expect_column_values_to_be_between",
    ) == []
    assert baseline_suite.find_expectation_indexes(
        column="zzz",
        expectation_type="expect_column_values_to_be_between",
    ) == []

    with pytest.raises(ValueError):
        assert baseline_suite.find_expectation_indexes(
            column="a",
            expectation_kwargs={
                "column": "b"
            }
        ) == []

def test_find_expectation_indexes_on_empty_suite(empty_suite):

    assert empty_suite.find_expectation_indexes(
        expectation_type="expect_column_values_to_not_be_null"
    ) == []

    assert empty_suite.find_expectation_indexes(
        column="x"
    ) == []

    assert empty_suite.find_expectation_indexes(
        expectation_kwargs={}
    ) == []

