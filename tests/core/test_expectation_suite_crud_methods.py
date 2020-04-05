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
def exp1():
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

