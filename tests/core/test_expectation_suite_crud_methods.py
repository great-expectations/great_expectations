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

def test_find_expectations(baseline_suite, exp1, exp2):
    # Note: most of the logic in this method is based on
    # find_expectation_indexes and _copy_and_clean_up_expectations_from_indexes
    # These tests do not thoroughly cover that logic.
    # Instead, they focus on the behavior of the discard_* methods

    assert baseline_suite.find_expectations(
        column="a",
        expectation_type="expect_column_values_to_be_between",
    ) == []

    result = baseline_suite.find_expectations(
        column="a",
        expectation_type="expect_column_values_to_be_in_set",
    )
    assert len(result) == 1
    assert result[0] == ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3],
            # "result_format": "BASIC"
        },
        meta={
            "notes": "This is an expectation."
        }
    )

    exp_with_all_the_params = ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "a",
            "result_format": "BASIC",
            "include_config": True,
            "catch_exceptions": True,
        },
        meta={}
    )
    baseline_suite.append_expectation(exp_with_all_the_params)

    assert baseline_suite.find_expectations(
        column="a",
        expectation_type="expect_column_values_to_not_be_null",
    )[0] == ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "a",
        },
        meta={}
    )

    assert baseline_suite.find_expectations(
        column="a",
        expectation_type="expect_column_values_to_not_be_null",
        discard_result_format_kwargs=False,
        discard_include_config_kwargs=False,
        discard_catch_exceptions_kwargs=False,
    )[0] == exp_with_all_the_params

    assert baseline_suite.find_expectations(
        column="a",
        expectation_type="expect_column_values_to_not_be_null",
        discard_result_format_kwargs=False,
        discard_catch_exceptions_kwargs=False,
    )[0] == ExpectationConfiguration(
        expectation_type="expect_column_values_to_not_be_null",
        kwargs={
            "column": "a",
            "result_format": "BASIC",
            "catch_exceptions": True,
        },
        meta={}
    )

def test_remove_expectation(baseline_suite):
    # ValueError: Multiple expectations matched arguments. No expectations removed.
    with pytest.raises(ValueError):
        baseline_suite.remove_expectation()

    # ValueError: No matching expectation found.
    with pytest.raises(ValueError):
        baseline_suite.remove_expectation(
            column="does_not_exist"
        )

    # ValueError: Multiple expectations matched arguments. No expectations removed.
    with pytest.raises(ValueError):
        baseline_suite.remove_expectation(
            expectation_type="expect_column_values_to_be_in_set"
        )

    assert len(baseline_suite.expectations) == 2
    assert baseline_suite.remove_expectation(
        column="a"
    ) == None
    assert len(baseline_suite.expectations) == 1

    baseline_suite.remove_expectation(
        expectation_type="expect_column_values_to_be_in_set"
    )
    assert len(baseline_suite.expectations) == 0

    # ValueError: No matching expectation found.
    with pytest.raises(ValueError):
        baseline_suite.remove_expectation(
            expectation_type="expect_column_values_to_be_in_set"
        )

    #Tests for dry_run
    baseline_suite.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "z",
                "value_set": [1,2,3,4,5]
            }
        )
    )
    baseline_suite.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "z",
                "value_set": [1,2,3,4,5]
            }
        )
    )
    baseline_suite.append_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_to_exist",
            kwargs={
                "column": "z",
            }
        )
    )
    assert len(baseline_suite.expectations) == 3

    assert baseline_suite.remove_expectation(
        expectation_type="expect_column_to_exist",
        dry_run=True
    ) == ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": "z"},
        meta={},
    )
    assert len(baseline_suite.expectations) == 3

    with pytest.raises(ValueError):
        baseline_suite.remove_expectation(
            expectation_type="expect_column_values_to_be_in_set",
            dry_run=True
        )
    assert len(baseline_suite.expectations) == 3

    assert baseline_suite.remove_expectation(
        expectation_type="expect_column_values_to_be_in_set",
        remove_multiple_matches=True,
        dry_run=True
    ) == [
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "z",
                "value_set": [1,2,3,4,5],
            },
            meta={},
        ),
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "z",
                "value_set": [1,2,3,4,5]
            },
            meta={},
        )
    ]
    assert len(baseline_suite.expectations) == 3

    # Tests for remove_multiple_matches
    baseline_suite.remove_expectation(
        expectation_type="expect_column_values_to_be_in_set",
        remove_multiple_matches=True,
    )
    assert len(baseline_suite.expectations) == 1


def test_update_expectation(baseline_suite):
    # ValueError: Multiple Expectations matched arguments. No Expectations updated.
    with pytest.raises(ValueError):
        baseline_suite.update_expectation(
            new_kwargs={}
        )

    # ValueError: No matching Expectation found.
    with pytest.raises(ValueError):
        baseline_suite.update_expectation(
            new_kwargs={},
            column="does_not_exist"
        )

    # ValueError: Multiple Expectations matched arguments. No Expectations updated.
    with pytest.raises(ValueError):
        baseline_suite.update_expectation(
            new_kwargs={},
            expectation_type="expect_column_values_to_be_in_set"
        )


    # Verify that update_expectation returns a correct ExpectationConfiguration
    assert len(baseline_suite.expectations) == 2
    assert baseline_suite.update_expectation(
        new_kwargs={
            "column": "c",
        },
        column="a"
    ) == ExpectationConfiguration(**{
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "c",
            "value_set": [1, 2, 3],
            "result_format": "BASIC",
        },
        "meta": {"notes": "This is an expectation."}
    })
    assert len(baseline_suite.expectations) == 2

    # Verify that the Expectation was indeed changed in place.
    assert baseline_suite.find_expectation_indexes(
        expectation_type="expect_column_values_to_be_in_set",
        column="c"
    ) == [0]

    assert baseline_suite.update_expectation(
        new_kwargs={
            "value_set": [1,2,3],
        },
        column="b"
    ) == ExpectationConfiguration(**{
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "b",
            "value_set": [1, 2, 3],
            "result_format": "BASIC",
        },
        "meta": {"notes": "This is an expectation."}
    })
    assert len(baseline_suite.expectations) == 2

    # TODO: Implement this check once ExpectationConfiguration knows how to validate kwargs against expectation_types.
    # ValueError: Specified kwargs aren't valid for expectation type expect_column_values_to_be_in_set.
    # with pytest.raises(ValueError):
    #     baseline_suite.update_expectation(
    #         new_kwargs={
    #             "value_set": [1,2,3],
    #         },
    #         column="b",
    #         replace_all_kwargs=True
    #     )
                
    baseline_suite.update_expectation(
        new_kwargs={
            "column": "d",
            "value_set": [3,4,5],
        },
        column="b",
        replace_all_kwargs=True
    ) == ExpectationConfiguration(**{
        "expectation_type": "expect_column_values_to_be_in_set",
        "kwargs": {
            "column": "d",
            "value_set": [3, 4, 5],
        },
        "meta": {"notes": "This is an expectation."}
    })
    assert len(baseline_suite.expectations) == 2