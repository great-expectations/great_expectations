import pytest

from great_expectations.core import ExpectationConfiguration, ExpectationSuite
from great_expectations.exceptions import (
    DataContextError,
    InvalidExpectationConfigurationError,
)


@pytest.fixture
def empty_suite():
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def baseline_suite(exp1, exp2):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1, exp2],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def exp1():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp2():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp3():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp4():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp5():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp6():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2]},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp7():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3, 4]},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def exp8():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3]},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def table_exp1():
    return ExpectationConfiguration(
        expectation_type="expect_table_columns_to_match_ordered_list",
        kwargs={"value": ["a", "b", "c"]},
    )


@pytest.fixture
def table_exp2():
    return ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={"min_value": 0, "max_value": 1},
    )


@pytest.fixture
def table_exp3():
    return ExpectationConfiguration(
        expectation_type="expect_table_row_count_to_equal", kwargs={"value": 1}
    )


@pytest.fixture
def column_pair_expectation():
    return ExpectationConfiguration(
        expectation_type="expect_column_pair_values_to_be_in_set",
        kwargs={
            "column_A": "1",
            "column_B": "b",
            "value_set": [(1, 1), (2, 2)],
            "result_format": "BASIC",
        },
    )


@pytest.fixture
def single_expectation_suite(exp1):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def different_suite(exp1, exp4):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1, exp4],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def domain_success_runtime_suite(exp1, exp2, exp3, exp4, exp5):
    return ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[exp1, exp2, exp3, exp4, exp5],
        meta={"notes": "This is an expectation suite."},
    )


@pytest.fixture
def suite_with_table_and_column_expectations(
    exp1, exp2, exp3, exp4, column_pair_expectation, table_exp1, table_exp2, table_exp3
):
    suite = ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[
            exp1,
            exp2,
            exp3,
            exp4,
            column_pair_expectation,
            table_exp1,
            table_exp2,
            table_exp3,
        ],
        meta={"notes": "This is an expectation suite."},
    )
    assert suite.expectations == [
        exp1,
        exp2,
        exp3,
        exp4,
        column_pair_expectation,
        table_exp1,
        table_exp2,
        table_exp3,
    ]
    return suite


@pytest.fixture
def suite_with_column_pair_and_table_expectations(
    table_exp1, table_exp2, table_exp3, column_pair_expectation
):
    suite = ExpectationSuite(
        expectation_suite_name="warning",
        expectations=[column_pair_expectation, table_exp1, table_exp2, table_exp3,],
        meta={"notes": "This is an expectation suite."},
    )
    assert suite.expectations == [
        column_pair_expectation,
        table_exp1,
        table_exp2,
        table_exp3,
    ]
    return suite


def test_find_expectation_indexes_on_empty_suite(exp1, empty_suite):
    assert empty_suite.find_expectation_indexes(exp1, "domain") == []


def test_find_expectation_indexes(
    exp1, exp2, exp3, exp4, exp5, domain_success_runtime_suite, single_expectation_suite
):
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "domain") == [
        1,
        2,
        3,
        4,
    ]
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "success") == [
        3,
        4,
    ]
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "runtime") == [3]

    assert single_expectation_suite.find_expectation_indexes(exp4, "runtime") == []

    with pytest.raises(InvalidExpectationConfigurationError):
        domain_success_runtime_suite.remove_expectation(
            "not an expectation", match_type="runtime"
        )

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(
            exp1, match_type="not a match_type"
        )


def test_find_expectations(
    exp1, exp2, exp3, exp4, exp5, domain_success_runtime_suite, single_expectation_suite
):
    expectation_to_find1 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [-1, -2, -3], "result_format": "COMPLETE"},
    )

    expectation_to_find2 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
    )

    assert domain_success_runtime_suite.find_expectations(
        expectation_to_find1, "domain"
    ) == [exp2, exp3, exp4, exp5]

    assert domain_success_runtime_suite.find_expectations(
        expectation_to_find1, "success"
    ) == [exp2, exp3]

    assert domain_success_runtime_suite.find_expectations(
        expectation_to_find2, "runtime"
    ) == [exp5]

    assert (
        domain_success_runtime_suite.find_expectations(expectation_to_find1, "runtime")
        == []
    )


def test_remove_expectation(
    exp1, exp2, exp3, exp4, exp5, single_expectation_suite, domain_success_runtime_suite
):
    domain_success_runtime_suite.remove_expectation(
        exp5, match_type="runtime", remove_multiple_matches=False
    )  # remove one matching expectation

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp5, match_type="runtime")
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "domain") == [
        1,
        2,
        3,
    ]
    assert domain_success_runtime_suite.find_expectation_indexes(exp4, "success") == [3]

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(
            exp4, match_type="domain", remove_multiple_matches=False
        )

    # remove 3 matching expectations
    domain_success_runtime_suite.remove_expectation(
        exp4, match_type="domain", remove_multiple_matches=True
    )

    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp2, match_type="runtime")
    with pytest.raises(ValueError):
        domain_success_runtime_suite.remove_expectation(exp3, match_type="runtime")

    assert domain_success_runtime_suite.find_expectation_indexes(
        exp1, match_type="domain"
    ) == [0]
    assert domain_success_runtime_suite.isEquivalentTo(single_expectation_suite)


def test_patch_expectation_replace(exp5, exp6, domain_success_runtime_suite):
    assert domain_success_runtime_suite.expectations[4] is exp5

    assert not domain_success_runtime_suite.expectations[4].isEquivalentTo(
        exp6, match_type="success"
    )
    domain_success_runtime_suite.patch_expectation(
        exp5, op="replace", path="/value_set", value=[1, 2], match_type="runtime",
    )
    assert domain_success_runtime_suite.expectations[4].isEquivalentTo(
        exp6, match_type="success"
    )


def test_patch_expectation_add(exp5, exp7, domain_success_runtime_suite):
    assert domain_success_runtime_suite.expectations[4] is exp5

    assert not domain_success_runtime_suite.expectations[4].isEquivalentTo(
        exp7, match_type="success"
    )
    domain_success_runtime_suite.patch_expectation(
        exp5, op="add", path="/value_set/-", value=4, match_type="runtime",
    )
    assert domain_success_runtime_suite.expectations[4].isEquivalentTo(
        exp7, match_type="success"
    )


def test_patch_expectation_remove(exp5, exp8, domain_success_runtime_suite):
    assert domain_success_runtime_suite.expectations[4] is exp5

    assert not domain_success_runtime_suite.expectations[4].isEquivalentTo(
        exp8, match_type="runtime"
    )
    domain_success_runtime_suite.patch_expectation(
        exp5, op="remove", path="/result_format", value=None, match_type="runtime",
    )
    assert domain_success_runtime_suite.expectations[4].isEquivalentTo(
        exp8, match_type="runtime"
    )


def test_add_expectation(
    exp1,
    exp2,
    exp4,
    single_expectation_suite,
    baseline_suite,
    different_suite,
    domain_success_runtime_suite,
):
    assert len(single_expectation_suite.expectations) == 1
    assert not single_expectation_suite.isEquivalentTo(baseline_suite)
    single_expectation_suite.add_expectation(
        exp2, match_type="runtime", overwrite_existing=False
    )
    assert single_expectation_suite.isEquivalentTo(baseline_suite)
    assert len(single_expectation_suite.expectations) == 2

    # Should raise if overwrite_existing=False and a matching expectation is found
    with pytest.raises(DataContextError):
        single_expectation_suite.add_expectation(
            exp4, match_type="domain", overwrite_existing=False
        )

    assert not single_expectation_suite.isEquivalentTo(different_suite)
    single_expectation_suite.add_expectation(
        exp4, match_type="domain", overwrite_existing=True
    )
    assert single_expectation_suite.isEquivalentTo(different_suite)
    assert len(single_expectation_suite.expectations) == 2

    # Should raise if more than one matching expectation is found
    with pytest.raises(ValueError):
        domain_success_runtime_suite.add_expectation(
            exp2, match_type="success", overwrite_existing=False
        )

    with pytest.raises(InvalidExpectationConfigurationError):
        single_expectation_suite.add_expectation("not an expectation")

    # Turn this on once we're ready to enforce strict typing.
    # with pytest.raises(TypeError):
    #     single_expectation_suite.append_expectation(exp1.to_json_dict())


def test_remove_all_expectations_of_type(
    suite_with_table_and_column_expectations,
    suite_with_column_pair_and_table_expectations,
):
    assert not suite_with_table_and_column_expectations.isEquivalentTo(
        suite_with_column_pair_and_table_expectations
    )

    suite_with_table_and_column_expectations.remove_all_expectations_of_type(
        "expect_column_values_to_be_in_set"
    )

    assert suite_with_table_and_column_expectations.isEquivalentTo(
        suite_with_column_pair_and_table_expectations
    )
