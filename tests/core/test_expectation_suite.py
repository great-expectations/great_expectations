import pytest

from copy import copy, deepcopy

from great_expectations.core import ExpectationSuite, ExpectationConfiguration, NamespaceAwareExpectationSuite
from great_expectations.data_context.types import DataAssetIdentifier


@pytest.fixture
def exp1():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3],
            "result_format": "BASIC"
        },
        meta={
            "notes": "This is an expectation."
        }
    )


@pytest.fixture
def exp2():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "value_set": [-1, -2, -3],
            "result_format": "BASIC"
        },
        meta={
            "notes": "This is an expectation."
        }
    )


@pytest.fixture
def exp3():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "value_set": [-1, -2, -3],
            "result_format": "BASIC"
        },
        meta={
            "notes": "This is an expectation."
        }
    )


@pytest.fixture
def exp4():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "b",
            "value_set": [1, 2, 3],
            "result_format": "BASIC"
        },
        meta={
            "notes": "This is an expectation."
        }
    )


@pytest.fixture
def baseline_suite(exp1, exp2):
    return NamespaceAwareExpectationSuite(
        data_asset_name=DataAssetIdentifier(datasource="my_source", generator="my_generator",
                                            generator_asset="my_asset"),
        expectation_suite_name="warning",
        expectations=[exp1, exp2],
        meta={
            "notes": "This is an expectation suite."
        }
    )


@pytest.fixture
def identical_suite(exp1, exp3):
    return NamespaceAwareExpectationSuite(
        data_asset_name=DataAssetIdentifier(datasource="my_source", generator="my_generator",
                                            generator_asset="my_asset"),
        expectation_suite_name="warning",
        expectations=[exp1, exp3],
        meta={
            "notes": "This is an expectation suite."
        }
    )


@pytest.fixture
def equivalent_suite(exp1, exp3):
    return NamespaceAwareExpectationSuite(
        data_asset_name=DataAssetIdentifier(datasource="my_source", generator="my_generator",
                                            generator_asset="my_other_asset"),
        expectation_suite_name="danger",
        expectations=[exp1, exp3],
        meta={
            "notes": "This is another expectation suite, with a different name and meta"
        }
    )


@pytest.fixture
def different_suite(exp1, exp4):
    return NamespaceAwareExpectationSuite(
        data_asset_name=DataAssetIdentifier(datasource="my_source", generator="my_generator",
                                            generator_asset="my_asset"),
        expectation_suite_name="warning",
        expectations=[exp1, exp4],
        meta={
            "notes": "This is an expectation suite."
        }
    )


def test_expectation_suite_equality(baseline_suite, identical_suite, equivalent_suite):
    """Equality should depend on all defined properties of a configuration object, but not on whether the *instances*
    are the same."""
    assert baseline_suite is baseline_suite  # no difference
    assert baseline_suite is not identical_suite  # different instances, but same content
    assert baseline_suite == identical_suite  # different instances, but same content
    assert not (baseline_suite != identical_suite)  # ne works properly
    assert not (baseline_suite == equivalent_suite)  # different meta
    assert baseline_suite != equivalent_suite  # ne works properly


def test_expectation_suite_equivalence(baseline_suite, identical_suite, equivalent_suite, different_suite):
    """Equivalence should depend only on properties that affect the result of the expectation."""
    assert baseline_suite.isEquivalentTo(baseline_suite)  # no difference
    assert baseline_suite.isEquivalentTo(identical_suite)
    assert baseline_suite.isEquivalentTo(equivalent_suite)  # different meta
    assert not baseline_suite.isEquivalentTo(different_suite)  # different value_set in one expectation


def test_expectation_suite_dictionary_equivalence(baseline_suite):
    assert baseline_suite.isEquivalentTo({
        "data_asset_name": {"datasource": "my_source", "generator": "my_generator", "generator_asset": "my_asset"},
        "expectation_suite_name": "warning",
        "expectations": [{
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "a",
                "value_set": [1, 2, 3],
                "result_format": "BASIC"
            },
            "meta": {
                "notes": "This is an expectation."
            }},
            {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "b",
                "value_set": [-1, -2, -3],
                "result_format": "BASIC"
            },
            "meta": {
                "notes": "This is an expectation."
            }}],
        "meta": {
            "notes": "This is an expectation suite."
        }
    }) is True

    assert baseline_suite.isEquivalentTo({
        "data_asset_name": {"datasource": "my_source", "generator": "my_generator", "generator_asset": "my_asset"},
        "expectation_suite_name": "warning",
        "expectations": [{
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "a",
                "value_set": [-1, 2, 3],  # One value changed here
                "result_format": "BASIC"
            },
            "meta": {
                "notes": "This is an expectation."
            }},
            {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "b",
                "value_set": [-1, -2, -3],
                "result_format": "BASIC"
            },
            "meta": {
                "notes": "This is an expectation."
            }}],
        "meta": {
            "notes": "This is an expectation suite."
        }
    }) is False


def test_expectation_suite_copy(baseline_suite):
    suite_copy = copy(baseline_suite)
    assert suite_copy == baseline_suite
    suite_copy.data_asset_name = "blarg!"
    assert baseline_suite.data_asset_name != "blarg"  # copy on primitive properties shouldn't propagate
    suite_copy.expectations[0].meta["notes"] = "a different note"
    assert baseline_suite.expectations[0].meta["notes"] == "a different note"  # copy on deep attributes does propagate


def test_expectation_suite_deepcopy(baseline_suite):
    suite_deepcopy = deepcopy(baseline_suite)
    assert suite_deepcopy == baseline_suite
    suite_deepcopy.data_asset_name = "blarg!"
    assert baseline_suite.data_asset_name != "blarg"  # copy on primitive properties shouldn't propagate
    suite_deepcopy.expectations[0].meta["notes"] = "a different note"
    # deepcopy on deep attributes does not propagate
    assert baseline_suite.expectations[0].meta["notes"] == "This is an expectation."
