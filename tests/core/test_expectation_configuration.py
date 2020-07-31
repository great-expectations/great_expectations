import pytest

from great_expectations.core import ExpectationConfiguration


@pytest.fixture
def config1():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def config2():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def config3():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config4():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config5():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2],  # differs from others
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is another expectation."},
    )


def test_expectation_configuration_equality(config1, config2, config3, config4):
    """Equality should depend on all defined properties of a configuration object, but not on whether the *instances*
    are the same."""
    assert config1 is config1  # no difference
    assert config1 is not config2  # different instances, but same content
    assert config1 == config2  # different instances, but same content
    assert not (config1 != config2)  # ne works properly
    assert not (config1 == config3)  # different meta
    assert config1 != config3  # ne works properly
    assert config3 != config4  # different result format


def test_expectation_configuration_equivalence(
    config1, config2, config3, config4, config5
):
    """Equivalence should depend only on properties that affect the result of the expectation."""
    assert config1.isEquivalentTo(config2)  # no difference
    assert config2.isEquivalentTo(config1)
    assert config1.isEquivalentTo(config3)  # different meta
    assert config1.isEquivalentTo(config4)  # different result format
    assert not config1.isEquivalentTo(config5)  # different value_set


def test_expectation_configuration_get_evaluation_parameter_dependencies():
    # Getting evaluation parameter dependencies relies on pyparsing, but the expectation
    # configuration is responsible for ensuring that it only returns one copy of required metrics.

    # If different expectations rely on the same upstream dependency,then it is possible for duplicates
    # to be present nonetheless
    ec = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={
            "column": "norm",
            "min_value": {
                "$PARAMETER": "(-3 * urn:great_expectations:validations:profile:expect_column_stdev_to_be_between"
                ".result.observed_value:column=norm) + "
                "urn:great_expectations:validations:profile:expect_column_mean_to_be_between.result.observed_value"
                ":column=norm"
            },
            "max_value": {
                "$PARAMETER": "(3 * urn:great_expectations:validations:profile:expect_column_stdev_to_be_between"
                ".result.observed_value:column=norm) + "
                "urn:great_expectations:validations:profile:expect_column_mean_to_be_between.result.observed_value"
                ":column=norm"
            },
        },
    )

    dependencies = ec.get_evaluation_parameter_dependencies()
    dependencies["profile"][0]["metric_kwargs_id"]["column=norm"] = set(
        dependencies["profile"][0]["metric_kwargs_id"]["column=norm"]
    )

    assert {
        "profile": [
            {
                "metric_kwargs_id": {
                    "column=norm": {
                        "expect_column_stdev_to_be_between.result.observed_value",
                        "expect_column_mean_to_be_between.result.observed_value",
                    }
                }
            }
        ]
    } == dependencies
