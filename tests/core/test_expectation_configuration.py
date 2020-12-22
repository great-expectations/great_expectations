import pytest

from great_expectations.core.expectation_configuration import ExpectationConfiguration


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


@pytest.fixture
def config6():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3, 4],  # differs from others
            "result_format": "COMPLETE",
        },
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config7():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3, 4],
        },  # differs from others
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
    assert config1.isEquivalentTo(config2, match_type="runtime")  # no difference
    assert config2.isEquivalentTo(config1, match_type="runtime")
    assert config1.isEquivalentTo(config3, match_type="runtime")  # different meta
    assert config1.isEquivalentTo(
        config4, match_type="success"
    )  # different result format
    assert not config1.isEquivalentTo(
        config5, match_type="success"
    )  # different value_set
    assert config1.isEquivalentTo(
        config5, match_type="domain"
    )  # different result format


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


def test_expectation_configuration_patch(config4, config5, config6, config7):
    assert not config5.isEquivalentTo(config4, match_type="runtime")
    assert config5.patch("replace", "/value_set", [1, 2, 3]).isEquivalentTo(
        config4, match_type="runtime"
    )

    assert not config5.isEquivalentTo(config6, match_type="runtime")
    assert config5.patch("add", "/value_set/-", 4).isEquivalentTo(
        config6, match_type="runtime"
    )

    assert not config6.isEquivalentTo(config7, match_type="runtime")
    assert config6.patch("remove", "/result_format", 4).isEquivalentTo(
        config7, match_type="runtime"
    )

    with pytest.raises(ValueError):
        config5.patch("move", "/value_set/-", 4)

    with pytest.raises(IndexError):
        config5.patch("add", "value_set", 4)

    with pytest.raises(ValueError):
        config5.patch("add", "/foo/-", 4)
