import pytest

from great_expectations.core import (
    ExpectationConfiguration,
    ExpectationIdentityConfiguration,
    ExpectationValidationConfiguration,
    ExpectationRuntimeConfiguration,
)


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
        expectation_type="expect_table_row_count_to_be_between",
        kwargs={
            "min_value": 0,
            "max_value": 100,
            "result_format": "BASIC",
        },
        meta={"notes": "This is another expectation."},
    )

@pytest.fixture
def config7():
    return ExpectationConfiguration(
        expectation_type="expect_column_mean_to_be_between",
        kwargs={
            "column": "a",
            "min_value": 0,
            "max_value": 100,
            "result_format": "BASIC",
        },
        meta={"notes": "This is another expectation."},
    )

@pytest.fixture
def config8():
    return ExpectationConfiguration(
        expectation_type="expect_column_pair_values_to_be_equal",
        kwargs={
            "column_A": "a",
            "column_B": "b",
            "mostly": .9,
            "result_format": "COMPLETE",
        },
    )

@pytest.fixture
def config9():
    return ExpectationConfiguration(
        expectation_type="expect_multicolumn_values_to_be_unique",
        kwargs={
            "column_list": ["a", "b", "c"],
            "result_format": "COMPLETE",
        },
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


def test_expectation_config_properties(
    config1, config6, config7, config8, config9
):
    assert type(config1.identity_config) == ExpectationIdentityConfiguration
    assert type(config1.validation_config) == ExpectationValidationConfiguration
    assert type(config1.runtime_config) == ExpectationRuntimeConfiguration

    assert config1.identity_config == {
        "expectation_type" : "expect_column_values_to_be_in_set",
        "kwargs" : {
            "column": "a"
        }
    }
    assert config1.validation_config == {
        "expectation_type" : "expect_column_values_to_be_in_set",
        "kwargs" : {
            "column": "a",
            "value_set": [1, 2, 3],
        }
    }
    assert config1.runtime_config == {
        "expectation_type" : "expect_column_values_to_be_in_set",
        "kwargs" : {
            "column": "a",
            "value_set": [1, 2, 3],
            "result_format": "BASIC",
        }
    }


    assert config6.identity_config == {
        "expectation_type" : "expect_table_row_count_to_be_between",
        "kwargs" : {}
    }
    assert config6.validation_config == {
        "expectation_type" : "expect_table_row_count_to_be_between",
        "kwargs" : {
            "min_value": 0,
            "max_value": 100,
        }
    }
    assert config6.runtime_config == {
        "expectation_type" : "expect_table_row_count_to_be_between",
        "kwargs" : {
            "min_value": 0,
            "max_value": 100,
            "result_format": "BASIC",
        }
    }


    assert config7.identity_config == {
        "expectation_type" : "expect_column_mean_to_be_between",
        "kwargs" : {
            "column": "a"
        }
    }
    assert config7.validation_config == {
        "expectation_type" : "expect_column_mean_to_be_between",
        "kwargs" : {
            "column": "a",
            "min_value": 0,
            "max_value": 100,
        }
    }
    assert config7.runtime_config == {
        "expectation_type" : "expect_column_mean_to_be_between",
        "kwargs" : {
            "column": "a",
            "min_value": 0,
            "max_value": 100,
            "result_format": "BASIC",
        }
    }

    assert config8.identity_config == {
        "expectation_type" : "expect_column_pair_values_to_be_equal",
        "kwargs" : {
            "column_A": "a",
            "column_B": "b",
        }
    }
    assert config8.validation_config == {
        "expectation_type" : "expect_column_pair_values_to_be_equal",
        "kwargs" : {
            "column_A": "a",
            "column_B": "b",
            "mostly": .9,
        }
    }
    assert config8.runtime_config == {
        "expectation_type" : "expect_column_pair_values_to_be_equal",
        "kwargs" : {
            "column_A": "a",
            "column_B": "b",
            "mostly": .9,
            "result_format": "COMPLETE",
        }
    }


    assert config9.identity_config == {
        "expectation_type" : "expect_multicolumn_values_to_be_unique",
        "kwargs" : {
            "column_list": ["a", "b", "c"]
        }
    }
    assert config9.validation_config == {
        "expectation_type" : "expect_multicolumn_values_to_be_unique",
        "kwargs" : {
            "column_list": ["a", "b", "c"]
        }
    }
    assert config9.runtime_config == {
        "expectation_type" : "expect_multicolumn_values_to_be_unique",
        "kwargs" : {
            "column_list": ["a", "b", "c"],
            "result_format": "COMPLETE",
        }
    }

def test_expectation_configuration_equivalence_2():
    config1 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )

    config2 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
        meta={"notes": "This is an expectation."},
    )

    config3 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1000, 2000, 3000],},
        meta={"notes": "This is an expectation."},
    )

    config4 = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "b", "value_set": [1000, 2000, 3000],},
        meta={"notes": "This is an expectation."},
    )

    assert config1.isIdentityEquivalentTo(config1)
    assert config1.isIdentityEquivalentTo(config2)
    assert config1.isIdentityEquivalentTo(config3)
    assert not config1.isIdentityEquivalentTo(config4)

    assert config1.isValidationEquivalentTo(config1)
    assert config1.isValidationEquivalentTo(config2)
    assert not config1.isValidationEquivalentTo(config3)
    assert not config2.isValidationEquivalentTo(config3)
    assert not config3.isValidationEquivalentTo(config4)

    assert config1.isRuntimeEquivalentTo(config1)
    assert not config1.isRuntimeEquivalentTo(config2)
    assert not config1.isRuntimeEquivalentTo(config3)
    assert not config1.isRuntimeEquivalentTo(config4)

def test_update_kwargs(config1):

    config1.update_kwargs(
        {"column": "z"}
    )
    assert config1.kwargs == {
        "column": "z",
        "value_set": [1, 2, 3],
        "result_format": "BASIC"
    }

    config1.update_kwargs(
        {"column": "z"},
        replace_all_kwargs=True
    )
    assert config1.kwargs == {
        "column": "z",
    }

    # TODO: Implement this check once ExpectationConfiguration knows how to validate kwargs against expectation_types.
    # ValueError: Specified kwargs aren't valid for expectation type expect_column_values_to_be_in_set.
    # with pytest.raises(ValueError):
    #     config1.update_kwargs(
    #         new_kwargs={
    #             "bogus_field": "BOGUS_VALUE",
    #         },
    #     )

    #Note: this is degenerate behavior and wouldn't be possible with type checking.
    config1.update_kwargs(
        {},
        replace_all_kwargs=True
    )
    assert config1.kwargs == {}

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