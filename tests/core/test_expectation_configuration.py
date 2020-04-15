import pytest

from great_expectations.core import ExpectationConfiguration


@pytest.fixture
def config1():
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
def config2():
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
def config3():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3],
            "result_format": "BASIC"
        },
        meta={
            "notes": "This is another expectation."
        }
    )


@pytest.fixture
def config4():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3],
            "result_format": "COMPLETE"
        },
        meta={
            "notes": "This is another expectation."
        }
    )


@pytest.fixture
def config5():
    return ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2],  # differs from others
            "result_format": "COMPLETE"
        },
        meta={
            "notes": "This is another expectation."
        }
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


def test_expectation_configuration_equivalence(config1, config2, config3, config4, config5):
    """Equivalence should depend only on properties that affect the result of the expectation."""
    assert config1.isEquivalentTo(config2)  # no difference
    assert config2.isEquivalentTo(config1)
    assert config1.isEquivalentTo(config3)  # different meta
    assert config1.isEquivalentTo(config4)  # different result format
    assert not config1.isEquivalentTo(config5)  # different value_set


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

    # NOTE 2020/04/15 Abe: We'll probably want to implement this check once ExpectationConfiguration knows how to validate kwargs against expectation_types.
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
