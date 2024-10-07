from __future__ import annotations

import pytest

from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)


@pytest.fixture
def config1():
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def config2():
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is an expectation."},
    )


@pytest.fixture
def config3():
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "BASIC"},
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config4():
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "a", "value_set": [1, 2, 3], "result_format": "COMPLETE"},
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config5():
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
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
        type="expect_column_values_to_be_in_set",
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
        type="expect_column_values_to_be_in_set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3, 4],
        },  # differs from others
        meta={"notes": "This is another expectation."},
    )


@pytest.fixture
def config8():
    return ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        description="The values should be in the specified set",
        kwargs={
            "column": "a",
            "value_set": [1, 2, 3, 4],
        },
        meta={"notes": "This is another expectation."},
    )


@pytest.mark.unit
def test_expectation_configuration_equality(config1, config2, config3, config4):
    """Equality should depend on all defined properties of a configuration object, but not on whether the *instances*
    are the same."""  # noqa: E501
    assert config1 is config1  # no difference  # noqa: PLR0124
    assert config1 is not config2  # different instances, but same content
    assert config1 == config2  # different instances, but same content
    assert not (config1 != config2)  # ne works properly
    assert config1 != config3  # different meta
    assert config1 != config3  # ne works properly
    assert config3 != config4  # different result format


@pytest.mark.unit
def test_expectation_configuration_equivalence(config1, config2, config3, config4, config5):
    """Equivalence should depend only on properties that affect the result of the expectation."""
    assert config1.isEquivalentTo(config2, match_type="runtime")  # no difference
    assert config2.isEquivalentTo(config1, match_type="runtime")
    assert config1.isEquivalentTo(config3, match_type="runtime")  # different meta
    assert config1.isEquivalentTo(config4, match_type="success")  # different result format
    assert not config1.isEquivalentTo(config5, match_type="success")  # different value_set
    assert config1.isEquivalentTo(config5, match_type="domain")  # different result format


@pytest.mark.parametrize(
    "notes",
    [
        pytest.param("my notes", id="string notes"),
        pytest.param(["my", "list", "of", "notes"], id="string notes"),
        pytest.param(None, id="no notes"),
    ],
)
@pytest.mark.unit
def test_expectation_configuration_to_domain_obj(notes: str | list[str] | None):
    expectation_type = "expect_column_values_to_be_in_set"
    column = "genre_id"
    value_set = {1, 2, 3}
    meta = {"foo": "bar"}

    config = ExpectationConfiguration(
        type=expectation_type,
        kwargs={"column": column, "value_set": value_set},
        notes=notes,
        meta=meta,
    )
    expectation = config.to_domain_obj()

    # Check that the expectation object has the same properties as the config
    assert expectation.expectation_type == expectation_type
    assert expectation.column == column
    assert expectation.value_set == value_set
    assert expectation.notes == notes
    assert expectation.meta == meta

    # Ensure that translation to/from config is consistent
    assert expectation.configuration == config


@pytest.mark.unit
def test_expectation_configuration_to_json_dict(config1, config4, config8):
    assert config1.to_json_dict() == {
        "kwargs": {"column": "a", "result_format": "BASIC", "value_set": [1, 2, 3]},
        "meta": {"notes": "This is an expectation."},
        "type": "expect_column_values_to_be_in_set",
    }
    assert config4.to_json_dict() == {
        "kwargs": {"column": "a", "result_format": "COMPLETE", "value_set": [1, 2, 3]},
        "meta": {"notes": "This is another expectation."},
        "type": "expect_column_values_to_be_in_set",
    }
    assert config8.to_json_dict() == {
        "description": "The values should be in the specified set",
        "kwargs": {"column": "a", "value_set": [1, 2, 3, 4]},
        "meta": {"notes": "This is another expectation."},
        "type": "expect_column_values_to_be_in_set",
    }
