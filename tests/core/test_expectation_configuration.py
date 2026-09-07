from __future__ import annotations

import pytest

import great_expectations.expectations as gxe
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
    are the same."""  # noqa: E501 # FIXME CoP
    assert config1 is config1  # no difference  # noqa: PLR0124 # FIXME CoP
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
    input_set = {1, 2, 3}
    expected_list = list(input_set)
    meta = {"foo": "bar"}

    config = ExpectationConfiguration(
        type=expectation_type,
        kwargs={"column": column, "value_set": input_set},
        notes=notes,
        meta=meta,
    )
    expectation = config.to_domain_obj()
    assert isinstance(expectation, gxe.ExpectColumnValuesToBeInSet)

    # Check that the expectation object has the same properties as the config
    assert expectation.expectation_type == expectation_type
    assert expectation.column == column
    assert sorted(expectation.value_set) == sorted(expected_list)
    assert expectation.notes == notes
    assert expectation.meta == meta

    # Ensure that translation to/from config is consistent
    assert expectation.configuration == config


@pytest.mark.unit
def test_expectation_configuration_to_json_dict(config1, config4, config8):
    assert config1.to_json_dict() == {
        "kwargs": {"column": "a", "result_format": "BASIC", "value_set": [1, 2, 3]},
        "meta": {"notes": "This is an expectation."},
        "severity": "critical",
        "type": "expect_column_values_to_be_in_set",
    }
    assert config4.to_json_dict() == {
        "kwargs": {"column": "a", "result_format": "COMPLETE", "value_set": [1, 2, 3]},
        "meta": {"notes": "This is another expectation."},
        "severity": "critical",
        "type": "expect_column_values_to_be_in_set",
    }
    assert config8.to_json_dict() == {
        "description": "The values should be in the specified set",
        "kwargs": {"column": "a", "value_set": [1, 2, 3, 4]},
        "meta": {"notes": "This is another expectation."},
        "severity": "critical",
        "type": "expect_column_values_to_be_in_set",
    }


class TestExpectationConfigurationHash:
    @pytest.mark.unit
    def test_hash_consistency_with_equality(self, config1, config2):
        assert config1 == config2
        assert hash(config1) == hash(config2)

    @pytest.mark.unit
    def test_hash_different_for_different_types(self):
        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column"}
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_be_between", kwargs={"column": "test_column"}
        )

        assert config1 != config2
        assert hash(config1) != hash(config2)

    @pytest.mark.unit
    def test_hash_different_for_different_kwargs(self):
        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column_1"}
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null", kwargs={"column": "test_column_2"}
        )

        assert config1 != config2
        assert hash(config1) != hash(config2)

    @pytest.mark.unit
    def test_hash_different_for_different_meta(self):
        config1 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "test_column"},
            meta={"test": "value1"},
        )
        config2 = ExpectationConfiguration(
            type="expect_column_values_to_not_be_null",
            kwargs={"column": "test_column"},
            meta={"test": "value2"},
        )

        assert config1 != config2
        assert hash(config1) != hash(config2)

    @pytest.mark.unit
    def test_hash_stable_across_runs(self, config1):
        hash1 = hash(config1)
        hash2 = hash(config1)
        hash3 = hash(config1)

        assert hash1 == hash2 == hash3


@pytest.mark.unit
def test_expectation_configuration_severity_functionality():
    """Test that severity is properly handled in ExpectationConfiguration."""
    from great_expectations.expectations.metadata_types import FailureSeverity

    # Test default severity
    config = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
    )
    assert config.severity == FailureSeverity.CRITICAL

    # Test setting severity via constructor
    config = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        severity=FailureSeverity.WARNING,
    )
    assert config.severity == FailureSeverity.WARNING

    # Test setting severity via property setter
    config.severity = FailureSeverity.INFO
    assert config.severity == FailureSeverity.INFO

    # Test setting severity via string
    config.severity = "warning"
    assert config.severity == FailureSeverity.WARNING

    # Test that severity is included in serialization
    json_dict = config.to_json_dict()
    assert "severity" in json_dict
    assert json_dict["severity"] == "warning"

    # Test that severity is preserved in to_domain_obj conversion
    expectation = config.to_domain_obj()
    assert expectation.severity == FailureSeverity.WARNING

    # Test that severity is included in configuration property
    expectation_config = expectation.configuration
    assert expectation_config.severity == FailureSeverity.WARNING

    # Test invalid severity values
    from great_expectations.exceptions import InvalidExpectationConfigurationError

    with pytest.raises(InvalidExpectationConfigurationError, match="Invalid severity"):
        config.severity = "invalid_severity"

    with pytest.raises(
        InvalidExpectationConfigurationError, match="Severity must be string or enum"
    ):
        config.severity = 123


@pytest.mark.unit
def test_expectation_configuration_severity_equality():
    """Test that severity is NOT considered in equality comparisons (current implementation)."""
    config1 = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        severity="critical",
    )
    config2 = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        severity="warning",
    )
    config3 = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        severity="critical",
    )

    # Note: Current implementation doesn't include severity in equality comparison
    assert config1 == config2  # Same type and kwargs, different severity
    assert config1 == config3  # Same type and kwargs, same severity
    assert hash(config1) == hash(config2)  # Same hash (severity not included)
    assert hash(config1) == hash(config3)  # Same hash


@pytest.mark.unit
def test_expectation_configurations_with_same_kwargs_and_meta_but_different_ids_are_not_equal():
    """Test that severity is NOT considered in equality comparisons (current implementation)."""
    config1 = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        meta={"test": "value1"},
        id="dbfd38bd-9724-4909-b937-82b8b5702e17",
    )
    config2 = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        meta={"test": "value1"},
        id="abc537ee-7b01-407d-8fa7-ac6aba34cc47",
    )

    # Note: Current implementation doesn't include severity in equality comparison
    assert config1 != config2  # Same type and kwargs, different severity
    assert not config1 == config2  # noqa: SIM201 # Same type and kwargs, different severity


@pytest.mark.unit
def test_expectation_configurations_with_same_kwargs_meta_and_ids_are_equal():
    """Test that severity is NOT considered in equality comparisons (current implementation)."""
    config1 = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        meta={"test": "value1"},
        id="dbfd38bd-9724-4909-b937-82b8b5702e17",
    )
    config2 = ExpectationConfiguration(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "test_column"},
        meta={"test": "value1"},
        id="dbfd38bd-9724-4909-b937-82b8b5702e17",
    )

    # Note: Current implementation doesn't include severity in equality comparison
    assert config1 == config2  # Same type and kwargs, different severity
    assert not config1 != config2  # Same type and kwargs, different severity
