import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.expectations.core.expect_column_values_to_be_in_set import (
    ExpectColumnValuesToBeInSet,
)
from great_expectations.expectations.registry import get_expectation_impl


def test_registry_basics():
    expectation = get_expectation_impl("expect_column_values_to_be_in_set")
    assert expectation == ExpectColumnValuesToBeInSet


def test_registry_from_configuration():
    configuration = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_in_set",
        kwargs={"column": "PClass", "value_set": [1, 2, 3]},
    )
    assert configuration._get_expectation_impl() == ExpectColumnValuesToBeInSet


def test_registry_raises_error_when_invalid_expectation_requested():
    with pytest.raises(ge_exceptions.ExpectationNotFoundError):
        get_expectation_impl("expect_something_in_beta")
