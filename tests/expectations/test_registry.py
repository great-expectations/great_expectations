import pytest

import great_expectations.exceptions as gx_exceptions
import great_expectations.expectations as gxe
from great_expectations.expectations.expectation_configuration import (
    ExpectationConfiguration,
)
from great_expectations.expectations.registry import get_expectation_impl

# module level markers
pytestmark = pytest.mark.unit


def test_registry_basics():
    expectation = get_expectation_impl("expect_column_values_to_be_in_set")
    assert expectation == gxe.ExpectColumnValuesToBeInSet


def test_registry_from_configuration():
    configuration = ExpectationConfiguration(
        type="expect_column_values_to_be_in_set",
        kwargs={"column": "PClass", "value_set": [1, 2, 3]},
    )
    assert configuration._get_expectation_impl() == gxe.ExpectColumnValuesToBeInSet


def test_registry_raises_error_when_invalid_expectation_requested():
    with pytest.raises(gx_exceptions.ExpectationNotFoundError):
        get_expectation_impl("expect_something_in_beta")
