import pytest

from great_expectations.expectations.core.validators import (
    validate_max_value,
    validate_min_value,
)


@pytest.mark.unit
def test_validate_min_value_success():
    min_val = {"$PARAMETER": "foo"}
    validate_min_value(min_val)


@pytest.mark.unit
def test_validate_min_value_failure():
    min_val = {}
    with pytest.raises(ValueError):
        validate_min_value(min_val)


@pytest.mark.unit
def test_validate_max_value_success():
    max_val = {"$PARAMETER": "foo"}
    validate_max_value(max_val)


@pytest.mark.unit
def test_validate_max_value_failure():
    max_val = {}
    with pytest.raises(ValueError):
        validate_max_value(max_val)
