import pytest

from great_expectations.expectations.core.validators import (
    validate_eval_parameter_dict,
)


@pytest.mark.unit
def test_validate_eval_parameter_dict_success():
    min_val = {"$PARAMETER": "foo"}
    validate_eval_parameter_dict(min_val)


@pytest.mark.unit
def test_validate_eval_parameter_dict_failure():
    min_val = {}
    with pytest.raises(ValueError):
        validate_eval_parameter_dict(min_val)
