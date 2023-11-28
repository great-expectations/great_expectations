import datetime

import pytest

from great_expectations.expectations.core.validators import (
    validate_min_value,
)

# def validate_min_value(
#     min_val: float | dict | datetime.datetime | None,
# ) -> float | dict | datetime.datetime | None:
#     if not (
#         min_val is None or isinstance(min_val, (float, int, dict, datetime.datetime))
#     ):
#         raise ValueError(
#             "Provided min threshold must be a datetime (for datetime columns) or number"
#         )
#     if isinstance(min_val, dict) and "$PARAMETER" not in min_val:
#         raise ValueError(
#             'Evaluation Parameter dict for min_value kwarg must have "$PARAMETER" key'
#         )

#     return min_val


@pytest.mark.unit
@pytest.mark.parametrize(
    "min_val",
    [
        pytest.param(1.5, id="float"),
        pytest.param({}, id="dict"),
        pytest.param(datetime.datetime(1996, 6, 1), id="datetime"),
        pytest.param(None, id="None"),
    ],
)
def test_validate_min_value_success(min_val):
    validate_min_value(min_val)


@pytest.mark.unit
def test_validate_max_value():
    pass
