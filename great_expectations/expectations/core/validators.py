from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import datetime


def validate_min_value(
    min_val: float | dict | datetime.datetime | None,
) -> float | dict | datetime.datetime | None:
    if isinstance(min_val, dict) and "$PARAMETER" not in min_val:
        raise ValueError(
            'Evaluation Parameter dict for min_value kwarg must have "$PARAMETER" key'
        )

    return min_val


def validate_max_value(
    max_val: float | dict | datetime.datetime | None,
) -> float | dict | datetime.datetime | None:
    if isinstance(max_val, dict) and "$PARAMETER" not in max_val:
        raise ValueError(
            'Evaluation Parameter dict for max_value kwarg must have "$PARAMETER" key'
        )

    return max_val
