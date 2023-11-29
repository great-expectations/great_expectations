from __future__ import annotations

from typing import Any


def validate_eval_parameter_dict(vals: Any):
    if isinstance(vals, dict) and "$PARAMETER" not in vals:
        raise ValueError('Evaluation Parameter dict must have "$PARAMETER" key')

    return vals
