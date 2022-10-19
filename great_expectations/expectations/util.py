import logging
from typing import Callable

from great_expectations.expectations.expectation import (
    render_evaluation_parameter_string,
)

logger = logging.getLogger(__name__)


@render_evaluation_parameter_string
def render_evaluation_parameter_string() -> Callable:
    pass


def add_values_with_json_schema_from_list_in_params(
    params: dict,
    params_with_json_schema: dict,
    param_key_with_list: str,
    list_values_type: str = "string",
) -> dict:
    """
    Utility function used in _atomic_prescriptive_template() to take list values from a given params dict key,
    convert each value to a dict with JSON schema type info, then add it to params_with_json_schema (dict).
    """
    target_list = params.get(param_key_with_list)
    if target_list is not None and len(target_list) > 0:
        for i, v in enumerate(target_list):
            params_with_json_schema[f"v__{str(i)}"] = {
                "schema": {"type": list_values_type},
                "value": v,
            }
    return params_with_json_schema
