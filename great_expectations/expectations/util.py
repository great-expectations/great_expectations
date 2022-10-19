import logging
from typing import Callable

from great_expectations.expectations.expectation import (
    add_values_with_json_schema_from_list_in_params,
    render_evaluation_parameter_string,
)

logger = logging.getLogger(__name__)


render_evaluation_parameter_string = render_evaluation_parameter_string


def add_values_with_json_schema_from_list_in_params(
    params: dict,
    params_with_json_schema: dict,
    param_key_with_list: str,
    list_values_type: str = "string",
) -> dict:
    return add_values_with_json_schema_from_list_in_params(
        params=params,
        params_with_json_schema=params_with_json_schema,
        param_key_with_list=param_key_with_list,
        list_values_type=list_values_type,
    )
