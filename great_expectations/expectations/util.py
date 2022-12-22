import warnings
from typing import Callable

from great_expectations.expectations.expectation import (
    add_values_with_json_schema_from_list_in_params as add_values_with_json_schema_from_list_in_params_expectation,
)
from great_expectations.expectations.expectation import (
    render_evaluation_parameter_string as render_evaluation_parameter_string_expectation,
)


def add_values_with_json_schema_from_list_in_params(
    params: dict,
    params_with_json_schema: dict,
    param_key_with_list: str,
    list_values_type: str = "string",
) -> dict:
    # deprecated-v0.15.29
    warnings.warn(
        "The module great_expectations.expectations.util.py is deprecated as of v0.15.29 in and will be removed in "
        "v0.18. Please import method add_values_with_json_schema_from_list_in_params from "
        "great_expectations.expectations.expectation.",
        DeprecationWarning,
    )
    return add_values_with_json_schema_from_list_in_params_expectation(
        params=params,
        params_with_json_schema=params_with_json_schema,
        param_key_with_list=param_key_with_list,
        list_values_type=list_values_type,
    )


def render_evaluation_parameter_string(render_func) -> Callable:
    # deprecated-v0.15.29
    warnings.warn(
        "The module great_expectations.expectations.util.py is deprecated as of v0.15.29 and will be removed in v0.18. "
        "Please import decorator render_evaluation_parameter_string from great_expectations.expectations.expectation.",
        DeprecationWarning,
    )
    return render_evaluation_parameter_string_expectation(render_func=render_func)


# add rendering helpers here

# TODO: method for adding query

# TODO: method for adding additional section

# TODO: method for retriving the different version of `result_format`


def partial_or_full(result_dict):
    # unexpected_counts? :
    # this is the thing?
    # [{'value': 1, 'count': 1}, {'value': 2, 'count': 1}, {'value': 3, 'count': 1}, {'value': 4, 'count': 1}, {'value': 5, 'count': 1}, {'value': 6, 'count': 1}, {'value': 7, 'count': 1}, {'value': 8, 'count': 1}, {'value': 9, 'count': 1}, {'value': 10, 'count': 1}, {'value': 11, 'count': 1}, {'value': 12, 'count': 1}, {'value': 13, 'count': 1}, {'value': 14, 'count': 1}, {'value': 15, 'count': 1}, {'value': 16, 'count': 1}, {'value': 17, 'count': 1}, {'value': 18, 'count': 1}, {'value': 19, 'count': 1}, {'value': 20, 'count': 1}]
    partial_unexpected_counts: Optional[List[dict]] = result_dict.get(
        "partial_unexpected_counts"
    )
    unexpected_counts: Optional[List[dict]] = result_dict.get("unexpected_counts")
    # [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    partial_unexpected_list: Optional[List[dict]] = result_dict.get(
        "partial_unexpected_list"
    )
    unexpected_list: Optional[List[dict]] = result_dict.get("unexpected_list")
    # [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    partial_unexpected_index_list: Optional[List[dict]] = result_dict.get(
        "partial_unexpected_index_list"
    )
    unexpected_index_list: Optional[List[dict]] = result_dict.get(
        "unexpected_index_list"
    )
    if unexpected_counts:
        counts = unexpected_counts
    else:
        counts = partial_unexpected_counts

    if unexpected_list:
        unexpected = unexpected_counts
    else:
        unexpected = partial_unexpected_list

    if unexpected_index_list:
        unexpected_index = unexpected_counts
    else:
        unexpected_index = partial_unexpected_index_list

    return counts, unexpected, unexpected_index
