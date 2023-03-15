import warnings
from typing import Callable

from great_expectations.core._docs_decorators import (
    deprecated_method_or_class,
    public_api,
)
from great_expectations.expectations.expectation import (
    add_values_with_json_schema_from_list_in_params as add_values_with_json_schema_from_list_in_params_expectation,
)
from great_expectations.expectations.expectation import (
    render_evaluation_parameter_string as render_evaluation_parameter_string_expectation,
)


@deprecated_method_or_class(
    version="0.15.29",
    message="Use `import add_values_with_json_schema_from_list_in_params from great_expectations.expectations.expectation` instead.",
)
def add_values_with_json_schema_from_list_in_params(
    params: dict,
    params_with_json_schema: dict,
    param_key_with_list: str,
    list_values_type: str = "string",
) -> dict:
    # deprecated-v0.15.29
    warnings.warn(
        "The module great_expectations.expectations.util.py is deprecated as of v0.15.29 and will be removed in "
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


@public_api
@deprecated_method_or_class(
    version="0.15.29",
    message="Use `import render_evaluation_parameter_string from great_expectations.expectations.expectation` instead.",
)
def render_evaluation_parameter_string(render_func) -> Callable:
    # deprecated-v0.15.29
    warnings.warn(
        "The module great_expectations.expectations.util.py is deprecated as of v0.15.29 and will be removed in v0.18. "
        "Please import decorator render_evaluation_parameter_string from great_expectations.expectations.expectation.",
        DeprecationWarning,
    )
    return render_evaluation_parameter_string_expectation(render_func=render_func)
