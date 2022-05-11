"Rendering utility"
import copy
import decimal
import locale
import re
import warnings

from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)

DEFAULT_PRECISION = 4
ctx = decimal.Context()
ctx.prec = DEFAULT_PRECISION


def num_to_str(f, precision=DEFAULT_PRECISION, use_locale=False, no_scientific=False):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Convert the given float to a string, centralizing standards for precision and decisions about scientific\n    notation. Adds an approximately equal sign in the event precision loss (e.g. rounding) has occurred.\n\n    There's a good discussion of related issues here:\n        https://stackoverflow.com/questions/38847690/convert-float-to-string-in-positional-format-without-scientific-notation-and-fa\n\n    Args:\n        f: the number to format\n        precision: the number of digits of precision to display\n        use_locale: if True, use locale-specific formatting (e.g. adding thousands separators)\n        no_scientific: if True, print all available digits of precision without scientific notation. This may insert\n            leading zeros before very small numbers, causing the resulting string to be longer than `precision`\n            characters\n\n    Returns:\n        A string representation of the float, according to the desired parameters\n\n    "
    assert not (use_locale and no_scientific)
    if precision != DEFAULT_PRECISION:
        local_context = decimal.Context()
        local_context.prec = precision
    else:
        local_context = ctx
    s = repr(f)
    if not isinstance(f, float):
        s += f"{locale.localeconv().get('decimal_point')}0"
    d = local_context.create_decimal(s)
    if no_scientific:
        result = format(d, "f")
    elif use_locale:
        result = format(d, "n")
    else:
        result = format(d, "g")
    if f != locale.atof(result):
        result = f"≈{result}"
    decimal_char = locale.localeconv().get("decimal_point")
    if ("e" not in result) and ("E" not in result) and (decimal_char in result):
        result = result.rstrip("0").rstrip(decimal_char)
    return result


SUFFIXES = {1: "st", 2: "nd", 3: "rd"}


def ordinal(num):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    "Convert a number to ordinal"
    if 10 <= (num % 100) <= 20:
        suffix = "th"
    else:
        suffix = SUFFIXES.get((num % 10), "th")
    return str(num) + suffix


def resource_key_passes_run_name_filter(resource_key, run_name_filter):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    if type(resource_key) == ValidationResultIdentifier:
        run_name = resource_key.run_id.run_name
    else:
        raise TypeError(
            "run_name_filter filtering is only implemented for ValidationResultResources."
        )
    if run_name_filter.get("equals"):
        return run_name_filter.get("equals") == run_name
    elif run_name_filter.get("not_equals"):
        return run_name_filter.get("not_equals") != run_name
    elif run_name_filter.get("includes"):
        return run_name_filter.get("includes") in run_name
    elif run_name_filter.get("not_includes"):
        return run_name_filter.get("not_includes") not in run_name
    elif run_name_filter.get("matches_regex"):
        regex = run_name_filter.get("matches_regex")
        regex_match = re.search(regex, run_name)
        return False if (regex_match is None) else True
    elif run_name_filter.get("eq"):
        warnings.warn(
            "The 'eq' key will is deprecated as of v0.11.9 and will be removed in v0.16. Please use the renamed 'equals' key.",
            DeprecationWarning,
        )
        return run_name_filter.get("eq") == run_name
    elif run_name_filter.get("ne"):
        warnings.warn(
            "The 'ne' key will is deprecated as of v0.11.9 and will be removed in v0.16. Please use the renamed 'not_equals' key.",
            DeprecationWarning,
        )
        return run_name_filter.get("ne") != run_name


def substitute_none_for_missing(kwargs, kwarg_list):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    'Utility function to plug Nones in when optional parameters are not specified in expectation kwargs.\n\n    Example:\n        Input:\n            kwargs={"a":1, "b":2},\n            kwarg_list=["c", "d"]\n\n        Output: {"a":1, "b":2, "c": None, "d": None}\n\n    This is helpful for standardizing the input objects for rendering functions.\n    The alternative is lots of awkward `if "some_param" not in kwargs or kwargs["some_param"] == None:` clauses in renderers.\n    '
    new_kwargs = copy.deepcopy(kwargs)
    for kwarg in kwarg_list:
        if kwarg not in new_kwargs:
            new_kwargs[kwarg] = None
    return new_kwargs


def parse_row_condition_string_pandas_engine(
    condition_string: str, with_schema: bool = False
) -> tuple:
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    if len(condition_string) == 0:
        condition_string = "True"
    template_str = "if "
    params = {}
    condition_string = (
        condition_string.replace("&", " AND ")
        .replace(" and ", " AND ")
        .replace("|", " OR ")
        .replace(" or ", " OR ")
        .replace("~", " NOT ")
        .replace(" not ", " NOT ")
    )
    condition_string = " ".join(condition_string.split())
    tuples_list = re.findall("\\([^\\(\\)]*,[^\\(\\)]*\\)", condition_string)
    for value_tuple in tuples_list:
        value_list = value_tuple.replace("(", "[").replace(")", "]")
        condition_string = condition_string.replace(value_tuple, value_list)
    conditions_list = re.split("AND|OR|NOT(?! in)|\\(|\\)", condition_string)
    conditions_list = [
        condition.strip()
        for condition in conditions_list
        if ((condition != "") and (condition != " "))
    ]
    for (i, condition) in enumerate(conditions_list):
        param_value = condition.replace(" NOT ", " not ")
        if with_schema:
            params[f"row_condition__{str(i)}"] = {
                "schema": {"type": "string"},
                "value": param_value,
            }
        else:
            params[f"row_condition__{str(i)}"] = param_value
            condition_string = condition_string.replace(
                condition, f"$row_condition__{str(i)}"
            )
    template_str += condition_string.lower()
    return (template_str, params)


def handle_strict_min_max(params: dict) -> (str, str):
    import inspect

    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any((var in k) for var in ("__frame", "__file", "__func")):
            continue
        print(f"<INTROSPECT> {__file}:{__func} - {k}:{v.__class__.__name__}")
    '\n    Utility function for the at least and at most conditions based on strictness.\n\n    Args:\n        params: dictionary containing "strict_min" and "strict_max" booleans.\n\n    Returns:\n        tuple of strings to use for the at least condition and the at most condition\n    '
    at_least_str = (
        "greater than"
        if (params.get("strict_min") is True)
        else "greater than or equal to"
    )
    at_most_str = (
        "less than" if (params.get("strict_max") is True) else "less than or equal to"
    )
    return (at_least_str, at_most_str)
