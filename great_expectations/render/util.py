"""Rendering utility"""

from __future__ import annotations

import copy
import decimal
import locale
import re
from typing import Any, Sequence

import pandas as pd

from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.exceptions import RenderingError

DEFAULT_PRECISION = 15
# create a new context for this task
ctx = decimal.Context()
# Lowering precision from the system default (28) can allow additional control over display
ctx.prec = DEFAULT_PRECISION


def num_to_str(  # noqa: C901
    f: float,
    precision: int = DEFAULT_PRECISION,
    use_locale: bool = False,
    no_scientific: bool = False,
) -> str:
    """Convert the given float to a string, centralizing standards for precision and decisions about scientific \
    notation. Adds an approximately equal sign in the event precision loss (e.g. rounding) has occurred.

    For more context, please review the following:
        https://stackoverflow.com/questions/38847690/convert-float-to-string-in-positional-format-without-scientific-notation-and-fa

    Args:
        f: The number to format.
        precision: The number of digits of precision to display (defaults to 4).
        use_locale: If True, use locale-specific formatting (e.g. adding thousands separators).
        no_scientific: If True, print all available digits of precision without scientific notation. This may insert
                       leading zeros before very small numbers, causing the resulting string to be longer than `precision`
                       characters.

    Returns:
        A string representation of the input float `f`, according to the desired parameters.
    """  # noqa: E501
    assert not (use_locale and no_scientific)
    if precision != DEFAULT_PRECISION:
        local_context = decimal.Context()
        local_context.prec = precision
    else:
        local_context = ctx
    # We cast to string; we want to avoid precision issues, but format everything as though it were a float.  # noqa: E501
    # So, if it's not already a float, we will append a decimal point to the string representation
    s = repr(f)
    if not isinstance(f, float):
        s += f"{locale.localeconv().get('decimal_point')}0"
    try:
        d = local_context.create_decimal(s)
    except decimal.InvalidOperation:
        raise TypeError(f"num_to_str received an invalid value: {f} of type {type(f).__name__}.")  # noqa: TRY003
    if no_scientific:
        result = format(d, "f")
    elif use_locale:
        result = format(d, "n")
    else:
        result = format(d, "g")
    if f != locale.atof(result):
        # result = '≈' + result
        #  ≈  # \u2248
        result = f"≈{result}"
    decimal_char = locale.localeconv().get("decimal_point")
    if not isinstance(decimal_char, str):
        raise TypeError(  # noqa: TRY003
            f"Expected str but got {decimal_char} which is type {type(decimal_char).__name__}."
        )
    if "e" not in result and "E" not in result and decimal_char in result:
        result = result.rstrip("0").rstrip(decimal_char)
    return result


SUFFIXES = {1: "st", 2: "nd", 3: "rd"}


def ordinal(num):
    """Convert a number to ordinal"""
    # Taken from https://codereview.stackexchange.com/questions/41298/producing-ordinal-numbers/41301
    # Consider a library like num2word when internationalization comes
    if 10 <= num % 100 <= 20:  # noqa: PLR2004
        suffix = "th"
    else:
        # the second parameter is a default.
        suffix = SUFFIXES.get(num % 10, "th")
    return str(num) + suffix


def resource_key_passes_run_name_filter(resource_key, run_name_filter):
    if type(resource_key) == ValidationResultIdentifier:  # noqa: E721 # ??
        run_name = resource_key.run_id.run_name
    else:
        raise TypeError(  # noqa: TRY003
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
        if run_name is None:
            return False
        regex_match = re.search(regex, run_name)
        return regex_match is not None


def substitute_none_for_missing(
    kwargs: dict[str, Any], kwarg_list: Sequence[str]
) -> dict[str, Any]:
    """Utility function to plug Nones in when optional parameters are not specified in expectation kwargs.

    Args:
        kwargs: A dictionary of keyword arguments.
        kwargs_list: A list or sequence of strings representing all possible keyword parameters to a function.

    Returns:
        A copy of the original `kwargs` with missing keys from `kwarg_list` defaulted to `None`.

    ```python
    >>> result = substitute_none_for_missing(
    ...    kwargs={"a":1, "b":2},
    ...    kwarg_list=["c", "d"]
    ... )
    ... print(result)
    {"a":1, "b":2, "c": None, "d": None}
    ```

    This is helpful for standardizing the input objects for rendering functions.
    The alternative is lots of awkward `if "some_param" not in kwargs or kwargs["some_param"] == None:` clauses in renderers.
    """  # noqa: E501

    new_kwargs = copy.deepcopy(kwargs)
    for kwarg in kwarg_list:
        if kwarg not in new_kwargs:
            new_kwargs[kwarg] = None
    return new_kwargs


# NOTE: the method is pretty dirty
def parse_row_condition_string_pandas_engine(
    condition_string: str, with_schema: bool = False
) -> tuple[str, dict]:
    """Parses the row condition string into a pandas engine compatible format.

    Args:
        condition_string: A pandas row condition string.
        with_schema: Return results in json schema format. Defaults to False.

    Returns:
        A tuple containing the template string and a `dict` of parameters.

    ```python
    >>> template_str, params = parse_row_condition_string_pandas_engine("Age in [0, 42]")
    >>> print(template_str)
    "if $row_condition__0"
    >>> params
    {"row_condition__0": "Age in [0, 42]"}
    ```

    """
    if len(condition_string) == 0:
        condition_string = "True"

    template_str = "if "
    params: dict[str, dict | str] = {}

    condition_string = (
        condition_string.replace("&", " AND ")
        .replace(" and ", " AND ")
        .replace("|", " OR ")
        .replace(" or ", " OR ")
        .replace("~", " NOT ")
        .replace(" not ", " NOT ")
    )
    condition_string = " ".join(condition_string.split())

    # replace tuples of values by lists of values
    tuples_list = re.findall(r"\([^\(\)]*,[^\(\)]*\)", condition_string)
    for value_tuple in tuples_list:
        value_list = value_tuple.replace("(", "[").replace(")", "]")
        condition_string = condition_string.replace(value_tuple, value_list)

    # divide the whole condition into smaller parts
    conditions_list: list[str] = [
        condition.strip()
        for condition in re.split(r"AND|OR|NOT(?! in)|\(|\)", condition_string)
        if condition != "" and condition != " "  # noqa: PLR1714
    ]

    for i, condition in enumerate(conditions_list):
        param_value = condition.replace(" NOT ", " not ")

        if with_schema:
            params[f"row_condition__{i}"] = {
                "schema": {"type": "string"},
                "value": param_value,
            }
        else:
            params[f"row_condition__{i}"] = param_value
            condition_string = condition_string.replace(condition, f"$row_condition__{i}")

    template_str += condition_string.lower()

    return template_str, params


def handle_strict_min_max(params: dict) -> tuple[str, str]:
    """Utility function for the at least and at most conditions based on strictness.

    Args:
        params: Dictionary containing "strict_min" and "strict_max" booleans.

    Returns:
        Tuple of strings to use for the at least condition and the at most condition.
    """
    at_least_str = (
        "greater than" if params.get("strict_min") is True else "greater than or equal to"
    )
    at_most_str = "less than" if params.get("strict_max") is True else "less than or equal to"

    return at_least_str, at_most_str


def _get_value_to_render(value_: Any) -> Any:
    if value_ is not None and value_ != "":
        return value_
    if value_ == "":
        return "EMPTY"
    return "null"


def _get_header_row(all_unexpected_in_samples: bool) -> list[str]:
    # Check to see if we have *all* of the unexpected values accounted for. If so,
    # we show counts. If not then we show Sampled Unexpected Values only.
    if all_unexpected_in_samples:
        header_row = ["Unexpected Value", "Count"]
    else:
        header_row = ["Sampled Unexpected Values"]
    return header_row


def _are_all_unexpected_values_in_samples(
    partial_unexpected_counts: list[dict], unexpected_count: int
) -> bool:
    total_count: int = 0

    for unexpected_count_dict in partial_unexpected_counts:
        count: int | None = unexpected_count_dict.get("count")
        if count:
            total_count += count

    return total_count == unexpected_count


def build_count_table(
    partial_unexpected_counts: list[dict], unexpected_count: int
) -> tuple[list[str], list[list[Any]]]:
    """
    Used by _diagnostic_unexpected_table_renderer() method in Expectation to render
    Unexpected Counts table.

    Args:
        partial_unexpected_counts: list of dictionaries containing unexpected values and counts
        unexpected_count: total number of unexpected values. Used to build the header

    Returns:
        list of strings that will be rendered into DataDocs

    """
    table_rows: list[list[str | int | None]] = []
    all_unexpected_in_samples: bool = _are_all_unexpected_values_in_samples(
        partial_unexpected_counts, unexpected_count
    )

    for unexpected_count_dict in partial_unexpected_counts:
        value = _get_value_to_render(unexpected_count_dict.get("value"))
        count = unexpected_count_dict.get("count")
        if all_unexpected_in_samples:
            table_rows.append([value, count])
        else:
            # Since accurate counts for the full dataset are not available,
            # we show Sampled Unexpected Values only.
            table_rows.append([value])

    header_row = _get_header_row(all_unexpected_in_samples)
    return header_row, table_rows


def build_count_and_index_table(  # noqa: C901
    partial_unexpected_counts: list[dict],
    unexpected_index_list: list[dict],
    unexpected_count: int,
    unexpected_list: list[dict] | None = None,
    unexpected_index_column_names: list[str] | None = None,
) -> tuple[list[str], list[list[Any]]]:
    """
        Used by _diagnostic_unexpected_table_renderer() method in Expectation to render
        Unexpected Counts and Indices table for ID/PK.
    Args:
        partial_unexpected_counts: list of dictionaries containing unexpected values and counts
        unexpected_index_list: list of dictionaries containing unexpected indices and their values
        unexpected_count: how many total unexpected values are there?
        unexpected_list: optional list of all unexpected values. Used with default Pandas unexpected
             indices (without defined id/pk columns)
        unexpected_index_column_names: list of unexpected_index_column_names

    Returns:
        List of strings that will be rendered into DataDocs

    """
    table_rows: list[list[str | int]] = []
    total_count: int = 0

    unexpected_index_df: pd.DataFrame = _convert_unexpected_indices_to_df(
        unexpected_index_list=unexpected_index_list,
        unexpected_index_column_names=unexpected_index_column_names,
        unexpected_list=unexpected_list,
        partial_unexpected_counts=partial_unexpected_counts,
    )
    if unexpected_index_df.empty:
        raise RenderingError(  # noqa: TRY003
            "GX ran into an issue while building count and index table for rendering. Please check your configuration."  # noqa: E501
        )

    # using default indices for Pandas
    if unexpected_index_column_names is None:
        unexpected_index_column_names = ["Index"]

    for index, row in unexpected_index_df.iterrows():
        row_list: list[str | int] = []

        unexpected_value = index
        count = int(row.Count)

        total_count += count

        if unexpected_value is not None and unexpected_value != "":
            row_list.append(unexpected_value)  # type: ignore[arg-type]
            row_list.append(count)
        elif unexpected_value == "":
            row_list.append("EMPTY")
            row_list.append(count)
        else:
            unexpected_value = "null"
            row_list.append("null")
            row_list.append(count)
        for column_name in unexpected_index_column_names:
            row_list.append(str(row[column_name]))

        if len(row_list) > 0:
            table_rows.append(row_list)

    # Check to see if we have *all* of the unexpected values accounted for. If so,
    # we show counts. If not, we only show "sampled" unexpected values.
    if total_count == unexpected_count:
        header_row = ["Unexpected Value", "Count"]
        for column_name in unexpected_index_column_names:
            header_row.append(column_name)
    else:
        header_row = ["Sampled Unexpected Values", "Count"]
        for column_name in unexpected_index_column_names:
            header_row.append(column_name)
    return header_row, table_rows


def _convert_unexpected_indices_to_df(
    unexpected_index_list: Sequence[dict | int],
    partial_unexpected_counts: list[dict],
    unexpected_index_column_names: list[str] | None = None,
    unexpected_list: list[Any] | None = None,
) -> pd.DataFrame:
    """
    Helper method to convert the list of unexpected indices into a DataFrame that can be used to
    display unexpected indices. domain_column_list (the list of column the Expectation is run on) is used
    as the index for the DataFrame, and the columns are the unexpected_index_column_names, or a default
    value in the case of Pandas, which provides default indices.

    In cases where the number of indices is too great (max 10 by default), the remaining values are
    truncated and the column contains "..." in their place.

                pk_1
    giraffe     3
    lion        4
    zebra       5 6 8 ...

    Args:
        unexpected_index_list : all unexpected values and indices
        partial_unexpected_counts : counts for unexpected values (max 20 by default)
        unexpected_index_column_names:  in the case of defining ID/PK columns
        unexpected_list: if we are using default Pandas output.
    Returns:
        pd.DataFrame that contains indices for unexpected values
    """  # noqa: E501
    domain_column_name_list: list[str]
    if unexpected_index_column_names:
        # if we have defined unexpected_index_column_names for ID/PK
        unexpected_index_df: pd.DataFrame = pd.DataFrame(unexpected_index_list, dtype="string")
        unexpected_index_df = unexpected_index_df.fillna(value="null")
        first_unexpected_index = unexpected_index_list[0]
        if isinstance(first_unexpected_index, dict):
            domain_column_name_list = list(
                set(first_unexpected_index.keys()).difference(set(unexpected_index_column_names))
            )
        else:
            raise TypeError(  # noqa: TRY003
                f"Expected dict but got {unexpected_index_list[0]} which is type {type(unexpected_index_list[0]).__name__}."  # noqa: E501
            )
    elif unexpected_list:
        # if we are using default Pandas unexpected indices
        unexpected_index_df = pd.DataFrame(
            list(zip(unexpected_list, unexpected_index_list)),
            columns=["Value", "Index"],
            dtype="string",
        )
        unexpected_index_df = unexpected_index_df.fillna(value="null")
        domain_column_name_list = ["Value"]
        unexpected_index_column_names = ["Index"]
    else:
        return pd.DataFrame()

    # 1. groupby on domain columns, and turn id/pk into list
    all_unexpected_indices: pd.DataFrame = unexpected_index_df.groupby(domain_column_name_list).agg(
        lambda y: list(y)
    )

    # 2. add count
    col_to_count: str = unexpected_index_column_names[0]
    all_unexpected_indices["Count"] = all_unexpected_indices[col_to_count].apply(lambda x: len(x))

    # 3. ensure index is a string
    all_unexpected_indices.index = all_unexpected_indices.index.map(str)

    # 4. truncate indices and replace with `...` if necessary
    for column in unexpected_index_column_names:
        all_unexpected_indices[column] = all_unexpected_indices[column].apply(
            lambda row: truncate_list_of_indices(row)
        )

    # 5. only keep the rows we are rendering
    filtered_unexpected_indices = all_unexpected_indices.head(len(partial_unexpected_counts))
    return filtered_unexpected_indices


def truncate_list_of_indices(indices: list[int | str], max_index: int = 10) -> str:
    """
    Lambda function used to take unexpected_indices and turn into a string that can be rendered in DataDocs.
    For lists that are greater than max_index, it will truncate the list and add a "..."

    Args:
        indices: indices to show
        max_index: Maximum number of indices to display before showing "..." in the column

    Returns:
        string of indices that are joined using ` `

    """  # noqa: E501
    if len(indices) > max_index:
        indices = indices[:max_index]
        indices.append("...")
    return ", ".join(map(str, indices))
