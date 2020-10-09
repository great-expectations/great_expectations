import copy
import json
import re
from typing import Optional

import altair as alt
import pandas as pd

from great_expectations.render.renderer.content_block.content_block import (
    ContentBlockRenderer,
)
from great_expectations.render.types import (
    RenderedGraphContent,
    RenderedStringTemplateContent,
)
from great_expectations.render.util import num_to_str, ordinal


def substitute_none_for_missing(kwargs, kwarg_list):
    """Utility function to plug Nones in when optional parameters are not specified in expectation kwargs.

    Example:
        Input:
            kwargs={"a":1, "b":2},
            kwarg_list=["c", "d"]

        Output: {"a":1, "b":2, "c": None, "d": None}

    This is helpful for standardizing the input objects for rendering functions.
    The alternative is lots of awkward `if "some_param" not in kwargs or kwargs["some_param"] == None:` clauses in renderers.
    """

    new_kwargs = copy.deepcopy(kwargs)
    for kwarg in kwarg_list:
        if kwarg not in new_kwargs:
            new_kwargs[kwarg] = None
    return new_kwargs


# NOTE: the method is pretty dirty
def parse_row_condition_string_pandas_engine(condition_string):
    if len(condition_string) == 0:
        condition_string = "True"

    template_str = "if "
    params = dict()

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
    conditions_list = re.split(r"AND|OR|NOT(?! in)|\(|\)", condition_string)
    conditions_list = [
        condition.strip()
        for condition in conditions_list
        if condition != "" and condition != " "
    ]

    for i, condition in enumerate(conditions_list):
        params["row_condition__" + str(i)] = condition.replace(" NOT ", " not ")
        condition_string = condition_string.replace(
            condition, "$row_condition__" + str(i)
        )

    template_str += condition_string.lower()

    return template_str, params


def handle_strict_min_max(params: dict) -> (str, str):
    """
    Utility function for the at least and at most conditions based on strictness.

    Args:
        params: dictionary containing "strict_min" and "strict_max" booleans.

    Returns:
        tuple of strings to use for the at least condition and the at most condition
    """

    at_least_str = (
        "greater than"
        if params.get("strict_min") is True
        else "greater than or equal to"
    )
    at_most_str = (
        "less than" if params.get("strict_max") is True else "less than or equal to"
    )

    return at_least_str, at_most_str


class ExpectationStringRenderer(ContentBlockRenderer):
    @classmethod
    def _missing_content_block_fn(
        cls, expectation, styling=None, include_column_name=True
    ):
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "styling": {"parent": {"classes": ["alert", "alert-warning"]}},
                    "string_template": {
                        "template": "$expectation_type(**$kwargs)",
                        "params": {
                            "expectation_type": expectation.expectation_type,
                            "kwargs": expectation.kwargs,
                        },
                        "styling": {
                            "params": {
                                "expectation_type": {
                                    "classes": ["badge", "badge-warning"],
                                }
                            }
                        },
                    },
                }
            )
        ]

    @classmethod
    def expect_column_to_exist(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs, ["column", "column_index"],
        )

        if params["column_index"] is None:
            if include_column_name:
                template_str = "$column is a required field."
            else:
                template_str = "is a required field."
        else:
            params["column_indexth"] = ordinal(params["column_index"])
            if include_column_name:
                template_str = "$column must be the $column_indexth field."
            else:
                template_str = "must be the $column_indexth field."

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_unique_value_count_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        at_least_str, at_most_str = handle_strict_min_max(params)

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "may have any number of unique values."
        else:
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                if params["min_value"] is None:
                    template_str = f"must have {at_most_str} $max_value unique values, at least $mostly_pct % of the time."
                elif params["max_value"] is None:
                    template_str = f"must have {at_least_str} $min_value unique values, at least $mostly_pct % of the time."
                else:
                    template_str = f"must have {at_least_str} $min_value and {at_most_str} $max_value unique values, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is None:
                    template_str = f"must have {at_most_str} $max_value unique values."
                elif params["max_value"] is None:
                    template_str = f"must have {at_least_str} $min_value unique values."
                else:
                    template_str = f"must have {at_least_str} $min_value and {at_most_str} $max_value unique values."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    # NOTE: This method is a pretty good example of good usage of `params`.
    @classmethod
    def expect_column_values_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        template_str = ""
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str += "may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            mostly_str = ""
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                mostly_str = ", at least $mostly_pct % of the time"

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str += f"values must be {at_least_str} $min_value and {at_most_str} $max_value{mostly_str}."

            elif params["min_value"] is None:
                template_str += f"values must be {at_most_str} $max_value{mostly_str}."

            elif params["max_value"] is None:
                template_str += f"values must be {at_least_str} $min_value{mostly_str}."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_pair_values_A_to_be_greater_than_B(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column_A",
                "column_B",
                "parse_strings_as_datetimes",
                "ignore_row_if",
                "mostly",
                "or_equal",
                "row_condition",
                "condition_parser",
            ],
        )

        if (params["column_A"] is None) or (params["column_B"] is None):
            template_str = "$column has a bogus `expect_column_pair_values_A_to_be_greater_than_B` expectation."
            params["row_condition"] = None

        if params["mostly"] is None:
            if params["or_equal"] in [None, False]:
                template_str = "Values in $column_A must always be greater than those in $column_B."
            else:
                template_str = "Values in $column_A must always be greater than or equal to those in $column_B."
        else:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            if params["or_equal"] in [None, False]:
                template_str = "Values in $column_A must be greater than those in $column_B, at least $mostly_pct % of the time."
            else:
                template_str = "Values in $column_A must be greater than or equal to those in $column_B, at least $mostly_pct % of the time."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_pair_values_to_be_equal(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column_A",
                "column_B",
                "ignore_row_if",
                "mostly",
                "row_condition",
                "condition_parser",
            ],
        )

        # NOTE: This renderer doesn't do anything with "ignore_row_if"

        if (params["column_A"] is None) or (params["column_B"] is None):
            template_str = " unrecognized kwargs for expect_column_pair_values_to_be_equal: missing column."
            params["row_condition"] = None

        if params["mostly"] is None:
            template_str = "Values in $column_A and $column_B must always be equal."
        else:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = "Values in $column_A and $column_B must be equal, at least $mostly_pct % of the time."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_table_columns_to_match_ordered_list(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(expectation.kwargs, ["column_list"])

        if params["column_list"] is None:
            template_str = "Must have a list of columns in a specific order, but that order is not specified."

        else:
            template_str = "Must have these columns in this order: "
            for idx in range(len(params["column_list"]) - 1):
                template_str += "$column_list_" + str(idx) + ", "
                params["column_list_" + str(idx)] = params["column_list"][idx]

            last_idx = len(params["column_list"]) - 1
            template_str += "$column_list_" + str(last_idx)
            params["column_list_" + str(last_idx)] = params["column_list"][last_idx]

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_table_columns_to_match_set(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs, ["column_set", "exact_match"]
        )

        if params["column_set"] is None:
            template_str = "Must specify a set or list of columns."

        else:
            # standardize order of the set for output
            params["column_list"] = list(params["column_set"])

            column_list_template_str = ", ".join(
                [f"$column_list_{idx}" for idx in range(len(params["column_list"]))]
            )

            exact_match_str = "exactly" if params["exact_match"] is True else "at least"

            template_str = f"Must have {exact_match_str} these columns (in any order): {column_list_template_str}"

            for idx in range(len(params["column_list"])):
                params["column_list_" + str(idx)] = params["column_list"][idx]

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_pair_cramers_phi_value_to_be_less_than(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs, ["column_A", "column_B"]
        )
        if (params["column_A"] is None) or (params["column_B"] is None):
            template_str = " unrecognized kwargs for expect_column_pair_cramers_phi_value_to_be_less_than: missing column."

        template_str = "Values in $column_A and $column_B must be independent."
        rendered_string_template_content = RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": template_str,
                    "params": params,
                    "styling": styling,
                },
            }
        )

        return [rendered_string_template_content]

    @classmethod
    def expect_compound_columns_to_be_unique(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )

        template_str = (
            f"Values for given compound columns must be unique together{mostly_str}: "
        )
        for idx in range(len(params["column_list"]) - 1):
            template_str += "$column_list_" + str(idx) + ", "
            params["column_list_" + str(idx)] = params["column_list"][idx]

        last_idx = len(params["column_list"]) - 1
        template_str += "$column_list_" + str(last_idx)
        params["column_list_" + str(last_idx)] = params["column_list"][last_idx]

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_multicolumn_values_to_be_unique(
        cls, expectation, styling=None, include_column_name=True
    ):
        # NOTE: This expectation is deprecated, please use
        # expect_select_column_values_to_be_unique_within_record instead.

        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )

        template_str = f"Values must always be unique across columns{mostly_str}: "
        for idx in range(len(params["column_list"]) - 1):
            template_str += "$column_list_" + str(idx) + ", "
            params["column_list_" + str(idx)] = params["column_list"][idx]

        last_idx = len(params["column_list"]) - 1
        template_str += "$column_list_" + str(last_idx)
        params["column_list_" + str(last_idx)] = params["column_list"][last_idx]

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_select_column_values_to_be_unique_within_record(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column_list",
                "ignore_row_if",
                "row_condition",
                "condition_parser",
                "mostly",
            ],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
        mostly_str = (
            ""
            if params.get("mostly") is None
            else ", at least $mostly_pct % of the time"
        )

        template_str = f"Values must always be unique across columns{mostly_str}: "
        for idx in range(len(params["column_list"]) - 1):
            template_str += "$column_list_" + str(idx) + ", "
            params["column_list_" + str(idx)] = params["column_list"][idx]

        last_idx = len(params["column_list"]) - 1
        template_str += "$column_list_" + str(last_idx)
        params["column_list_" + str(last_idx)] = params["column_list"][last_idx]

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_table_column_count_to_equal(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(expectation.kwargs, ["value"])
        template_str = "Must have exactly $value columns."
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_table_column_count_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs, ["min_value", "max_value", "strict_min", "strict_max"]
        )
        if params["min_value"] is None and params["max_value"] is None:
            template_str = "May have any number of columns."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"Must have {at_least_str} $min_value and {at_most_str} $max_value columns."
            elif params["min_value"] is None:
                template_str = f"Must have {at_most_str} $max_value columns."
            elif params["max_value"] is None:
                template_str = f"Must have {at_least_str} $min_value columns."
        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_table_row_count_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if params["min_value"] is None and params["max_value"] is None:
            template_str = "May have any number of rows."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"Must have {at_least_str} $min_value and {at_most_str} $max_value rows."
            elif params["min_value"] is None:
                template_str = f"Must have {at_most_str} $max_value rows."
            elif params["max_value"] is None:
                template_str = f"Must have {at_least_str} $min_value rows."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_table_row_count_to_equal(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs, ["value", "row_condition", "condition_parser"]
        )
        template_str = "Must have exactly $value rows."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_table_row_count_to_equal_other_table(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(expectation.kwargs, ["other_table_name"])
        template_str = "Row count must equal the row count of table $other_table_name."

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_distinct_values_to_be_in_set(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "value_set", "row_condition", "condition_parser"],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:

            if include_column_name:
                template_str = "$column distinct values must belong to this set: [ ]"
            else:
                template_str = "distinct values must belong to a set, but that set is not specified."

        else:

            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )

            if include_column_name:
                template_str = (
                    "$column distinct values must belong to this set: "
                    + values_string
                    + "."
                )
            else:
                template_str = (
                    "distinct values must belong to this set: " + values_string + "."
                )

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_not_be_null(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            if include_column_name:
                template_str = "$column values must not be null, at least $mostly_pct % of the time."
            else:
                template_str = (
                    "values must not be null, at least $mostly_pct % of the time."
                )
        else:
            if include_column_name:
                template_str = "$column values must never be null."
            else:
                template_str = "values must never be null."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_be_null(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = "values must be null, at least $mostly_pct % of the time."
        else:
            template_str = "values must be null."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_be_of_type(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "type_", "mostly", "row_condition", "condition_parser"],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str = (
                "values must be of type $type_, at least $mostly_pct % of the time."
            )
        else:
            template_str = "values must be of type $type_."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_be_in_type_list(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "type_list", "mostly", "row_condition", "condition_parser"],
        )

        if params["type_list"] is not None:
            for i, v in enumerate(params["type_list"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["type_list"])]
            )

            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                if include_column_name:
                    template_str = (
                        "$column value types must belong to this set: "
                        + values_string
                        + ", at least $mostly_pct % of the time."
                    )
                else:
                    template_str = (
                        "value types must belong to this set: "
                        + values_string
                        + ", at least $mostly_pct % of the time."
                    )
            else:
                if include_column_name:
                    template_str = (
                        "$column value types must belong to this set: "
                        + values_string
                        + "."
                    )
                else:
                    template_str = (
                        "value types must belong to this set: " + values_string + "."
                    )
        else:
            if include_column_name:
                template_str = "$column value types may be any value, but observed value will be reported"
            else:
                template_str = (
                    "value types may be any value, but observed value will be reported"
                )

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_be_in_set(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "value_set",
                "mostly",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
            ],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v

            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )

        template_str = "values must belong to this set: " + values_string

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_not_be_in_set(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "value_set",
                "mostly",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
            ],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v

            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )

        template_str = "values must not belong to this set: " + values_string

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_proportion_of_unique_values_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if params["min_value"] is None and params["max_value"] is None:
            template_str = "may have any fraction of unique values."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)
            if params["min_value"] is None:
                template_str = (
                    f"fraction of unique values must be {at_most_str} $max_value."
                )
            elif params["max_value"] is None:
                template_str = (
                    f"fraction of unique values must be {at_least_str} $min_value."
                )
            else:
                if params["min_value"] != params["max_value"]:
                    template_str = f"fraction of unique values must be {at_least_str} $min_value and {at_most_str} $max_value."
                else:
                    template_str = (
                        "fraction of unique values must be exactly $min_value."
                    )

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    # TODO: test parse_strings_as_datetimes
    @classmethod
    def expect_column_values_to_be_increasing(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "strictly",
                "mostly",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
            ],
        )

        if params.get("strictly"):
            template_str = "values must be strictly greater than previous values"
        else:
            template_str = "values must be greater than or equal to previous values"

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    # TODO: test parse_strings_as_datetimes
    @classmethod
    def expect_column_values_to_be_decreasing(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "strictly",
                "mostly",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
            ],
        )

        if params.get("strictly"):
            template_str = "values must be strictly less than previous values"
        else:
            template_str = "values must be less than or equal to previous values"

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_value_lengths_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "mostly",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "values may have any length."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must be {at_least_str} $min_value and {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["min_value"] is None:
                    template_str = f"values must be {at_most_str} $max_value characters long, at least $mostly_pct % of the time."

                elif params["max_value"] is None:
                    template_str = f"values must be {at_least_str} $min_value characters long, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = f"values must always be {at_least_str} $min_value and {at_most_str} $max_value characters long."

                elif params["min_value"] is None:
                    template_str = f"values must always be {at_most_str} $max_value characters long."

                elif params["max_value"] is None:
                    template_str = f"values must always be {at_least_str} $min_value characters long."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_value_lengths_to_equal(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "value", "mostly", "row_condition", "condition_parser"],
        )

        if params.get("value") is None:
            template_str = "values may have any length."
        else:
            template_str = "values must be $value characters long"
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_match_regex(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "regex", "mostly", "row_condition", "condition_parser"],
        )

        if not params.get("regex"):
            template_str = (
                "values must match a regular expression but none was specified."
            )
        else:
            template_str = "values must match this regular expression: $regex"
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_not_match_regex(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "regex", "mostly", "row_condition", "condition_parser"],
        )

        if not params.get("regex"):
            template_str = (
                "values must not match a regular expression but none was specified."
            )
        else:
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                if include_column_name:
                    template_str = "$column values must not match this regular expression: $regex, at least $mostly_pct % of the time."
                else:
                    template_str = "values must not match this regular expression: $regex, at least $mostly_pct % of the time."
            else:
                if include_column_name:
                    template_str = (
                        "$column values must not match this regular expression: $regex."
                    )
                else:
                    template_str = (
                        "values must not match this regular expression: $regex."
                    )

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_match_regex_list(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "regex_list",
                "mostly",
                "match_on",
                "row_condition",
                "condition_parser",
            ],
        )

        if not params.get("regex_list") or len(params.get("regex_list")) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["regex_list"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["regex_list"])]
            )

        if params.get("match_on") == "all":
            template_str = (
                "values must match all of the following regular expressions: "
                + values_string
            )
        else:
            template_str = (
                "values must match any of the following regular expressions: "
                + values_string
            )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_not_match_regex_list(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "regex_list", "mostly", "row_condition", "condition_parser"],
        )

        if not params.get("regex_list") or len(params.get("regex_list")) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["regex_list"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["regex_list"])]
            )

        template_str = (
            "values must not match any of the following regular expressions: "
            + values_string
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_match_strftime_format(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "strftime_format",
                "mostly",
                "row_condition",
                "condition_parser",
            ],
        )

        if not params.get("strftime_format"):
            template_str = "values must match a strftime format but none was specified."
        else:
            template_str = (
                "values must match the following strftime format: $strftime_format"
            )
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_be_dateutil_parseable(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        template_str = "values must be parseable by dateutil"

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_be_json_parseable(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        template_str = "values must be parseable as JSON"

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_values_to_match_json_schema(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "mostly", "json_schema", "row_condition", "condition_parser"],
        )

        if not params.get("json_schema"):
            template_str = "values must match a JSON Schema but none was specified."
        else:
            params["formatted_json"] = (
                "<pre>" + json.dumps(params.get("json_schema"), indent=4) + "</pre>"
            )
            if params["mostly"] is not None:
                params["mostly_pct"] = num_to_str(
                    params["mostly"] * 100, precision=15, no_scientific=True
                )
                # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
                template_str = "values must match the following JSON Schema, at least $mostly_pct % of the time: $formatted_json"
            else:
                template_str = (
                    "values must match the following JSON Schema: $formatted_json"
                )

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": {"params": {"formatted_json": {"classes": []}}},
                    },
                }
            )
        ]

    @classmethod
    def expect_column_distinct_values_to_contain_set(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "value_set",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
            ],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v

            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )

        template_str = "distinct values must contain this set: " + values_string + "."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_distinct_values_to_equal_set(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "value_set",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
            ],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v

            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )

        template_str = "distinct values must match this set: " + values_string + "."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_mean_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "mean may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"mean must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"mean must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"mean must be {at_least_str} $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_median_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "median may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"median must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"median must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"median must be {at_least_str} $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_stdev_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "standard deviation may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"standard deviation must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"standard deviation must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"standard deviation must be {at_least_str} $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_max_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "maximum value may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"maximum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"maximum value must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"maximum value must be {at_least_str} $min_value."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_min_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "parse_strings_as_datetimes",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "minimum value may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"minimum value must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"minimum value must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"minimum value must be {at_least_str} $min_value."

        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_sum_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "min_value",
                "max_value",
                "row_condition",
                "condition_parser",
                "strict_min",
                "strict_max",
            ],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "sum may have any numerical value."
        else:
            at_least_str, at_most_str = handle_strict_min_max(params)

            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = f"sum must be {at_least_str} $min_value and {at_most_str} $max_value."
            elif params["min_value"] is None:
                template_str = f"sum must be {at_most_str} $max_value."
            elif params["max_value"] is None:
                template_str = f"sum must be {at_least_str} $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def expect_column_most_common_value_to_be_in_set(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "value_set", "ties_okay", "row_condition", "condition_parser"],
        )

        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v

            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )

        template_str = (
            "most common value must belong to this set: " + values_string + "."
        )

        if params.get("ties_okay"):
            template_str += " Values outside this set that are as common (but not more common) are allowed."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]

    @classmethod
    def _get_kl_divergence_partition_object_table(cls, partition_object, header=None):
        table_rows = []
        fractions = partition_object["weights"]

        if partition_object.get("bins"):
            bins = partition_object["bins"]

            for idx, fraction in enumerate(fractions):
                if idx == len(fractions) - 1:
                    table_rows.append(
                        [
                            "[{} - {}]".format(
                                num_to_str(bins[idx]), num_to_str(bins[idx + 1])
                            ),
                            num_to_str(fraction),
                        ]
                    )
                else:
                    table_rows.append(
                        [
                            "[{} - {})".format(
                                num_to_str(bins[idx]), num_to_str(bins[idx + 1])
                            ),
                            num_to_str(fraction),
                        ]
                    )
        else:
            values = partition_object["values"]
            table_rows = [
                [value, num_to_str(fractions[idx])] for idx, value in enumerate(values)
            ]

        if header:
            return {
                "content_block_type": "table",
                "header": header,
                "header_row": ["Interval", "Fraction"]
                if partition_object.get("bins")
                else ["Value", "Fraction"],
                "table": table_rows,
                "styling": {
                    "classes": ["table-responsive"],
                    "body": {
                        "classes": [
                            "table",
                            "table-sm",
                            "table-bordered",
                            "mt-2",
                            "mb-2",
                        ],
                    },
                    "parent": {
                        "classes": ["show-scrollbars", "p-2"],
                        "styles": {
                            "list-style-type": "none",
                            "overflow": "auto",
                            "max-height": "80vh",
                        },
                    },
                },
            }
        else:
            return {
                "content_block_type": "table",
                "header_row": ["Interval", "Fraction"]
                if partition_object.get("bins")
                else ["Value", "Fraction"],
                "table": table_rows,
                "styling": {
                    "classes": ["table-responsive"],
                    "body": {
                        "classes": [
                            "table",
                            "table-sm",
                            "table-bordered",
                            "mt-2",
                            "mb-2",
                        ],
                    },
                    "parent": {
                        "classes": ["show-scrollbars", "p-2"],
                        "styles": {
                            "list-style-type": "none",
                            "overflow": "auto",
                            "max-height": "80vh",
                        },
                    },
                },
            }

    @classmethod
    def expect_column_quantile_values_to_be_between(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "quantile_ranges", "row_condition", "condition_parser"],
        )
        template_str = "quantiles must be within the following value ranges."

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = (
                conditional_template_str
                + ", then "
                + template_str[0].lower()
                + template_str[1:]
            )
            params.update(conditional_params)

        expectation_string_obj = {
            "content_block_type": "string_template",
            "string_template": {"template": template_str, "params": params},
        }

        quantiles = params["quantile_ranges"]["quantiles"]
        value_ranges = params["quantile_ranges"]["value_ranges"]

        table_header_row = ["Quantile", "Min Value", "Max Value"]
        table_rows = []

        quantile_strings = {0.25: "Q1", 0.75: "Q3", 0.50: "Median"}

        for quantile, value_range in zip(quantiles, value_ranges):
            quantile_string = quantile_strings.get(quantile, "{:3.2f}".format(quantile))
            table_rows.append(
                [
                    quantile_string,
                    str(value_range[0]) if value_range[0] is not None else "Any",
                    str(value_range[1]) if value_range[1] is not None else "Any",
                ]
            )

        quantile_range_table = {
            "content_block_type": "table",
            "header_row": table_header_row,
            "table": table_rows,
            "styling": {
                "body": {
                    "classes": [
                        "table",
                        "table-sm",
                        "table-unbordered",
                        "col-4",
                        "mt-2",
                    ],
                },
                "parent": {"styles": {"list-style-type": "none"}},
            },
        }

        return [expectation_string_obj, quantile_range_table]

    @classmethod
    def _get_kl_divergence_chart(cls, partition_object, header=None):
        weights = partition_object["weights"]

        if len(weights) > 60:
            expected_distribution = cls._get_kl_divergence_partition_object_table(
                partition_object, header=header
            )
        else:
            chart_pixel_width = (len(weights) / 60.0) * 500
            if chart_pixel_width < 250:
                chart_pixel_width = 250
            chart_container_col_width = round((len(weights) / 60.0) * 6)
            if chart_container_col_width < 4:
                chart_container_col_width = 4
            elif chart_container_col_width >= 5:
                chart_container_col_width = 6
            elif chart_container_col_width >= 4:
                chart_container_col_width = 5

            mark_bar_args = {}
            if len(weights) == 1:
                mark_bar_args["size"] = 20

            if partition_object.get("bins"):
                bins = partition_object["bins"]
                bins_x1 = [round(value, 1) for value in bins[:-1]]
                bins_x2 = [round(value, 1) for value in bins[1:]]

                df = pd.DataFrame(
                    {"bin_min": bins_x1, "bin_max": bins_x2, "fraction": weights,}
                )

                bars = (
                    alt.Chart(df)
                    .mark_bar()
                    .encode(
                        x="bin_min:O",
                        x2="bin_max:O",
                        y="fraction:Q",
                        tooltip=["bin_min", "bin_max", "fraction"],
                    )
                    .properties(width=chart_pixel_width, height=400, autosize="fit")
                )

                chart = bars.to_json()
            elif partition_object.get("values"):
                values = partition_object["values"]

                df = pd.DataFrame({"values": values, "fraction": weights})

                bars = (
                    alt.Chart(df)
                    .mark_bar()
                    .encode(
                        x="values:N", y="fraction:Q", tooltip=["values", "fraction"]
                    )
                    .properties(width=chart_pixel_width, height=400, autosize="fit")
                )
                chart = bars.to_json()

            if header:
                expected_distribution = RenderedGraphContent(
                    **{
                        "content_block_type": "graph",
                        "graph": chart,
                        "header": header,
                        "styling": {
                            "classes": [
                                "col-" + str(chart_container_col_width),
                                "mt-2",
                                "pl-1",
                                "pr-1",
                            ],
                            "parent": {"styles": {"list-style-type": "none"}},
                        },
                    }
                )
            else:
                expected_distribution = RenderedGraphContent(
                    **{
                        "content_block_type": "graph",
                        "graph": chart,
                        "styling": {
                            "classes": [
                                "col-" + str(chart_container_col_width),
                                "mt-2",
                                "pl-1",
                                "pr-1",
                            ],
                            "parent": {"styles": {"list-style-type": "none"}},
                        },
                    }
                )
        return expected_distribution

    @classmethod
    def expect_column_kl_divergence_to_be_less_than(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            [
                "column",
                "partition_object",
                "threshold",
                "row_condition",
                "condition_parser",
            ],
        )

        expected_distribution = None
        if not params.get("partition_object"):
            template_str = "can match any distribution."
        else:
            template_str = (
                "Kullback-Leibler (KL) divergence with respect to the following distribution must be "
                "lower than $threshold."
            )
            expected_distribution = cls._get_kl_divergence_chart(
                params.get("partition_object")
            )

        if include_column_name:
            template_str = "$column " + template_str

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        expectation_string_obj = {
            "content_block_type": "string_template",
            "string_template": {"template": template_str, "params": params},
        }

        if expected_distribution:
            return [expectation_string_obj, expected_distribution]
        else:
            return [expectation_string_obj]

    @classmethod
    def expect_column_values_to_be_unique(
        cls, expectation, styling=None, include_column_name=True
    ):
        params = substitute_none_for_missing(
            expectation.kwargs,
            ["column", "mostly", "row_condition", "condition_parser"],
        )

        if include_column_name:
            template_str = "$column values must be unique"
        else:
            template_str = "values must be unique"

        if params["mostly"] is not None:
            params["mostly_pct"] = num_to_str(
                params["mostly"] * 100, precision=15, no_scientific=True
            )
            # params["mostly_pct"] = "{:.14f}".format(params["mostly"]*100).rstrip("0").rstrip(".")
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."

        if params["row_condition"] is not None:
            (
                conditional_template_str,
                conditional_params,
            ) = parse_row_condition_string_pandas_engine(params["row_condition"])
            template_str = conditional_template_str + ", then " + template_str
            params.update(conditional_params)

        return [
            RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": template_str,
                        "params": params,
                        "styling": styling,
                    },
                }
            )
        ]
