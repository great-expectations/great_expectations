import copy

from .content_block import ContentBlockRenderer
from ...util import ordinal


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


# class DescriptiveBulletListContentBlockRenderer(BulletListContentBlockRenderer):
# class FailedExpectationBulletListContentBlockRenderer(BulletListContentBlockRenderer):
# class FailedExpectationBulletListContentBlockRenderer(BulletListContentBlockRenderer):

class PrescriptiveBulletListContentBlockRenderer(ContentBlockRenderer):
    _content_block_type = "bullet_list"

    _default_element_styling = {
        "default": {
            "classes": ["badge", "badge-secondary"]
        },
        "params": {
            "column": {
                "classes": ["badge", "badge-primary"]
            }
        }
    }

    @classmethod
    def _missing_content_block_fn(cls, expectation, styling=None, include_column_name=True):
        return [{
            "template": "$expectation_type(**$kwargs)",
            "params": {
                "expectation_type": expectation["expectation_type"],
                "kwargs": expectation["kwargs"]
            },
            # "styling": styling,
            "styling": {
                "classes": ["alert", "alert-warning"],
                "attributes": {
                    "role": "alert",
                    # "data-container": "body",
                    # "data-toggle": "popover",
                    # "data-placement": "bottom",
                    # "data-trigger": "hover",
                    # "data-content": expectation["expectation_type"],
                },
                "params": {
                    "expectation_type": {
                        "classes": ["badge", "badge-warning"],
                    }
                }
            },
        }]

    @classmethod
    def expect_column_to_exist(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "column_index"],
        )

        if params["column_index"] is None:
            if include_column_name:
                template_str = "$column is a required field."
            else:
                template_str = "is a required field."
        else:
            params["column_indexth"] = ordinal(params["column_index"])
            if include_column_name:
                template_str = "$column must be the $column_indexth field"
            else:
                template_str = "must be the $column_indexth field"

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_value_lengths_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "values may have any length."

        elif "mostly" in params:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "must be between $min_value and $max_value characters long at least $mostly% of the time."

            elif params["min_value"] is None:
                template_str = "must be less than $max_value characters long at least $mostly% of the time."

            elif params["max_value"] is None:
                template_str = "must be more than $min_value characters long at least $mostly% of the time."

        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "must always be between $min_value and $max_value characters long."

            elif params["min_value"] is None:
                template_str = "must always be less than $max_value characters long."

            elif params["max_value"] is None:
                template_str = "must always be more than $min_value characters long."

        if include_column_name:
            template_str = "$column " + template_str

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_unique_value_count_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "may have any number of unique values."
        elif params["min_value"] is None:
            template_str = "must have fewer than $max_value unique values."
        elif params["max_value"] is None:
            template_str = "must have more than $min_value unique values."
        else:
            template_str = "must have between $min_value and $max_value unique values."

        if include_column_name:
            template_str = "$column " + template_str

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    # NOTE: This method is a pretty good example of good usage of `params`.
    @classmethod
    def expect_column_values_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"]
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "may have any numerical value."

        elif "mostly" in params:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "must be between $min_value and $max_value at least $mostly% of the time."

            elif params["min_value"] is None:
                template_str = "must be less than $max_value at least $mostly% of the time."

            elif params["max_value"] is None:
                template_str = "must be less than $max_value at least $mostly% of the time."

        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "must always be between $min_value and $max_value."

            elif params["min_value"] is None:
                template_str = "must always be less than $max_value."

            elif params["max_value"] is None:
                template_str = "must always be more than $min_value."

        if include_column_name:
            template_str = "$column " + template_str

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_pair_values_A_to_be_greater_than_B(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_A", "column_B", "parse_strings_as_datetimes",
                "ignore_row_if", "mostly", "or_equal"]
        )

        if (params["column_A"] is None) or (params["column_B"] is None):
            template_str = "$column has a bogus `expect_column_pair_values_A_to_be_greater_than_B` expectation."

        if params["mostly"] is None:
            if params["or_equal"] in [None, False]:
                template_str = "Values in $column_A must always be greater than those in $column_B."
            else:
                template_str = "Values in $column_A must always be greater than or equal to those in $column_B."

        else:
            if params["or_equal"] in [None, False]:
                template_str = "Values in $column_A must be greater than those in $column_B at least $mostly % of the time."
            else:
                template_str = "Values in $column_A must be greater than or equal to those in $column_B at least $mostly % of the time."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_pair_values_to_be_equal(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_A", "column_B",
                "ignore_row_if", "mostly", ]
        )

        # NOTE: This renderer doesn't do anything with "ignore_row_if"

        if (params["column_A"] is None) or (params["column_B"] is None):
            template_str = " unrecognized kwargs for expect_column_pair_values_to_be_equal: missing column."

        if params["mostly"] is None:
            template_str = "Values in $column_A and $column_B must always be equal."

        else:
            # Note: this pattern for type conversion seems to work reasonably well.
            # Note: I'm not 100% sure that this is the right place to encode details like how many decimals to show.
            params["mostly_pct"] = "%.1f" % (params["mostly"]*100,)
            template_str = "Values in $column_A and $column_B must be equal at least $mostly_pct % of the time."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_table_columns_to_match_ordered_list(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_list"]
        )

        if params["column_list"] is None:
            template_str = "This table should have a list of columns in a specific order, but that order is not specified."

        else:
            template_str = "This table should have these columns in this order: "
            for idx in range(len(params["column_list"]) - 1):
                template_str += "$column_list_" + str(idx) + ", "
                params["column_list_" + str(idx)] = params["column_list"][idx]

            template_str += "$column_list_" + str(idx+1)
            params["column_list_" + str(idx+1)] = params["column_list"][idx+1]

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_multicolumn_values_to_be_unique(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_list", "ignore_row_if"]
        )

        template_str = "Values must always be unique across columns: "
        for idx in range(len(params["column_list"]) - 1):
            template_str += "$column_list_" + str(idx) + ", "
            params["column_list_" + str(idx)] = params["column_list"][idx]

        template_str += "$column_list_" + str(idx + 1)
        params["column_list_" + str(idx + 1)] = params["column_list"][idx + 1]

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_table_row_count_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["min_value", "max_value"]
        )

        if params["min_value"] is not None and params["max_value"] is not None:
            template_str = "Must have between $min_value and $max_value rows."

        elif params["min_value"] is None:
            template_str = "Must have less than than $max_value rows."

        elif params["max_value"] is None:
            template_str = "Must have more than $min_value rows."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_table_row_count_to_equal(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["value"]
        )

        template_str = "Must have exactly $value rows."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_distinct_values_to_be_in_set(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set"],
        )

        if params["value_set"] is None:

            if include_column_name:
                template_str = "$column values must belong to a set, but that set is not specified."
            else:
                template_str = "values must belong to a set, but that set is not specified."

        else:

            for i, v in enumerate(params["value_set"]):
                params["v__"+str(i)] = v
            values_string = " ".join(
                ["$v__"+str(i) for i, v in enumerate(params["value_set"])]
            )

            if include_column_name:
                template_str = "$column values must belong to this set: "+values_string+"."
            else:
                template_str = "values must belong to this set: "+values_string+"."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_values_to_not_match_regex(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "regex", "mostly"],
        )

        if include_column_name:
            template_str = "$column values must not match this regular expression: $regex."
        else:
            template_str = "values must not match this regular expression: $regex."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_values_to_not_be_null(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "mostly"],
        )

        if include_column_name:
            template_str = "$column values must never be null."
        else:
            template_str = "values must never be null."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_proportion_of_unique_values_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value"],
        )

        if params["min_value"] is None and params["max_value"] is None:
            template_str = "may have any percentage of unique values."
        elif params["min_value"] is None:
            template_str = "must have no more than $max_value% unique values."
        elif params["max_value"] is None:
            template_str = "must have at least $min_value% unique values."
        else:
            template_str = "must have between $min_value and $max_value% unique values."

        if include_column_name:
            template_str = "$column " + template_str

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_values_to_be_unique(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", ],
        )

        if include_column_name:
            template_str = "$column values must be unique."
        else:
            template_str = "values must be unique."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_values_to_be_in_type_list(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "type_list", "mostly"],
        )

        for i, v in enumerate(params["type_list"]):
            params["v__"+str(i)] = v
        values_string = " ".join(
            ["$v__"+str(i) for i, v in enumerate(params["type_list"])]
        )

        if include_column_name:
            # NOTE: Localization will be tricky for this template_str.
            template_str = "$column value types must belong to this set: "+values_string+"."
        else:
            # NOTE: Localization will be tricky for this template_str.
            template_str = "value types must belong to this set: "+values_string+"."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]
