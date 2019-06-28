import copy
import datetime

from .content_block import ContentBlockRenderer


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
        if not kwarg in new_kwargs:
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
            "template": "Couldn't render expectation of type $expectation_type",
            "params": {
                "expectation_type": expectation["expectation_type"],
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

        if params["column_index"] == None:
            if include_column_name:
                template_str = "$column is a required field."
            else:
                template_str = "is a required field."
        else:
            #!!! FIXME: this works for 4th, 5th, 6th, etc, but is dumb about 1th, 2th, and 3th.
            params["column_indexth"] = str(params["column_index"])+"th"
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
            if include_column_name:
                template_str = "$column has a bogus `expect_column_value_lengths_to_be_between` expectation."
            else:
                template_str = "has a bogus `expect_column_value_lengths_to_be_between` expectation."

        if "mostly" in params:
            if params["min_value"] is not None and params["max_value"] is not None:
                if include_column_name:
                    template_str = "$column must be between $min_value and $max_value characters long at least $mostly% of the time."
                else:
                    template_str = "must be between $min_value and $max_value characters long at least $mostly% of the time."

            elif params["min_value"] is None:
                if include_column_name:
                    template_str = "$column must be less than $max_value characters long at least $mostly% of the time."
                else:
                    template_str = "must be less than $max_value characters long at least $mostly% of the time."

            elif params["max_value"] is None:
                if include_column_name:
                    template_str = "$column must be more than $min_value characters long at least $mostly% of the time."
                else:
                    template_str = "must be more than $min_value characters long at least $mostly% of the time."

        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                if include_column_name:
                    template_str = "$column must always be between $min_value and $max_value characters long."
                else:
                    template_str = "must always be between $min_value and $max_value characters long."

            elif params["min_value"] is None:
                if include_column_name:
                    template_str = "$column must always be less than $max_value characters long."
                else:
                    template_str = "must always be less than $max_value characters long."

            elif params["max_value"] is None:
                if include_column_name:
                    template_str = "$column must always be more than $min_value characters long."
                else:
                    template_str = "must always be more than $min_value characters long."

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
            if include_column_name:
                template_str = "$column has a bogus `expect_column_unique_value_count_to_be_between` expectation."
            else:
                template_str = "has a bogus `expect_column_unique_value_count_to_be_between` expectation."
        elif params["min_value"] is None:
            if include_column_name:
                template_str = "$column must have fewer than $max_value unique values."
            else:
                template_str = "must have fewer than $max_value unique values."
        elif params["max_value"] is None:
            if include_column_name:
                template_str = "$column must have at least $min_value unique values."
            else:
                template_str = "must have fewer than $max_value unique values."
        else:
            if include_column_name:
                template_str = "$column must have between $min_value and $max_value unique values."
            else:
                template_str = "must have between $min_value and $max_value unique values."

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
            params["expectation_name"] = "expect_column_values_to_be_between"
            template_str = "$column has a bogus $expectation_name expectation."

        if "mostly" in params:
            if params["min_value"] is not None and params["max_value"] is not None:
                if include_column_name:
                    template_str = "$column must be between $min_value and $max_value at least $mostly% of the time."
                else:
                    template_str = "must be between $min_value and $max_value at least $mostly% of the time."

            elif params["min_value"] is None:
                if include_column_name:
                    template_str = "$column must be less than $max_value at least $mostly% of the time."
                else:
                    template_str = "must be less than $max_value at least $mostly% of the time."

            elif params["max_value"] is None:
                if include_column_name:
                    template_str = "$column must be more than $min_value at least $mostly% of the time."
                else:
                    template_str = "must be less than $max_value at least $mostly% of the time."

        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                if include_column_name:
                    template_str = "$column must always be between $min_value and $max_value."
                else:
                    template_str = "must always be between $min_value and $max_value."

            elif params["min_value"] is None:
                if include_column_name:
                    template_str = "$column must always be less than $max_value."
                else:
                    template_str = "must always be less than $max_value."

            elif params["max_value"] is None:
                if include_column_name:
                    template_str = "$column must always be more than $min_value."
                else:
                    template_str = "must always be more than $min_value."

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

        if params["mostly"] == None:
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
            # FIXME: this string is wrong
            template_str = " has a bogus $expectation_name expectation."

        if params["mostly"] == None:
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

        # FIXME: This is slightly wrong, since the whole string (including commas) will get syntax highlighting.
        # It would be better to have each element highlighted separately, but I need to research methods to do this elegantly.
        params["column_list_str"] = ", ".join(params["column_list"])
        template_str = "This table should have these columns in this order: $column_list_str"

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

        # FIXME: This is slightly wrong, since the whole string (including commas) will get syntax highlighting.
        # It would be better to have each element highlighted separately, but I need to research methods to do this elegantly.
        params["column_list_str"] = ", ".join(params["column_list"])
        template_str = "Values must always be unique across columns: $column_list_str"

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
        # TODO: thoroughly review this method. It was implemented quickly and hackily.
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set"],
        )

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
        # TODO: thoroughly review this method. It was implemented quickly and hackily.
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
        # TODO: thoroughly review this method. It was implemented quickly and hackily.
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
        # TODO: thoroughly review this method. It was implemented quickly and hackily.
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value"],
        )

        if include_column_name:
            template_str = "$column must have between $min_value and $max_value% unique values."
        else:
            template_str = "must have between $min_value and $max_value% unique values."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]

    @classmethod
    def expect_column_values_to_be_unique(cls, expectation, styling=None, include_column_name=True):
        # TODO: thoroughly review this method. It was implemented quickly and hackily.
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
        # TODO: thoroughly review this method. It was implemented quickly and hackily.
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
            template_str = "$column values must belong to this set: "+values_string+"."
        else:
            template_str = "values must belong to this set: "+values_string+"."

        return [{
            "template": template_str,
            "params": params,
            "styling": styling,
        }]
