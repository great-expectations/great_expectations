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

    @classmethod
    def missing_content_block_fn(cls, expectation, **kwargs):
        return [{
            "template": "Couldn't render expectation of type $expectation_type",
            "params": {
                "expectation_type": expectation["expectation_type"],
            },
            "styling": {
                "params": {
                    "expectation_type": {
                        "classes": ""
                    }
                }
            },
        }]

    @classmethod
    def expect_column_to_exist(cls, expectation, styling=None):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "column_index"],
        )

        if params["column_index"] == None:
            # return [{
            #     "template": "{{column}} is a required field.",
            #     "params": params,
            # }]
            return [{
                "template": "$column is a required field "+str(datetime.datetime.now()),
                "params": params,
                "styling": {
                    # "classes"
                    "tooltip": "blah",
                    "params": {
                        "column": {
                            "classes": "my_css_value_class",
                            "tooltip_text": "testing",
                        }
                    }
                }
            }]

        else:
            #!!! FIXME: this works for 4th, 5th, 6th, etc, but is dumb about 1th, 2th, and 3th.
            params["column_indexth"] = str(params["column_index"])+"th"
            return [{
                "template": "$column must be the $column_indexth field",
                "params": params,
                "styling": {
                    # "classes"
                    "tooltip": "blah",
                    "params": {
                        "column": {
                            "classes": ["class_A"],
                            "tooltip_text": "testing",
                        },
                        "column_index": {
                            "classes": ["class_B"],
                            "tooltip_text": "testing msg B",
                        }
                    }
                }
            }]

    @classmethod
    def expect_column_value_lengths_to_be_between(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            return [{
                "template": "$column has a bogus `expect_column_value_lengths_to_be_between` expectation.",
                "params": params,
            }]

        if "mostly" in params:
            if params["min_value"] is not None and params["max_value"] is not None:
                return [{
                    "template": "$column must be between $min_value and $max_value characters long at least $mostly% of the time.",
                    "params": params,
                }]

            elif params["min_value"] is None:
                return [{
                    "template": "$column must be less than $max_value characters long at least $mostly% of the time.",
                    "params": params,
                }]

            elif params["max_value"] is None:
                return [{
                    "template": "$column must be more than $min_value characters long at least $mostly% of the time.",
                    "params": params
                }]

        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                return [{
                    "template": "$column must always be between $min_value and $max_value characters long.",
                    "params": params
                }]

            elif params["min_value"] is None:
                return [{
                    "template": "$column must always be less than $max_value characters long.",
                    "params": params
                }]

            elif params["max_value"] is None:
                return [{
                    "template": "$column must always be more than $min_value characters long.",
                    "params": params
                }]

    @classmethod
    def expect_column_unique_value_count_to_be_between(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"],
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            return [{
                "template": "$column has a bogus `expect_column_unique_value_count_to_be_between` expectation.",
                "params": params
            }]

        elif params["min_value"] is None:
            return [{
                "template": "$column must have fewer than $max_value unique values.",
                "params": params
            }]

        elif params["max_value"] is None:
            return [{
                "template": "$column must have at least $min_value unique values.",
                "params": params
            }]
        else:
            return [{
                "template": "$column must have between $min_value and $max_value unique values.",
                "params": params
            }]

    # NOTE: This method is a pretty good example of good usage of `params`.
    # NOTE: I think `column_name` should be deprecated.
    @classmethod
    def expect_column_values_to_be_between(cls, expectation, column_name=""):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"]
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            # Note: I'm not sure why we're using a different pattern for templating column names...
            # This would probably be better as `"$column has a bogus $expectation_name expectation.",`

            # Related issue: are we going to want to allow for different styles of parameters?
            # For example, I could imagine wanting one color of syntax highlighting for variable names,
            # and a different color for expectations.
            # In that case, perhaps returning something like this would work?
            # {
            #     "template": "$column has a bogus $expectation_name expectation.",
            #     "params": {
            #         "column": "my_column",
            #         "expectation_name": "expect_column_values_to_be_between",
            #     },
            #     "param_types": {
            #         "column": "variable"
            #         "expectation_name": "expectation"
            #     }
            # }
            # Thoughtfully enumerating values for the param_types k/v object will be important.
            return [{
                "template": column_name + " has a bogus $expectation_name expectation.",
                "params": {
                    "expectation_name": "expect_column_values_to_be_between"
                }
            }]

        if "mostly" in params:
            if params["min_value"] is not None and params["max_value"] is not None:
                return [{
                    "template": column_name + " must be between $min_value and $max_value at least $mostly% of the time.",
                    "params": params
                }]

            elif params["min_value"] is None:
                return [{
                    "template": column_name + " must be less than $max_value at least $mostly% of the time.",
                    "params": params
                }]

            elif params["max_value"] is None:
                return [{
                    "template": column_name + " must be more than $min_value at least $mostly% of the time.",
                    "params": params
                }]

        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                return [{
                    "template": column_name + " must always be between $min_value and $max_value.",
                    "params": params
                }]

            elif params["min_value"] is None:
                return [{
                    "template": column_name + " must always be less than $max_value.",
                    "params": params
                }]

            elif params["max_value"] is None:
                return [{
                    "template": column_name + " must always be more than $min_value.",
                    "params": params
                }]

    @classmethod
    def expect_column_pair_values_A_to_be_greater_than_B(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_A", "column_B", "parse_strings_as_datetimes",
                "ignore_row_if", "mostly", "or_equal"]
        )

        if (params["column_A"] is None) or (params["column_B"] is None):
            return [{
                "template": "$column has a bogus `expect_column_pair_values_A_to_be_greater_than_B` expectation.",
                "params": params
            }]

        if params["mostly"] == None:
            if params["or_equal"] in [None, False]:
                return [{
                    "template": "Values in $column_A must always be greater than those in $column_B.",
                    "params": params
                }]
            else:
                return [{
                    "template": "Values in $column_A must always be greater than or equal to those in $column_B.",
                    "params": params
                }]

        else:
            if params["or_equal"] in [None, False]:
                return [{
                    "template": "Values in $column_A must be greater than those in $column_B at least $mostly % of the time.",
                    "params": params
                }]
            else:
                return [{
                    "template": "Values in $column_A must be greater than or equal to those in $column_B at least $mostly % of the time.",
                    "params": params
                }]

    @classmethod
    def expect_column_pair_values_to_be_equal(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_A", "column_B",
                "ignore_row_if", "mostly", ]
        )

        # NOTE: This renderer doesn't do anything with "ignore_row_if"

        if (params["column_A"] is None) or (params["column_B"] is None):
            # FIXME: this string is wrong
            return [{
                "template": " has a bogus $expectation_name expectation.",
                "params": {
                    "expectation_name": "expect_column_pair_values_to_be_equal"
                }
            }]

        if params["mostly"] == None:
            return [{
                "template": "Values in $column_A and $column_B must always be equal.",
                "params": params
            }]

        else:
            # Note: this pattern for type conversion seems to work reasonably well.
            # Note: I'm not 100% sure that this is the right place to encode details like how many decimals to show.
            params["mostly_pct"] = "%.1f" % (params["mostly"]*100,)
            return [{
                "template": "Values in $column_A and $column_B must be equal at least $mostly_pct % of the time.",
                "params": params
            }]

    @classmethod
    def expect_table_columns_to_match_ordered_list(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_list"]
        )

        # FIXME: This is slightly wrong, since the whole string (including commas) will get syntax highlighting.
        # It would be better to have each element highlighted separately, but I need to research methods to do this elegantly.
        params["column_list_str"] = ", ".join(params["column_list"])
        return [{
            "template": "This table should have these columns in this order: $column_list_str",
            "params": params
        }]

    @classmethod
    def expect_multicolumn_values_to_be_unique(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_list", "ignore_row_if"]
        )

        # FIXME: This is slightly wrong, since the whole string (including commas) will get syntax highlighting.
        # It would be better to have each element highlighted separately, but I need to research methods to do this elegantly.
        params["column_list_str"] = ", ".join(params["column_list"])
        return [{
            "template": "Values must always be unique across columns: $column_list_str",
            "params": params
        }]

    @classmethod
    def expect_table_row_count_to_be_between(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["min_value", "max_value"]
        )

        if params["min_value"] is not None and params["max_value"] is not None:
            return [{
                "template": "Must have between $min_value and $max_value rows.",
                "params": params
            }]

        elif params["min_value"] is None:
            return [{
                "template": "Must have less than than $max_value rows.",
                "params": params
            }]

        elif params["max_value"] is None:
            return [{
                "template": "Must have more than $min_value rows.",
                "params": params
            }]

    # @classmethod
    # def expect_table_row_count_to_be_between(cls, expectation):
    #     params = substitute_none_for_missing(
    #         expectation["kwargs"],
    #         ["min_value", "max_value"]
    #     )

    #     if params["min_value"] is not None and params["max_value"] is not None:
    #         return [{
    #             "template": "Must have between $min_value and $max_value rows.",
    #             "params": params
    #         }]

    #     elif params["min_value"] is None:
    #         return [{
    #             "template": "Must have less than than $max_value rows.",
    #             "params": params
    #         }]

    #     elif params["max_value"] is None:
    #         return [{
    #             "template": "Must have more than $min_value rows.",
    #             "params": params
    #         }]

    @classmethod
    def expect_table_row_count_to_equal(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["value"]
        )

        return [{
            "template": "Must have exactly $value rows.",
            "params": params
        }]
