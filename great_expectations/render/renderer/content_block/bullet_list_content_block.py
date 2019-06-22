import copy

from .content_block import ContentBlock


def substitute_none_for_missing(kwargs, kwarg_list):
    new_kwargs = copy.deepcopy(kwargs)
    for kwarg in kwarg_list:
        if not kwarg in new_kwargs:
            new_kwargs[kwarg] = None
    return new_kwargs


class BulletListContentBlock(ContentBlock):
    _content_block_type = "bullet_list"

    @classmethod
    def expect_column_to_exist(cls, expectation, column_name=""):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "column_index"],
        )

        if params["column_index"] == None:
            return [{
                "template": "$column is a required field.",
                "params": params,
            }]

        else:
            #!!! FIXME:
            params["column_indexth"] = str(params["column_index"])+"th"
            return [{
                "template": "$column must be the $column_indexth field.",
                "params": params,
            }]

    @classmethod
    def expect_column_value_lengths_to_be_between(cls, expectation, column_name=""):
        if (expectation["kwargs"]["min_value"] is None) and (expectation["kwargs"]["max_value"] is None):
            return [{
                "template": column_name + " has a bogus $expectation_name expectation.",
                "params": {
                    "expectation_name": "expect_column_value_lengths_to_be_between"
                }
            }]

        if "mostly" in expectation["kwargs"]:
            if expectation["kwargs"]["min_value"] is not None and expectation["kwargs"]["max_value"] is not None:
                return [{
                    "template": column_name + " must be between $min and $max characters long at least $mostly% of the time.",
                    "params": {
                        "min": expectation["kwargs"]["min_value"],
                        "max": expectation["kwargs"]["max_value"],
                        "mostly": expectation["kwargs"]["mostly"]
                    }
                }]

            elif expectation["kwargs"]["min_value"] is None:
                return [{
                    "template": column_name + " must be less than $max characters long at least $mostly% of the time.",
                    "params": {
                        "max": expectation["kwargs"]["max_value"],
                        "mostly": expectation["kwargs"]["mostly"]
                    }
                }]

            elif expectation["kwargs"]["max_value"] is None:
                return [{
                    "template": column_name + " must be more than $min characters long at least $mostly% of the time.",
                    "params": {
                        "min": expectation["kwargs"]["min_value"],
                        "mostly": expectation["kwargs"]["mostly"]
                    }
                }]

        else:
            if expectation["kwargs"]["min_value"] is not None and expectation["kwargs"]["max_value"] is not None:
                return [{
                    "template": column_name + " must always be between $min and $max characters long.",
                    "params": {
                        "min": expectation["kwargs"]["min_value"],
                        "max": expectation["kwargs"]["max_value"]
                    }
                }]

            elif expectation["kwargs"]["min_value"] is None:
                return [{
                    "template": column_name + " must always be less than $max characters long.",
                    "params": {
                        "max": expectation["kwargs"]["max_value"]
                    }
                }]

            elif expectation["kwargs"]["max_value"] is None:
                return [{
                    "template": column_name + " must always be more than $min characters long.",
                    "params": {
                        "min": expectation["kwargs"]["min_value"]
                    }
                }]

    @classmethod
    def expect_column_unique_value_count_to_be_between(cls, expectation, column_name=""):
        if (expectation["kwargs"]["min_value"] is None) and (expectation["kwargs"]["max_value"] is None):
            return [{
                "template": column_name + " has a bogus $expectation_name expectation.",
                "params": {
                    "expectation_name": "expect_column_unique_value_count_to_be_between"
                }
            }]

        elif expectation["kwargs"]["min_value"] is None:
            return [{
                "template": column_name + " must have fewer than $max unique values.",
                "params": {
                    "max": expectation["kwargs"]["max_value"]
                }
            }]

        elif expectation["kwargs"]["max_value"] is None:
            return [{
                "template": column_name + " must have at least $min unique values.",
                "params": {
                    "min": expectation["kwargs"]["min_value"]
                }
            }]
        else:
            return [{
                "template": column_name + " must have between $min and $max unique values.",
                "params": {
                    "min": expectation["kwargs"]["min_value"],
                    "max": expectation["kwargs"]["min_value"]
                }
            }]

    @classmethod
    def expect_column_values_to_be_between(cls, expectation, column_name=""):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"]
        )

        if (params["min_value"] is None) and (params["max_value"] is None):
            #!!! Not sure why we're using a different pattern for templating column names...
            return [{
                "template": column_name + " has a bogus $expectation_name expectation.",
                "params": {
                    "expectation_name": "expect_column_values_to_be_between"
                }
            }]

        if "mostly" in params:
            if params["min_value"] is not None and params["max_value"] is not None:
                return [{
                    "template": column_name + " must be between $min and $max at least $mostly% of the time.",
                    "params": params
                }]

            elif params["min_value"] is None:
                return [{
                    "template": column_name + " must be less than $max at least $mostly% of the time.",
                    "params": params
                }]

            elif params["max_value"] is None:
                return [{
                    "template": column_name + " must be more than $min at least $mostly% of the time.",
                    "params": params
                }]

        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                return [{
                    "template": column_name + " must always be between $min and $max.",
                    "params": params
                }]

            elif params["min_value"] is None:
                return [{
                    "template": column_name + " must always be less than $max.",
                    "params": params
                }]

            elif params["max_value"] is None:
                return [{
                    "template": column_name + " must always be more than $min.",
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
            # FIXME: this string is wrong
            return [{
                "template": " has a bogus $expectation_name expectation.",
                "params": {
                    "expectation_name": "expect_column_pair_values_A_to_be_greater_than_B"
                }
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
            return [{
                "template": "Values in $column_A and $column_B must be equal at least $mostly % of the time.",
                "params": params
            }]

    @classmethod
    def expect_table_columns_to_match_ordered_list(cls, expectation):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column_list"]
        )

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

        params["column_list_str"] = ", ".join(params["column_list"])
        return [{
            "template": "Values must always be unique across columns: $column_list_str",
            "params": params
        }]
