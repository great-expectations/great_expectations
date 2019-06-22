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
        return [{
            "template": column_name + " is a required field.",
            "params": {}
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
