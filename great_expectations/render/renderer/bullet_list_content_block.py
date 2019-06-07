from .content_block import ContentBlock

class BulletListContentBlock(ContentBlock):
    _content_block_type = "bullet_list"

    @classmethod
    def _expect_column_to_exist(cls, expectation, column_name=""):
        return [column_name + " is a required field."]

    @classmethod
    def _expect_column_value_lengths_to_be_between(cls, expectation, column_name=""):
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
