# -*- coding: utf-8 -*-

import copy
import json

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


class PrescriptiveStringRenderer(ContentBlockRenderer):
    # Unicode: 9601, 9602, 9603, 9604, 9605, 9606, 9607, 9608
    bar = '▁▂▃▄▅▆▇█'
    barcount = len(bar)

    @classmethod
    def sparkline(cls, weights):
        """Builds a unicode-text based sparkline for the provided histogram.

        Code from https://rosettacode.org/wiki/Sparkline_in_unicode#Python
        """
        mn, mx = min(weights), max(weights)
        extent = mx - mn
        sparkline = ''.join(cls.bar[min([cls.barcount - 1,
                                         int((n - mn) / extent * cls.barcount)])]
                            for n in weights)
        return sparkline, mn, mx

    @classmethod
    def _missing_content_block_fn(cls, expectation, styling=None, include_column_name=True):
        return [{
            "content_block_type": "string_template",
            "styling": {
              "parent": {
                  "classes": ["alert", "alert-warning"]
              }
            },
            "string_template": {
                "template": "$expectation_type(**$kwargs)",
                "params": {
                    "expectation_type": expectation["expectation_type"],
                    "kwargs": expectation["kwargs"]
                },
                "styling": {
                    "params": {
                        "expectation_type": {
                            "classes": ["badge", "badge-warning"],
                        }
                    }
                },
            }
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
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_unique_value_count_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"],
        )
        
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "may have any number of unique values."
        else:
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"]*100,)
                if params["min_value"] is None:
                    template_str = "must have fewer than $max_value unique values, at least $mostly_pct % of the time."
                elif params["max_value"] is None:
                    template_str = "must have more than $min_value unique values, at least $mostly_pct % of the time."
                else:
                    template_str = "must have between $min_value and $max_value unique values, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is None:
                    template_str = "must have fewer than $max_value unique values."
                elif params["max_value"] is None:
                    template_str = "must have more than $min_value unique values."
                else:
                    template_str = "must have between $min_value and $max_value unique values."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
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
        else:
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"]*100,)
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = "values must be between $min_value and $max_value, at least $mostly_pct % of the time."
                
                elif params["min_value"] is None:
                    template_str = "values must be less than $max_value, at least $mostly_pct % of the time."
                
                elif params["max_value"] is None:
                    template_str = "values must be less than $max_value, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = "values must always be between $min_value and $max_value."
                
                elif params["min_value"] is None:
                    template_str = "values must always be less than $max_value."
                
                elif params["max_value"] is None:
                    template_str = "values must always be more than $min_value."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
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
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            if params["or_equal"] in [None, False]:
                template_str = "Values in $column_A must be greater than those in $column_B, at least $mostly_pct % of the time."
            else:
                template_str = "Values in $column_A must be greater than or equal to those in $column_B, at least $mostly_pct % of the time."
        
        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
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
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str = "Values in $column_A and $column_B must be equal, at least $mostly_pct % of the time."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
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
            
            template_str += "$column_list_" + str(idx + 1)
            params["column_list_" + str(idx + 1)] = params["column_list"][idx + 1]
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
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
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_table_row_count_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["min_value", "max_value"]
        )
        
        if params["min_value"] is None and params["max_value"] is None:
            template_str = "May have any number of rows."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "Must have between $min_value and $max_value rows."
            elif params["min_value"] is None:
                template_str = "Must have less than than $max_value rows."
            elif params["max_value"] is None:
                template_str = "Must have more than $min_value rows."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_table_row_count_to_equal(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["value"]
        )
        
        template_str = "Must have exactly $value rows."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_distinct_values_to_be_in_set(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set"],
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
                template_str = "$column distinct values must belong to this set: " + values_string + "."
            else:
                template_str = "distinct values must belong to this set: " + values_string + "."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_not_be_null(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "mostly"],
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            if include_column_name:
                template_str = "$column values must not be null, at least $mostly_pct % of the time."
            else:
                template_str = "values must not be null, at least $mostly_pct % of the time."
        else:
            if include_column_name:
                template_str = "$column values must never be null."
            else:
                template_str = "values must never be null."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_be_null(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "mostly"]
        )
        
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str = "values must be null, at least $mostly_pct % of the time."
        else:
            template_str = "values must be null."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_be_of_type(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "type_", "mostly"]
        )
        
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str = "values must be of type $type_, at least $mostly_pct % of the time."
        else:
            template_str = "values must be of type $type_."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_be_in_type_list(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "type_list", "mostly"],
        )
        
        for i, v in enumerate(params["type_list"]):
            params["v__" + str(i)] = v
        values_string = " ".join(
            ["$v__" + str(i) for i, v in enumerate(params["type_list"])]
        )

        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            
            if include_column_name:
                # NOTE: Localization will be tricky for this template_str.
                template_str = "$column value types must belong to this set: " + values_string + ", at least $mostly_pct % of the time."
            else:
                # NOTE: Localization will be tricky for this template_str.
                template_str = "value types must belong to this set: " + values_string + ", at least $mostly_pct % of the time."
        else:
            if include_column_name:
                # NOTE: Localization will be tricky for this template_str.
                template_str = "$column value types must belong to this set: " + values_string + "."
            else:
                # NOTE: Localization will be tricky for this template_str.
                template_str = "value types must belong to this set: " + values_string + "."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_be_in_set(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set", "mostly", "parse_strings_as_datetimes"]
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
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_not_be_in_set(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set", "mostly", "parse_strings_as_datetimes"]
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
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."
        
        if include_column_name:
            template_str = "$column"
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_proportion_of_unique_values_to_be_between(cls, expectation, styling=None,
                                                                include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value"],
        )
        
        if params["min_value"] is None and params["max_value"] is None:
            template_str = "may have any percentage of unique values."
        else:
            if params["min_value"] is None:
                template_str = "must have no more than $max_value% unique values."
            elif params["max_value"] is None:
                template_str = "must have at least $min_value% unique values."
            else:
                template_str = "must have between $min_value and $max_value% unique values."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    # TODO: test parse_strings_as_datetimes
    @classmethod
    def expect_column_values_to_be_increasing(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "strictly", "mostly", "parse_strings_as_datetimes"]
        )
        
        if params.get("strictly"):
            template_str = "values must be strictly greater than previous values"
        else:
            template_str = "values must be greater than or equal to previous values"
            
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    # TODO: test parse_strings_as_datetimes
    @classmethod
    def expect_column_values_to_be_decreasing(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "strictly", "mostly", "parse_strings_as_datetimes"]
        )
        
        if params.get("strictly"):
            template_str = "values must be strictly less than previous values"
        else:
            template_str = "values must be less than or equal to previous values"
    
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_value_lengths_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "mostly"],
        )
        
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "values may have any length."
        else:
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"]*100,)
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = "values must be between $min_value and $max_value characters long, at least $mostly_pct % of the time."
                
                elif params["min_value"] is None:
                    template_str = "values must be less than $max_value characters long, at least $mostly_pct % of the time."
                
                elif params["max_value"] is None:
                    template_str = "values must be more than $min_value characters long, at least $mostly_pct % of the time."
            else:
                if params["min_value"] is not None and params["max_value"] is not None:
                    template_str = "values must always be between $min_value and $max_value characters long."
                
                elif params["min_value"] is None:
                    template_str = "values must always be less than $max_value characters long."
                
                elif params["max_value"] is None:
                    template_str = "values must always be more than $min_value characters long."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_value_lengths_to_equal(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value", "mostly"]
        )
        
        if params.get("value") is None:
            template_str = "values may have any length."
        else:
            template_str = "values must be $value characters long"
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_match_regex(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "regex", "mostly"]
        )
        
        if not params.get("regex"):
            template_str = "values must match a regular expression but none was specified."
        else:
            template_str = "values must match this regular expression: $regex"
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_not_match_regex(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "regex", "mostly"],
        )
        
        if not params.get("regex"):
            template_str = "values must not match a regular expression but none was specified."
        else:
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
                if include_column_name:
                    template_str = "$column values must not match this regular expression: $regex, at least $mostly_pct % of the time."
                else:
                    template_str = "values must not match this regular expression: $regex, at least $mostly_pct % of the time."
            else:
                if include_column_name:
                    template_str = "$column values must not match this regular expression: $regex."
                else:
                    template_str = "values must not match this regular expression: $regex."
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_match_regex_list(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "regex_list", "mostly", "match_on"],
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
            template_str = "values must match all of the following regular expressions: " + values_string
        else:
            template_str = "values must match any of the following regular expressions: " + values_string
            
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_not_match_regex_list(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "regex_list", "mostly"],
        )
        
        if not params.get("regex_list") or len(params.get("regex_list")) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["regex_list"]):
                params["v__" + str(i)] = v
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["regex_list"])]
            )
            
        template_str = "values must not match any of the following regular expressions: " + values_string
        
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_match_strftime_format(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "strftime_format", "mostly"],
        )
        
        if not params.get("strftime_format"):
            template_str = "values must match a strftime format but none was specified."
        else:
            template_str = "values must match the following strftime format: $strftime_format"
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
                template_str += ", at least $mostly_pct % of the time."
            else:
                template_str += "."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_be_dateutil_parseable(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "mostly"],
        )
        
        template_str = "values must be parseable by dateutil"
        
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_be_json_parseable(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "mostly"],
        )
        
        template_str = "values must be parseable as JSON"
    
        if params["mostly"] is not None:
            params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
            template_str += ", at least $mostly_pct % of the time."
        else:
            template_str += "."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_values_to_match_json_schema(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "mostly", "json_schema"],
        )
        
        if not params.get("json_schema"):
            template_str = "values must match a JSON Schema but none was specified."
        else:
            params["formatted_json"] = "<pre>" + json.dumps(params.get("json_schema"), indent=4) + "</pre>"
            if params["mostly"] is not None:
                params["mostly_pct"] = "%.1f" % (params["mostly"] * 100,)
                template_str = "values must match the following JSON Schema, at least $mostly_pct % of the time: $formatted_json"
            else:
                template_str = "values must match the following JSON Schema: $formatted_json"
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": {
                    "params":
                        {
                            "formatted_json": {
                                "classes": []
                            }
                        }
                },
            }
        }]
    
    @classmethod
    def expect_column_distinct_values_to_contain_set(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set", "parse_strings_as_datetimes"]
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
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_distinct_values_to_equal_set(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set", "parse_strings_as_datetimes"]
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
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_mean_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value"]
        )
        
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "mean may have any numerical value."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "mean must be between $min_value and $max_value."
            elif params["min_value"] is None:
                template_str = "mean must be less than $max_value."
            elif params["max_value"] is None:
                template_str = "mean must be more than $min_value."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]

    @classmethod
    def expect_column_median_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value"]
        )
    
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "median may have any numerical value."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "median must be between $min_value and $max_value."
            elif params["min_value"] is None:
                template_str = "median must be less than $max_value."
            elif params["max_value"] is None:
                template_str = "median must be more than $min_value."
    
        if include_column_name:
            template_str = "$column " + template_str
    
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
        
    @classmethod
    def expect_column_stdev_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value"]
        )
    
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "standard deviation may have any numerical value."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "standard deviation must be between $min_value and $max_value."
            elif params["min_value"] is None:
                template_str = "standard deviation must be less than $max_value."
            elif params["max_value"] is None:
                template_str = "standard deviation must be more than $min_value."
    
        if include_column_name:
            template_str = "$column " + template_str
    
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
        
    @classmethod
    def expect_column_max_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "parse_strings_as_datetimes"]
        )
    
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "maximum value may have any numerical value."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "maximum value must be between $min_value and $max_value."
            elif params["min_value"] is None:
                template_str = "maximum value must be less than $max_value."
            elif params["max_value"] is None:
                template_str = "maximum value must be more than $min_value."
    
        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."
    
        if include_column_name:
            template_str = "$column " + template_str
    
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_min_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value", "parse_strings_as_datetimes"]
        )
        
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "minimum value may have any numerical value."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "minimum value must be between $min_value and $max_value."
            elif params["min_value"] is None:
                template_str = "minimum value must be less than $max_value."
            elif params["max_value"] is None:
                template_str = "minimum value must be more than $min_value."
        
        if params.get("parse_strings_as_datetimes"):
            template_str += " Values should be parsed as datetimes."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_sum_to_be_between(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "min_value", "max_value"]
        )
        
        if (params["min_value"] is None) and (params["max_value"] is None):
            template_str = "sum may have any numerical value."
        else:
            if params["min_value"] is not None and params["max_value"] is not None:
                template_str = "sum must be between $min_value and $max_value."
            elif params["min_value"] is None:
                template_str = "sum must be less than $max_value."
            elif params["max_value"] is None:
                template_str = "sum must be more than $min_value."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_most_common_value_to_be_in_set(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "value_set", "ties_okay"]
        )
        
        if params["value_set"] is None or len(params["value_set"]) == 0:
            values_string = "[ ]"
        else:
            for i, v in enumerate(params["value_set"]):
                params["v__" + str(i)] = v
            
            values_string = " ".join(
                ["$v__" + str(i) for i, v in enumerate(params["value_set"])]
            )
            
        template_str = "most common value must belong to this set: " + values_string + "."
        
        if params.get("ties_okay"):
            template_str += " Values outside this set that are as common (but not more common) are allowed."
        
        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]
    
    @classmethod
    def expect_column_kl_divergence_to_be_less_than(cls, expectation, styling=None, include_column_name=True):
        params = substitute_none_for_missing(
            expectation["kwargs"],
            ["column", "partition_object", "threshold"]
        )

        styling.update({
            "params": {
                "sparklines_histogram": {
                    "styles": {
                        "font-family": "serif !important"
                    }
                }
            }
        })
        
        if not params.get("partition_object"):
            template_str = "Kullback-Leibler (KL) divergence with respect to a given distribution must be lower than a \
                provided threshold but no distribution was specified."
        else:
            params["sparklines_histogram"]= cls.sparkline(params.get("partition_object")["weights"])[0]
            template_str = "Kullback-Leibler (KL) divergence with respect to the following distribution must be " \
                           "lower than $threshold: $sparklines_histogram"

        if include_column_name:
            template_str = "$column " + template_str
        
        return [{
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
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
            "content_block_type": "string_template",
            "string_template": {
                "template": template_str,
                "params": params,
                "styling": styling,
            }
        }]


class PrescriptiveEvrTableContentBlockRenderer(PrescriptiveStringRenderer):
    _content_block_type = "table"

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
    
    _default_content_block_styling = {
        "body": {
            "classes": ["table"],
        },
        "classes": ["m-3", "table-responsive"],
    }

    @classmethod
    def _get_status_icon(cls, evr):
        if evr["success"]:
            return {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$icon",
                    "params": {"icon": ""},
                    "styling": {
                        "params": {
                            "icon": {
                                "classes": ["fas", "fa-check-circle", "text-success"],
                                "tag": "i"
                            }
                        }
                    }
                }
            }
        else:
            return {
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$icon",
                    "params": {"icon": ""},
                    "styling": {
                        "params": {
                            "icon": {
                                "tag": "i",
                                "classes": ["fas", "fa-times", "text-danger"]
                            }
                        }
                    }
                }
            }

    @classmethod
    def _get_exception_table(cls, evr):
        result = evr["result"]
        if not result.get("partial_unexpected_list") and not result.get("partial_unexpected_counts"):
            return None
        
        table_rows = []
        
        if result.get("partial_unexpected_counts"):
            header_row = ["Unexpected Value", "Count"]
            for unexpected_count in result.get("partial_unexpected_counts"):
                if unexpected_count.get("value"):
                    table_rows.append([unexpected_count.get("value"), unexpected_count.get("count")])
                else:
                    table_rows.append(["null", unexpected_count.get("count")])
        else:
            header_row = ["Unexpected Value"]
            for unexpected_value in result.get("partial_unexpected_list"):
                if unexpected_value.get("value"):
                    table_rows.append([unexpected_value])
                else:
                    table_rows.append(["null"])
                    
        exception_table_content_block = {
            "content_block_type": "table",
            "table": table_rows,
            "header_row": header_row,
            "styling": {
                "body": {
                    "classes": ["table-bordered", "table-sm", "mt-3"]
                }
            }
        }
        
        return exception_table_content_block

    @classmethod
    def _get_exception_statement(cls, evr):
        success = evr["success"]
        result = evr["result"]
        
        if success or not result.get("unexpected_count"):
            return None
        else:
            unexpected_count = result["unexpected_count"]
            unexpected_percent = "%.2f%%" % (result["unexpected_percent"] * 100.0)
            element_count = result["element_count"]
            
            template_str = "\n\n$unexpected_count exceptions found. $unexpected_percent of $element_count total rows."
            
            return {
                "content_block_type": "string_template",
                "string_template": {
                    "template": template_str,
                    "params": {
                        "unexpected_count": unexpected_count,
                        "unexpected_percent": unexpected_percent,
                        "element_count": element_count
                    },
                    "tag": "strong",
                    "styling": {
                        "classes": ["text-danger"]
                    }
                }
            }

    @classmethod
    def _get_observed_value(cls, evr):
        result = evr["result"]
        expectation_type = evr["expectation_config"]["expectation_type"]

        if result.get("observed_value"):
            return result.get("observed_value")
        elif expectation_type == "expect_column_values_to_be_null":
            element_count = result["element_count"]
            unexpected_count = result["unexpected_count"]
            null_count = element_count - unexpected_count
            return "{null_count} null".format(null_count=null_count)
        elif expectation_type == "expect_column_values_to_not_be_null":
            null_count = result["unexpected_count"]
            return "{null_count} null".format(null_count=null_count)
        else:
            return "--"

    @classmethod
    def generate_expectation_row(cls, expectation_type):
        prescriptive_string_fn = getattr(PrescriptiveStringRenderer, expectation_type, None)
        if prescriptive_string_fn is None:
            prescriptive_string_fn = getattr(PrescriptiveStringRenderer, "_missing_content_block_fn")
        
        def row_generator_fn(evr, styling=None, include_column_name=True):
            expectation = evr["expectation_config"]
            prescriptive_string_obj = prescriptive_string_fn(expectation, styling, include_column_name)
            
            status_cell = [cls._get_status_icon(evr)]
            exception_statement = cls._get_exception_statement(evr)
            exception_table = cls._get_exception_table(evr)
            expectation_cell = prescriptive_string_obj
            observed_value = [str(cls._get_observed_value(evr))]

            if exception_statement or exception_table:
                prescriptive_string_obj.append(exception_statement)
                prescriptive_string_obj.append(exception_table)
                return [status_cell + [expectation_cell] + observed_value]
            
            return [status_cell + expectation_cell + observed_value]
        
        return row_generator_fn


class PrescriptiveBulletListContentBlockRenderer(PrescriptiveStringRenderer):
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

