import json

from . import SnippetRenderer
from .util import render_parameter


class EvrTableRowSnippetRenderer(SnippetRenderer):
    def __init__(self, evr):
        self.evr = evr

    def validate_input(self, expectation):
        return True

    def render(self):
        """
        Returns: a list of table rows to render

        Note: this does NOT return a single row. It's always a list!
        """
        evr = self.evr

        expectation_type = evr["expectation_config"]["expectation_type"]
        if "result" in evr:
            if "unexpected_percent" in evr["result"]:
                unexpected_percent = evr["result"]["unexpected_percent"]
            else:
                unexpected_percent = None

            if "unexpected_count" in evr["result"]:
                unexpected_count = evr["result"]["unexpected_count"]
            else:
                unexpected_count = None

        if "result" in evr and "observed_value" in evr["result"]:
            observed_value = evr["result"]["observed_value"]
        else:
            observed_value = None

        unrenderable_expectation_types = [
            "expect_column_to_exist",
            "expect_column_values_to_be_of_type",
            "expect_column_values_to_be_unique",
            "expect_column_values_to_be_increasing",
            "expect_column_values_to_be_between",
            "expect_column_values_to_be_in_set",
        ]
        if expectation_type in unrenderable_expectation_types:
            return None

        if expectation_type == "expect_column_values_to_not_be_null":
            return [
                ["Missing (%)", "%.1f%%" % unexpected_percent],
                ["Missing (n)", str(unexpected_count)],
            ]

        if expectation_type == "expect_column_values_to_not_match_regex":
            # print(evr["expectation_config"]["regex"])
            if evr["expectation_config"]["kwargs"]["regex"] == '^\\s+|\\s+$':
                return [["Leading or trailing whitespace (n)", unexpected_count]]

        if expectation_type == "expect_column_unique_value_count_to_be_between":
            return [["Distinct count", observed_value]]

        if expectation_type == "expect_column_proportion_of_unique_values_to_be_between":
            return [["Unique (%)", "%.1f%%" % (100*observed_value)]]

        if expectation_type == "expect_column_max_to_be_between":
            return [["Max", observed_value]]

        else:
            return [[
                expectation_type, "%.1f%%" % unexpected_percent, observed_value
            ]]
