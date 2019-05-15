import json

from .base import SnippetRenderer
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

        # First, we add the generic ones...
        expectation_type = evr["expectation_config"]["expectation_type"]
        content_blocks = []

        if "result" in evr:
            if "unexpected_percent" in evr["result"]:
                unexpected_percent = evr["result"]["unexpected_percent"]
                content_blocks.append(
                    ["Unexpected (%)", "%.1f%%" % unexpected_percent]
                )

            if "unexpected_count" in evr["result"]:
                unexpected_count = evr["result"]["unexpected_count"]
                content_blocks.append(
                    ["Unexpected (n)", str(unexpected_count)]
                )

            if "missing_percent" in evr["result"]:
                missing_percent = evr["result"]["missing_percent"]
                content_blocks.append(
                    ["Missing (%)", "%.1f%%" % missing_percent]
                )

            if "missing_count" in evr["result"]:
                missing_count = evr["result"]["missing_count"]
                content_blocks.append(
                    ["Missing (n)", str(missing_count)]
                )

        # Now, if relevant, we add additional blocks based on expectation type
        table_row_fn = getattr(self, expectation_type)
        if table_row_fn is not None:
            content_blocks.append(table_row_fn())
     
        return content_blocks

    def expect_column_values_to_not_match_regex(self):
        regex = self.evr["expectation_config"]["kwargs"]["regex"]
        unexpected_count = self.evr["result"]["unexpected_count"]
        if regex == '^\\s+|\\s+$':
            return ["Leading or trailing whitespace (n)", unexpected_count]
        else:
            return ["Regex: %s" % regex, unexpected_count]

    def expect_column_unique_value_count_to_be_between(self):
        observed_value = self.evr["result"]["observed_value"]
        return ["Distinct count", observed_value]

    def expect_column_proportion_of_unique_values_to_be_between(self):
        observed_value = self.evr["result"]["observed_value"]
        return ["Unique (%)", "%.1f%%" % (100*observed_value)]

    def expect_column_max_to_be_between(self):
        observed_value = self.evr["result"]["observed_value"]
        return ["Max", observed_value]

    def expect_column_values_to_be_in_set(self):
        return ["test row", "garbage data"]