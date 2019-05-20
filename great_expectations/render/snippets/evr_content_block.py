import json

import pandas as pd
import altair as alt

from .base import SnippetRenderer
from .util import render_parameter


class EvrContentBlockSnippetRenderer(SnippetRenderer):
    """Render EVRs to ContentBlocks in view_model.default

    Notes:
    * Many EVRs probably aren't renderable this way.
    * I'm not 100% sure that this should be a SnippetRenderer class. It might work better as a view_model.
    """

    @classmethod
    def render(cls, evr, result_key):
        cls.validate_input(evr)
        expectation_type = evr["expectation_config"]["expectation_type"]

        content_block_fn = getattr(cls, expectation_type)
        if content_block_fn is not None:
            return content_block_fn(evr, result_key)
        else:
            raise NotImplementedError

    @classmethod
    def expect_column_values_to_be_in_set(cls, evr, result_key):
        new_block = None
        if result_key == "partial_unexpected_counts":

            partial_unexpected_counts = evr["result"]["partial_unexpected_counts"]
            if len(partial_unexpected_counts) > 10:
                new_block = {
                    "content_block_type": "text",
                    "content": [
                        "<b>Example values:</b><br/> " + ", ".join([
                            render_parameter(str(item["value"]), "s") for item in partial_unexpected_counts
                        ])
                    ]
                }

            else:
                df = pd.DataFrame(partial_unexpected_counts)

                bars = alt.Chart(df).mark_bar().encode(
                    x='count:Q',
                    y="value:O"
                ).properties(height=40+20*len(partial_unexpected_counts), width=240)

                text = bars.mark_text(
                    align='left',
                    baseline='middle',
                    dx=3  # Nudges text to right so it doesn't appear on top of the bar
                ).encode(
                    text='count:Q'
                )

                chart = (bars + text).properties(height=900)

                new_block = {
                    "content_block_type": "graph",
                    "content": [chart.to_json()]
                }

        elif result_key == "partial_unexpected_list":
            partial_unexpected_list = evr["result"]["partial_unexpected_list"]
            new_block = {
                "content_block_type": "text",
                "content": [
                    "<b>Example values:</b><br/> " + ", ".join([
                            render_parameter(str(item), "s") for item in partial_unexpected_list
                    ])
                ]
            }

        return new_block


# Create a function map for our SnippetRenderer class.
# Because our snippet functions are classmethods, this must be done after the class is declared.
# https://stackoverflow.com/questions/11058686/various-errors-in-code-that-tries-to-call-classmethods
# EvrContentBlockSnippetRenderer.supported_expectation_types = {
#     "expect_column_values_to_be_in_set": EvrContentBlockSnippetRenderer._expect_column_values_to_be_in_set,
# }
