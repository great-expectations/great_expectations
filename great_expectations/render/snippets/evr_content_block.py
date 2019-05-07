import json

import pandas as pd
import altair as alt

from . import SnippetRenderer
from .util import render_parameter


class EvrContentBlockSnippetRenderer(SnippetRenderer):

    @classmethod
    def render(cls, evr):
        expectation_type = evr["expectation_config"]["expectation_type"]
        if expectation_type in cls.supported_expectation_types:
            return cls.supported_expectation_types[expectation_type](evr)
        else:
            raise NotImplementedError

    @classmethod
    def _expect_column_values_to_be_in_set(cls, evr):

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
                "content": [json.loads(chart.to_json())]
            }

        # return json.dumps(new_block, indent=2)
        return new_block


EvrContentBlockSnippetRenderer.supported_expectation_types = {
    "expect_column_values_to_be_in_set": EvrContentBlockSnippetRenderer._expect_column_values_to_be_in_set,
}
