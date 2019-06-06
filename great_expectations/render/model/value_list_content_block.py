import pandas as pd
import altair as alt

from .content_block import ContentBlock

class ValueListContentBlock(ContentBlock):
    """Render ValuesList Style Content Blocks

    Produces content blocks with type
    values_list and graph, based on the size of the value list requested
    """

    @classmethod
    def expect_column_values_to_be_in_set(cls, evr, result_key="partial_unexpected_counts"):
        new_block = None
        if result_key == "partial_unexpected_counts":

            partial_unexpected_counts = evr["result"]["partial_unexpected_counts"]
            if len(partial_unexpected_counts) > 10:
                new_block = {
                    "content_block_type": "value_list",
                    "value_list": partial_unexpected_counts
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
                    "graph": [chart.to_json()]
                }

        elif result_key == "partial_unexpected_list":
            partial_unexpected_list = evr["result"]["partial_unexpected_list"]
            new_block = {
                "content_block_type": "value_list",
                "value_list": partial_unexpected_list
            }

        return new_block
