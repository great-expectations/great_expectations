import pandas as pd
import altair as alt

from .content_block import ContentBlockRenderer


class ValueListContentBlockRenderer(ContentBlockRenderer):
    """Render ValuesList Style Content Blocks

    Produces content blocks with type
    values_list and graph, based on the size of the value list requested
    """

    @classmethod
    def expect_column_values_to_be_in_set(cls, evr, result_key="partial_unexpected_counts"):
        new_block = None
        if result_key == "partial_unexpected_counts":
            partial_unexpected_counts = evr["result"]["partial_unexpected_counts"]
            new_block = RenderedComponentContent(**{
                "content_block_type": "value_list",
                "value_list": partial_unexpected_counts
            })

        elif result_key == "partial_unexpected_list":
            partial_unexpected_list = evr["result"]["partial_unexpected_list"]
            new_block = RenderedComponentContent(**{
                "content_block_type": "value_list",
                "value_list": partial_unexpected_list
            })

        return new_block
