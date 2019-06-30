import json
from string import Template
from collections import defaultdict

from .renderer import Renderer
from .content_block import ValueListContentBlockRenderer
from .content_block import GraphContentBlockRenderer
from .content_block import TableContentBlockRenderer
from .content_block import PrescriptiveBulletListContentBlockRenderer

from .column_section_renderer import ColumnSectionRenderer


def get_hacky_column_type_from_evr_list(evr_list):
    for evr in evr_list:
        # print(evr)
        pass
    # return


def get_column_name_from_evr_list(evr_list):
    for evr in evr_list:
        # print(evr)
        pass
    pass


class FancyDescriptiveColumnSectionRenderer(ColumnSectionRenderer):

    @classmethod
    def render(cls, evrs, column=None):
        if column is None:
            column = cls._get_column_name(evrs)

        content_blocks = []
        cls._render_header(evrs, content_blocks)
        # cls._render_column_type(evrs, content_blocks)
        cls._render_overview_table(evrs, content_blocks)
        cls._render_stats_table(evrs, content_blocks)
        cls._render_values_set(evrs, content_blocks)

        # cls._render_statistics(evrs, content_blocks)
        # cls._render_histogram(evrs, content_blocks)
        # cls._render_common_values(evrs, content_blocks)
        # cls._render_extreme_values(evrs, content_blocks)

        # cls._render_frequency(evrs, content_blocks)
        # cls._render_composition(evrs, content_blocks)

        cls._render_expectation_types(evrs, content_blocks)
        # cls._render_unrecognized(evrs, content_blocks)

        # FIXME: shown here as an example of bullet list
        # content_blocks.append({
        #     "content_block_type": "bullet_list",
        #     "bullet_list": [
        #         {
        #             "template": "i1",
        #             "params": {}
        #         },
        #         {
        #             "template": "i2",
        #             "params": {}
        #         }
        #     ]
        # })

        return {
            "section_name": column,
            "content_blocks": content_blocks
        }

    @classmethod
    def _render_header(cls, evrs, content_blocks):

        # NOTE: This logic is brittle
        try:
            column_name = evrs[0]["expectation_config"]["kwargs"]["column"]
        except KeyError:
            column_name = "Table-level expectations"

        try:
            column_type_list = cls._find_evr_by_type(
                evrs, "expect_column_values_to_be_in_type_list"
            )["expectation_config"]["kwargs"]["type_list"]
            print(column_type_list)
            column_type = ", ".join(column_type_list)

        except TypeError:
            column_type = "None"

        # assert False

        content_blocks.append({
            "content_block_type": "header",
            "header": column_name,
            "sub_header": column_type,
            # {
            #     "template": column_type,
            # },
            "styling": {
                "classes": ["col-12"]
            }
        })

    # @classmethod
    # def _render_column_type(cls, evrs, content_blocks):
    #     new_block = None
    #     type_evr = cls._find_evr_by_type(
    #         evrs,
    #         "expect_column_values_to_be_of_type"
    #     )
    #     if type_evr:
    #         # Kinda weird to be pulling *descriptive* info out of expectation kwargs
    #         # Maybe at least check success?
    #         type_ = type_evr["expectation_config"]["kwargs"]["type_"]
    #         new_block = {
    #             "content_block_type": "text",
    #             "content": [type_]
    #         }
    #         content_blocks.append(new_block)

    @classmethod
    def _render_expectation_types(cls, evrs, content_blocks):
        # NOTE: The evr-fetching function is an exact dupe of the code other_section_renderer.DescriptiveOverviewSectionRenderer._render_expectation_types
        # TODO: Pull this code out into an EVR class

        type_counts = defaultdict(int)

        for evr in evrs:
            type_counts[evr["expectation_config"]["expectation_type"]] += 1

        bullet_list = sorted(type_counts.items(), key=lambda kv: -1*kv[1])

        bullet_list = [{
            "template": "$expectation_type $expectation_count",
            "params": {
                "expectation_type": tr[0],
                "expectation_count": tr[1],
            },
            "styling": {
                "classes": ["list-group-item", "d-flex", "justify-content-between", "align-items-center"],
                "params": {
                    "expectation_count": {
                        "classes": ["badge", "badge-secondary", "badge-pill"],
                    }
                }
            }
        } for tr in bullet_list]

        content_blocks.append({
            "content_block_type": "bullet_list",
            "header": "Expectation types",
            "bullet_list": bullet_list,
            "styling": {
                "classes": ["col-12"],
                "styles": {
                    "margin-top": "20px"
                },
                "header": {
                    "attributes": {
                        "data-toggle": "collapse",
                        "href": "#section-{{section_loop}}-content-block-{{content_block_loop}}",
                        "aria-expanded": "false",
                        "aria-controls": "collapseExample",
                    },
                },
                "body": {
                    "classes": ["list-group"],
                },
            },
        })

    @classmethod
    def _render_overview_table(cls, evrs, content_blocks):
        unique_n = cls._find_evr_by_type(
            evrs,
            "expect_column_unique_value_count_to_be_between"
        )
        unique_proportion = cls._find_evr_by_type(
            evrs,
            "expect_column_proportion_of_unique_values_to_be_between"
        )
        null_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_values_to_not_be_null"
        )
        evrs = [evr for evr in [
            unique_n, unique_proportion, null_evr] if evr is not None]

        if len(evrs) > 0:
            new_content_block = TableContentBlockRenderer.render(evrs)
            new_content_block["header"] = "Properties"
            new_content_block["styling"] = {
                "classes": ["col-4", ],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table"],
                    "styles": {
                        "width": "100%"
                    },
                }

            }
            content_blocks.append(new_content_block)

    @classmethod
    def _render_stats_table(cls, evrs, content_blocks):
        # min_evr = cls._find_evr_by_type(
        #     evrs,
        #     "expect_column_min_to_be_between"
        # )
        # mean_evr = cls._find_evr_by_type(
        #     evrs,
        #     "expect_column_mean_to_be_between"
        # )
        # max_evr = cls._find_evr_by_type(
        #     evrs,
        #     "expect_column_max_to_be_between"
        # )
        # evrs = [evr for evr in [min_evr, mean_evr, max_evr] if evr is not None]

        # if len(evrs) > 0:
        #     content_blocks.append(
        #         TableContentBlockRenderer.render(evrs)
        #     )

        table_rows = [
            ["Mean", "446", ],
            ["Minimum", "1", ],
            ["Maximum", "891", ],
            ["Zeros (%)", "0.0%", ],
        ]

        content_blocks.append({
            "content_block_type": "table",
            "header": "Statistics",
            "table_rows": table_rows,
            "styling": {
                "classes": ["col-4"],
                "styles": {
                    "margin-top": "20px"
                }
            },
        })

    @classmethod
    def _render_values_set(cls, evrs, content_blocks):
        # FIXME: Eugene, here's where the logic for using "cardinality" in form of which expectations are defined
        # should determine which blocks are generated
        # Relatedly, this will change to grab values_list and to use expect_column_distinct_values_to_be_in_set
        set_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_distinct_values_to_be_in_set"
        )

        if set_evr and "partial_unexpected_counts" in set_evr["result"]:
            result_key = "partial_unexpected_counts"
        elif set_evr and "partial_unexpected_list" in set_evr["result"]:
            result_key = "partial_unexpected_list"
        else:
            return

        if len(set_evr["result"][result_key]) < 10:
            content_blocks.append(
                ValueListContentBlockRenderer.render(
                    set_evr,
                    result_key=result_key
                )
            )
        else:
            content_blocks.append(
                GraphContentBlockRenderer.render(
                    set_evr,
                    result_key=result_key
                )
            )

    @classmethod
    def _render_unrecognized(cls, evrs, content_blocks):
        unrendered_blocks = []
        new_block = None
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] not in [
                "expect_column_to_exist",
                "expect_column_values_to_be_of_type",
                "expect_column_values_to_be_in_set",
                "expect_column_unique_value_count_to_be_between",
                "expect_column_proportion_of_unique_values_to_be_between",
                "expect_column_values_to_not_be_null",
                "expect_column_max_to_be_between",
                "expect_column_mean_to_be_between",
                "expect_column_min_to_be_between"
            ]:
                new_block = {
                    "content_block_type": "text",
                    "content": []
                }
                new_block["content"].append("""
    <div class="alert alert-primary" role="alert">
        Warning! Unrendered EVR:<br/>
    <pre>"""+json.dumps(evr, indent=2)+"""</pre>
    </div>
                """)

        if new_block is not None:
            unrendered_blocks.append(new_block)

        # print(unrendered_blocks)
        content_blocks += unrendered_blocks


class PrescriptiveColumnSectionRenderer(ColumnSectionRenderer):

    @classmethod
    def _render_header(cls, expectations, content_blocks):
        column = cls._get_column_name(expectations)

        content_blocks.append({
            "content_block_type": "header",
            "header": column
        })

        return expectations, content_blocks

    @classmethod
    def _render_bullet_list(cls, expectations, content_blocks):
        content = PrescriptiveBulletListContentBlockRenderer.render(
            expectations,
            include_column_name=False,
        )
        content_blocks.append(content)

        return [], content_blocks

    @classmethod
    def render(cls, expectations):
        column = cls._get_column_name(expectations)

        remaining_expectations, content_blocks = cls._render_header(
            expectations, [])
        # remaining_expectations, content_blocks = cls._render_column_type(
        # remaining_expectations, content_blocks)
        remaining_expectations, content_blocks = cls._render_bullet_list(
            remaining_expectations, content_blocks)

        return {
            "section_name": column,
            "content_blocks": content_blocks
        }
