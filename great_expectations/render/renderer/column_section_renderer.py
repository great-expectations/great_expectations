import json
from string import Template

from .renderer import Renderer
from .content_block import ValueListContentBlock
from .content_block import GraphContentBlock
from .content_block import TableContentBlock
from .content_block import BulletListContentBlock


class ColumnSectionRenderer(Renderer):
    @classmethod
    def _get_column_name(cls, ge_object):
        # This is broken out for ease of locating future validation here
        if isinstance(ge_object, list):
            candidate_object = ge_object[0]
        else:
            candidate_object = ge_object
        try:
            if "kwargs" in candidate_object:
                # This is an expectation (prescriptive)
                return candidate_object["kwargs"]["column"]
            elif "expectation_config" in candidate_object:
                # This is a validation (descriptive)
                return candidate_object["expectation_config"]["kwargs"]["column"]
            else:
                raise ValueError(
                    "Provide a column section renderer an expectation, list of expectations, evr, or list of evrs.")
        except KeyError:
            return None


class DescriptiveColumnSectionRenderer(ColumnSectionRenderer):

    @classmethod
    def render(cls, evrs, column=None):
        if column is None:
            column = cls._get_column_name(evrs)

        content_blocks = {}
        cls._render_header(evrs, column, content_blocks)
        cls._render_column_type(evrs, content_blocks)
        cls._render_overview_table(evrs, content_blocks)
        cls._render_stats_table(evrs, content_blocks)
        cls._render_values_set(evrs, content_blocks)
        cls._render_unrecognized(evrs, content_blocks)


        # FIXME: shown here as an example of bullet list
        content_blocks["summary_list"] = {
            "content_block_type": "bullet_list",
            "bullet_list": [
                {
                    "template": "i1",
                    "params": {}
                },
                {
                    "template": "i2",
                    "params": {}
                }
            ]
        }


        return {
            "section_name": column,
            "content_blocks": content_blocks
        }

    @classmethod
    def _render_header(cls, evrs, column_name, content_blocks):
        content_blocks["header"] = {
            "content_block_type": "header",
            "header": column_name,
        }

    @classmethod
    def _render_column_type(cls, evrs, content_blocks):
        new_block = None
        type_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_values_to_be_of_type"
        )
        if type_evr:
            # Kinda weird to be pulling *descriptive* info out of expectation kwargs
            # Maybe at least check success?
            type_ = type_evr["expectation_config"]["kwargs"]["type_"]
            new_block = {
                "content_block_type": "text",
                "content": [type_]
            }
            content_blocks["column_type"] = new_block

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
        evrs = [evr for evr in [unique_n, unique_proportion, null_evr] if evr is not None]
        if len(evrs) > 0:
            content_blocks["overview_table"] = TableContentBlock.render(evrs)

    @classmethod
    def _render_stats_table(cls, evrs, content_blocks):
        min_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_min_to_be_between"
        )
        mean_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_mean_to_be_between"
        )
        max_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_max_to_be_between"
        )
        evrs = [evr for evr in [min_evr, mean_evr, max_evr] if evr is not None]
        if len(evrs) > 0:
            content_blocks["stats_table"] = TableContentBlock.render(evrs)

    @classmethod
    def _render_values_set(cls, evrs, content_blocks):
        # FIXME: Eugene, here's where the logic for using "cardinality" in form of which expectations are defined
        # should determine which blocks are generated
        # Relatedly, this will change to grab values_list and to use expect_column_distinct_values_to_be_in_set
        set_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_values_to_be_in_set"
        )

        if set_evr and "partial_unexpected_counts" in set_evr["result"]:
            result_key = "partial_unexpected_counts"
        elif set_evr and "partial_unexpected_list" in set_evr["result"]:
            result_key = "partial_unexpected_list"
        else:
            return

        if len(set_evr["result"][result_key]) > 10:
            content_blocks["value_list"] = ValueListContentBlock.render(
                set_evr, result_key=result_key)
        else:
            content_blocks["value_graph"] = GraphContentBlock.render(
                set_evr, result_key=result_key)

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
        content_blocks["other_blocks"] = unrendered_blocks


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
        content = BulletListContentBlock.render(expectations)
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
