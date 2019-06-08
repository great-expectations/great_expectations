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
        # This feels nice and tidy. We should probably use this pattern elsewhere, too.
        remaining_evrs, content_blocks = cls._render_header(evrs, column, [])
        remaining_evrs, content_blocks = cls._render_column_type(
            remaining_evrs, content_blocks)
        remaining_evrs, content_blocks = cls._render_values_set(
            remaining_evrs, content_blocks)
        remaining_evrs, content_blocks = cls._render_stats_table(
            remaining_evrs, content_blocks)
        remaining_evrs, content_blocks = cls._render_bullet_list(
            remaining_evrs, content_blocks)

        return {
            "section_name": column,
            "content_blocks": content_blocks
        }

    @classmethod
    def _render_header(cls, evrs, column_name, content_blocks):
        #!!! We should get the column name from an expectation, not another rando param.
        content_blocks.append({
            "content_block_type": "header",
            "header": column_name,
        })

        return evrs, content_blocks

    @classmethod
    def _render_column_type(cls, evrs, content_blocks):
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
            content_blocks.append(new_block)

            #!!! Before returning evrs, we should find and delete the `type_evr` that was used to render new_block.
            remaining_evrs = evrs
            return remaining_evrs, content_blocks

        else:
            return evrs, content_blocks

    @classmethod
    def _render_values_set(cls, evrs, content_blocks):
        set_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_values_to_be_in_set"
        )

        new_block = None

        if set_evr and "partial_unexpected_counts" in set_evr["result"]:
            result_key = "partial_unexpected_counts"
        elif set_evr and "partial_unexpected_list" in set_evr["result"]:
            result_key = "partial_unexpected_list"
        else:
            return evrs, content_blocks

        if len(set_evr["result"][result_key]) > 10:
            new_block = ValueListContentBlock.render(
                set_evr, result_key=result_key)
        else:
            new_block = GraphContentBlock.render(
                set_evr, result_key=result_key)

        if new_block is not None:
            content_blocks.append(new_block)

        return evrs, content_blocks

    @classmethod
    def _render_stats_table(cls, evrs, content_blocks):
        new_block = TableContentBlock.render(evrs)
        content_blocks.append(new_block)
        return evrs, content_blocks

    @classmethod
    def _render_bullet_list(cls, evrs, content_blocks):
        new_block = None
        for evr in evrs:
            #!!! This is a hack to cover up the fact that we're not yet pulling these EVRs out of the list.
            if evr["expectation_config"]["expectation_type"] not in [
                "expect_column_to_exist",
                "expect_column_values_to_be_of_type",
                "expect_column_values_to_be_in_set",
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
            content_blocks.append(new_block)
        return [], content_blocks


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
        # print(content)
        bullet_points = [Template(bullet_point["template"]).substitute(
            bullet_point["params"]) for bullet_point in content["bullet_list"]]
        # print(bullet_points)
        try:
            bullet_list = {
                "content_block_type": "bullet_list",
                "bullet_list": bullet_points,
            }
        except IndexError:
            bullet_list = {
                "content_block_type": "bullet_list",
                "bullet_list": [],
            }
        # bullet_list = BulletListContentBlock.render(expectations)
#         for expectation in expectations:
#             try:
#                 bullet_point = ExpectationBulletPointSnippetRenderer().render(expectation)
#                 assert bullet_point != None
#                 bullet_list["content"].append(bullet_point)

#             except Exception as e:
#                 bullet_list["content"].append("""
# <div class="alert alert-danger" role="alert">
#   Failed to render Expectation:<br/><pre>"""+json.dumps(expectation, indent=2)+"""</pre>
#   <p>"""+str(e)+"""
# </div>
#                 """)

        content_blocks.append(bullet_list)

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
