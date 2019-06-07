### PORTED


import json
import random
import logging

from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape
)
import pandas as pd
import altair as alt

from .base import SectionRenderer
from ....snippets import (
    ExpectationBulletPointSnippetRenderer,
    EvrTableRowSnippetRenderer,
    # render_parameter,
    EvrContentBlockSnippetRenderer
)

logger = logging.getLogger(__name__)

class DescriptiveEvrColumnSectionRenderer(SectionRenderer):
    """Generates a section's worth of descriptive content blocks for a set of EVRs from the same column."""

    @classmethod
    def _find_evr_by_type(cls, evrs, type_):
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] == type_:
                return evr

    @classmethod
    def _render_header(cls, evrs, evr_group, content_blocks):
        #!!! We should get the column name from an expectation, not another rando param.
        content_blocks.append({
            "content_block_type": "header",
            "content": [evr_group],
        })

        return evrs, content_blocks

    @classmethod
    def _render_column_type(cls, evrs, content_blocks):
        type_evr = cls._find_evr_by_type(
            evrs,
            "expect_column_values_to_be_of_type"
        )
        if type_evr:
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
        
        # TODO: _render_values_set differs from the other content block generators in that it gets back a full
        # content block; I think that is a better pattern than the snippetRenderer pattern that gets back snippets
        # without the accompanying content block

        # Further, the template for the content block should be packaged separately so that
        # content block renderers can self-render and be includeed in the bigger templates
        if set_evr and "partial_unexpected_counts" in set_evr["result"]:
            new_block = EvrContentBlockSnippetRenderer().render(set_evr, "partial_unexpected_counts")
        elif set_evr and "partial_unexpected_list" in set_evr["result"]:
            new_block = EvrContentBlockSnippetRenderer().render(set_evr, "partial_unexpected_list")

        if new_block is not None:
            content_blocks.append(new_block)

        #!!! Before returning evrs, we should find and delete the `set_evr` that was used to render new_block.
        ## JPC: I'm not sure that's necessary
        return evrs, content_blocks

    @classmethod
    def _render_stats_table(cls, evrs, content_blocks):
        remaining_evrs = []
        new_block = {
            "content_block_type": "table",
            "content": []
        }
        for evr in evrs:
            evr_renderer = EvrTableRowSnippetRenderer(evr=evr)
            table_rows = evr_renderer.render()
            if table_rows:
                new_block["content"] += table_rows
            else:
                remaining_evrs.append(evr)

        content_blocks.append(new_block)

        return remaining_evrs, content_blocks

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

    @classmethod
    def _get_template(cls, template):
        recognized_templates = ['html', 'json', 'widget']
        env = Environment(
            loader=PackageLoader('great_expectations',
                                'render/view_models/default/fixtures/templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        if template not in recognized_templates:
            try:
                t = env.get_template(template)
            except:
                logger.warning(f"Unable to find template {template}. Registered templates are {recognized_templates}")
        elif template == 'html':
            t = env.get_template('sections.j2')
        elif template == 'widget':
            t = env.get_template('widget.j2')
        else:
            class NoOpTemplate(object):
                @classmethod
                def render(cls, render_object):
                    return render_object
            t = NoOpTemplate

        return t

    @classmethod
    def render(cls, evrs, section_name, section_type=None, template='json'):
        t = cls._get_template(template)

        # This feels nice and tidy. We should probably use this pattern elsewhere, too.
        remaining_evrs, content_blocks = cls._render_header(evrs, section_name, [])
        remaining_evrs, content_blocks = cls._render_column_type(
            evrs, content_blocks)
        remaining_evrs, content_blocks = cls._render_values_set(
            remaining_evrs, content_blocks)
        remaining_evrs, content_blocks = cls._render_stats_table(
            remaining_evrs, content_blocks)
        remaining_evrs, content_blocks = cls._render_bullet_list(
            remaining_evrs, content_blocks)

        section = {
            "section_name": section_name,
            "section_type": section_type,
            "content_blocks": content_blocks
        }
        
        render_obj = {
            "sections": [section]
        }
        if template == "widget":
            render_obj["nowrap"] = True

        return t.render(render_obj)