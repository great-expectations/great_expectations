import json
import random

from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape
)

from .base import Renderer
from .snippet import (
    ExpectationBulletPointSnippetRenderer,
    EvrTableRowSnippetRenderer,
    render_parameter,
)

class SectionRenderer(Renderer):
    def __init__(self, expectations, inspectable):
        self.expectations = expectations

    def _validate_input(self, expectations):
        # raise NotImplementedError
        #!!! Need to fix this
        return True

    def _get_template(self):
        raise NotImplementedError

    def render(self):
        raise NotImplementedError

class PrescriptiveExpectationColumnSectionRenderer(SectionRenderer):
    """Generates a section's worth of prescriptive content blocks for a set of Expectations from the same column."""

    def __init__(self, column_name, expectations_list):
        self.column_name = column_name
        self.expectations_list = expectations_list

    def render(self, mode='json'):
        description = {
            "content_block_type" : "header",
            "content" : [self.column_name]
        }
        bullet_list = {
            "content_block_type" : "bullet_list",
            "content" : []
        }
        if random.random() > .5:
            graph = {
                "content_block_type" : "graph",
                "content" : []
            }
        else:
            graph = {}

        table = {
            "content_block_type" : "table",
            "content" : []
        }
        example_list = {
            "content_block_type" : "example_list",
            "content" : []
        }
        more_description = {
            "content_block_type" : "text",
            "content" : []
        }

        for expectation in self.expectations_list:
            try:
                expectation_renderer = ExpectationBulletPointSnippetRenderer(
                    expectation=expectation,
                )
                # print(expectation)
                bullet_point = expectation_renderer.render()
                assert bullet_point != None
                bullet_list["content"].append(bullet_point)
            except Exception as e:
                bullet_list["content"].append("""
<div class="alert alert-danger" role="alert">
  Failed to render Expectation:<br/><pre>"""+json.dumps(expectation, indent=2)+"""</pre>
  <p>"""+str(e)+"""
</div>
                """)

        section = {
            "section_name" : self.column_name,
            "content_blocks" : [
                graph,
                # graph2,
                description,
                table,
                bullet_list,
                example_list,
                more_description,
            ]
        }

        if mode == "json":
            return section
        
        elif mode == "html":
            env = Environment(
                loader=PackageLoader('great_expectations', 'render/fixtures/templates'),
                autoescape=select_autoescape(['html', 'xml'])
            )
            t = env.get_template('section.j2')

            return t.render(**{'section' : section})


class DescriptiveEvrColumnSectionRenderer(SectionRenderer):
    """Generates a section's worth of descriptive content blocks for a set of EVRs from the same column."""

    def __init__(self, column_name, evrs):
        #!!! We should get the column name from an expectation, not another rando param.
        self.column_name = column_name
        self.evrs = evrs

    def _find_evr_by_type(self, evrs, type_):
        for evr in evrs:
            if evr["expectation_config"]["expectation_type"] == type_:
                return evr
    
    def _render_header(self, evrs, content_blocks):
        #!!! We should get the column name from an expectation, not another rando param.
        content_blocks.append({
            "content_block_type" : "header",
            "content" : [self.column_name],
        })

        return evrs, content_blocks



    def _render_column_type(self, evrs, content_blocks):
        type_evr = self._find_evr_by_type(self.evrs, "expect_column_values_to_be_of_type")
        if type_evr:
            type_ = type_evr["expectation_config"]["kwargs"]["type_"]
            new_block = {
                "content_block_type" : "text",
                "content" : [type_]
            }
            content_blocks.append(new_block)

            #!!! Before returning evrs, we should find and delete the `type_evr` that was used to render new_block.
            remaining_evrs = evrs
            return remaining_evrs, content_blocks

        else:
            return evrs, content_blocks

    def _render_values_set(self, evrs, content_blocks):
        set_evr = self._find_evr_by_type(self.evrs, "expect_column_values_to_be_in_set")
        if set_evr and "partial_unexpected_counts" in set_evr["result"]:
            partial_unexpected_counts = set_evr["result"]["partial_unexpected_counts"]
            if len(partial_unexpected_counts) > 10 :
                new_block = {
                    "content_block_type" : "text",
                    "content" : [
                        "<b>Example values:</b><br/> " + ", ".join([
                            render_parameter(str(item["value"]), "s") for item in partial_unexpected_counts
                        ])
                    ]
                }
            else:
                new_block = {
                    "content_block_type" : "text",
                    "content" : [
                        "<b>GRAPH!!!:</b><br/> " + ", ".join([
                            render_parameter(str(item["value"]), "s") for item in partial_unexpected_counts
                        ])
                    ]
                }

            content_blocks.append(new_block)

        #!!! Before returning evrs, we should find and delete the `set_evr` that was used to render new_block.
        return evrs, content_blocks

    def _render_stats_table(self, evrs, content_blocks):
        remaining_evrs = []
        new_block = {
            "content_block_type" : "table",
            "content" : []
        }
        for evr in self.evrs:
            evr_renderer = EvrTableRowSnippetRenderer(evr=evr)
            table_rows = evr_renderer.render()
            if table_rows:
                new_block["content"] += table_rows
            else:
                remaining_evrs.append(evr)
        
        content_blocks.append(new_block)

        return remaining_evrs, content_blocks

    def _render_bullet_list(self, evrs, content_blocks):
        new_block = {
            "content_block_type" : "text",
            "content" : []
        }
        for evr in evrs:
            #!!! This is a hack to cover up the fact that we're not yet pulling these EVRs out of the list.
            if evr["expectation_config"]["expectation_type"] not in [
                "expect_column_to_exist",
                "expect_column_values_to_be_of_type",
                # "expect_column_values_to_be_in_set",
            ]:
                new_block["content"].append("""
    <div class="alert alert-primary" role="alert">
        Warning! Unrendered EVR:<br/>
    <pre>"""+json.dumps(evr, indent=2)+"""</pre>
    </div>
                """)

        content_blocks.append(new_block)
        return [], content_blocks
        

    def render(self, mode='json'):
        #!!! Someday we may add markdown and others
        assert mode in ['html', 'json']

        # This feels nice and tidy. We should probably use this pattern elsewhere, too.
        remaining_evrs, content_blocks = self._render_header(self.evrs, [])
        remaining_evrs, content_blocks = self._render_column_type(self.evrs, content_blocks)
        remaining_evrs, content_blocks = self._render_values_set(remaining_evrs, content_blocks)
        remaining_evrs, content_blocks = self._render_stats_table(remaining_evrs, content_blocks)
        remaining_evrs, content_blocks = self._render_bullet_list(remaining_evrs, content_blocks)

        section = {
            "section_name" : self.column_name,
            "content_blocks" : content_blocks
        }

        #!!! This code should probably be factored out. We'll use it for many a renderer...
        if mode == "json":
            return section
        
        elif mode == "html":
            env = Environment(
                loader=PackageLoader('great_expectations', 'render/fixtures/templates'),
                autoescape=select_autoescape(['html', 'xml'])
            )
            t = env.get_template('section.j2')

            return t.render(**{'section' : section})
