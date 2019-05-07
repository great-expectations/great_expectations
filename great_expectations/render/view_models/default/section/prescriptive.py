import json
import random

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


class PrescriptiveExpectationColumnSectionRenderer(SectionRenderer):
    """Generates a section's worth of prescriptive content blocks for a set of Expectations from the same column."""

    #!!! Refactor this class to use the cascade in DescriptiveEvrColumnSectionRenderer
    @classmethod
    def render(cls, expectations_list, mode='json'):
        description = {
            "content_block_type": "header",
            "content": ["FOOBAR"]
        }
        bullet_list = {
            "content_block_type": "bullet_list",
            "content": []
        }
        if random.random() > .5:
            graph = {
                "content_block_type": "graph",
                "content": []
            }
        else:
            graph = {}

        table = {
            "content_block_type": "table",
            "content": []
        }
        example_list = {
            "content_block_type": "example_list",
            "content": []
        }
        more_description = {
            "content_block_type": "text",
            "content": []
        }

        for expectation in expectations_list:
            try:
                bullet_point = ExpectationBulletPointSnippetRenderer().render(expectation)
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
            "section_name": "FOOBAR",
            "content_blocks": [
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
                loader=PackageLoader(
                    'great_expectations',
                    'render/view_models/default/fixtures/templates'
                ),
                autoescape=select_autoescape(['html', 'xml'])
            )
            t = env.get_template('section.j2')

            return t.render(**{'section': section})
