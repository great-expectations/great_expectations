import os
import random
import json
from collections import defaultdict

from jinja2 import Template
from jinja2 import Environment, BaseLoader, PackageLoader, select_autoescape

from .base import Renderer
from .single_expectation import SingleExpectationRenderer
from .section import EvrColumnSectionRenderer

class FullPageHtmlRenderer(Renderer):
    def __init__(self, expectations, inspectable):
        self.expectations = expectations

    def _validate_input(self, expectations):
        # raise NotImplementedError
        #!!! Need to fix this
        return True

    def _get_template(self):
        env = Environment(
            loader=PackageLoader('great_expectations', 'render/fixtures/templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('single_page_prescriptive.j2')

    def render(self):
        raise NotImplementedError

class PrescriptiveExpectationPageRenderer(FullPageHtmlRenderer):
    """Renders an Expectation Suite as a standalone HTML file."""

    def render(self):
        t = self._get_template()

        grouped_expectations = self._group_expectations_by_columns(self.expectations)

        sections = []
        for group, expectations in grouped_expectations.items():
            sections.append(
                self._render_column_section_from_expectations(
                    group, expectations
                )
            )

        rendered_page = t.render(
            **{
            "sections": sections
        })

        return rendered_page

    def _group_expectations_by_columns(self, expectations_list):
        column_expectations_dict = defaultdict(list)

        for exp in expectations_list:
            if "column" in exp["kwargs"]:
                column_expectations_dict[exp["kwargs"]["column"]].append(exp)
            else:
                column_expectations_dict["NO_COLUMN"].append(exp)

        return column_expectations_dict

    #!!! Factor this out into an appropriate renderer in section.py
    def _render_column_section_from_expectations(self, column_name, expectations_list):
        description = {
            "content_block_type" : "text",
            "content" : []
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

        graph2 = {
            "content_block_type" : "graph",
            "content" : []
        }
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

        for expectation in expectations_list:
            try:
                expectation_renderer = SingleExpectationRenderer(
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
            "section_name" : column_name,
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

        return section

class DescriptiveEvrPageRenderer(FullPageHtmlRenderer):
    """Renders an EVR set as a standalone HTML file."""

    def __init__(self, evrs):
        self.evrs = evrs

    def _get_template(self):
        env = Environment(
            loader=PackageLoader('great_expectations', 'render/fixtures/templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('single_page_descriptive.j2')

    def render(self):
        t = self._get_template()

        grouped_evrs = self._group_evrs_by_columns(self.evrs)

        sections = []
        for group, evrs in grouped_evrs.items():
            section_renderer = EvrColumnSectionRenderer(group, evrs)
            sections.append(
                section_renderer.render()
            )

        rendered_page = t.render(
            **{
            "sections": sections
        })

        return rendered_page


    def _group_evrs_by_columns(self, evrs_list):
        column_evrs_dict = defaultdict(list)

        for evr in evrs_list:
            exp = evr["expectation_config"]
            if "column" in exp["kwargs"]:
                column_evrs_dict[exp["kwargs"]["column"]].append(evr)
            else:
                column_evrs_dict["NO_COLUMN"].append(evr)

        return column_evrs_dict