import os
import random
from collections import defaultdict

from jinja2 import Template
from jinja2 import Environment, BaseLoader, PackageLoader, select_autoescape

from .base import Renderer
from .single_expectation import SingleExpectationRenderer

#FullPageExpectationHtmlRenderer
#FullPageValidationResultHtmlRenderer

class FullPageHtmlRenderer(Renderer):
    def __init__(self, expectations, inspectable):
        self.expectations = expectations

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

    def _validate_input(self, expectations):
        # raise NotImplementedError
        #!!! Need to fix this
        return True

    def _get_template(self):
        raise NotImplementedError

    def _group_expectations_by_columns(self, expectations_list):
        column_expectations_dict = defaultdict(list)

        for exp in expectations_list:
            if "column" in exp["kwargs"]:
                column_expectations_dict[exp["kwargs"]["column"]].append(exp)
            else:
                column_expectations_dict["NO_COLUMN"].append(exp)

        return column_expectations_dict

    def _render_column_section_from_expectations(self, column_name, expectations_list):
        description = {
            "content_block_type" : "text",
            "content" : []
        }
        bullet_list = {
            "content_block_type" : "bullet_list",
            "content" : []
        }
        graph = {
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
                bullet_list["content"].append(expectation_renderer.render())
            except:
                bullet_list["content"].append("Broken!")

        section = {
            "section_name" : column_name,
            "content_blocks" : [
                graph,
                description,
                bullet_list,
                example_list,
                more_description,
            ]
        }

        return section

class MockFullPageHtmlRenderer(FullPageHtmlRenderer):

    def _get_template(self):
        env = Environment(
            loader=PackageLoader('great_expectations', 'render/fixtures/templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('mock.j2')


class FullPagePrescriptiveExpectationRenderer(FullPageHtmlRenderer):

    def _get_template(self):
        env = Environment(
            loader=PackageLoader('great_expectations', 'render/fixtures/templates'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('single_page_prescriptive.j2')
