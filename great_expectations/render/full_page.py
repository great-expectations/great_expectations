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
            section = {
                "section_name" : group,
                "expectations" : []
            }
            
            for expectation in expectations:
                expectation_renderer = SingleExpectationRenderer(
                    expectation=expectation,
                )
                section["expectations"].append(expectation_renderer.render())

            sections.append(section)

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

class MockFullPageHtmlRenderer(FullPageHtmlRenderer):

    def _get_template(self):
        env = Environment(
            loader=PackageLoader('great_expectations', 'render/fixtures'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('mock.j2')


class FullPagePrescriptiveExpectationRenderer(FullPageHtmlRenderer):

    def _get_template(self):
        env = Environment(
            loader=PackageLoader('great_expectations', 'render/fixtures'),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('single_page_prescriptive.j2')
