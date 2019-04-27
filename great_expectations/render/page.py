import os
import random
import json
from collections import defaultdict

from jinja2 import Template
from jinja2 import Environment, BaseLoader, PackageLoader, select_autoescape

from .base import Renderer
from .section import (
    PrescriptiveExpectationColumnSectionRenderer,
    DescriptiveEvrColumnSectionRenderer,
)

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
        return env.get_template('page.j2')

    def render(self):
        raise NotImplementedError

class PrescriptiveExpectationPageRenderer(FullPageHtmlRenderer):
    """Renders an Expectation Suite as a standalone HTML file."""

    def render(self):
        t = self._get_template()

        grouped_expectations = self._group_expectations_by_columns(self.expectations)

        sections = []
        for group, expectations in grouped_expectations.items():
            section_renderer = PrescriptiveExpectationColumnSectionRenderer(group, expectations)
            sections.append(
                section_renderer.render()
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

class DescriptiveEvrPageRenderer(FullPageHtmlRenderer):
    """Renders an EVR set as a standalone HTML file."""

    def __init__(self, evrs):
        self.evrs = evrs

    def render(self):
        t = self._get_template()

        grouped_evrs = self._group_evrs_by_columns(self.evrs)

        sections = []
        for group, evrs in grouped_evrs.items():
            section_renderer = DescriptiveEvrColumnSectionRenderer(group, evrs)
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