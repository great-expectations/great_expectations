import os
import random
import json
from collections import defaultdict

from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape
)

from ...base import Renderer
from .section import (
    PrescriptiveExpectationColumnSectionRenderer,
    DescriptiveEvrColumnSectionRenderer,
)


class FullPageHtmlRenderer(Renderer):
    @classmethod
    def _validate_input(cls, expectations):
        # raise NotImplementedError
        #!!! Need to fix this
        return True

    @classmethod
    def _get_template(cls):
        env = Environment(
            loader=PackageLoader(
                'great_expectations',
                'render/view_models/default/fixtures/templates'
            ),
            autoescape=select_autoescape(['html', 'xml'])
        )
        return env.get_template('page.j2')

    @classmethod
    def render(cls, input):
        raise NotImplementedError


class PrescriptiveExpectationPageRenderer(FullPageHtmlRenderer):
    """Renders an Expectation Suite as a standalone HTML file."""
    @classmethod
    def _validate_input(cls, expectations_config):

        #!!! Add informative error messages here
        assert type(expectations_config) == dict

        # assert {expectations_config.keys()} == {""}
        return True

    @classmethod
    def render(cls, expectations_config):
        cls._validate_input(expectations_config)

        print(json.dumps(expectations_config, indent=2))
        expectations = expectations_config["expectations"]
        t = cls._get_template()

        grouped_expectations = cls._group_expectations_by_columns(
            expectations
        )

        sections = []
        for group, expectations in grouped_expectations.items():
            section_renderer = PrescriptiveExpectationColumnSectionRenderer(
                group, expectations)
            sections.append(
                section_renderer.render()
            )

        rendered_page = t.render(
            **{
                "sections": sections
            })

        return rendered_page

    @classmethod
    def _group_expectations_by_columns(cls, expectations_list):
        column_expectations_dict = defaultdict(list)

        for exp in expectations_list:
            if "column" in exp["kwargs"]:
                column_expectations_dict[exp["kwargs"]["column"]].append(exp)
            else:
                column_expectations_dict["NO_COLUMN"].append(exp)

        return column_expectations_dict


class DescriptiveEvrPageRenderer(FullPageHtmlRenderer):
    """Renders an EVR set as a standalone HTML file."""
    @classmethod
    def _validate_input(cls, evrs):
        assert type(evrs) == list

        return True

    @classmethod
    def render(cls, evrs):
        cls._validate_input(evrs)
        t = cls._get_template()

        grouped_evrs = cls._group_evrs_by_columns(evrs)

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

    @classmethod
    def _group_evrs_by_columns(cls, evrs_list):
        column_evrs_dict = defaultdict(list)

        for evr in evrs_list:
            exp = evr["expectation_config"]
            if "column" in exp["kwargs"]:
                column_evrs_dict[exp["kwargs"]["column"]].append(evr)
            else:
                column_evrs_dict["NO_COLUMN"].append(evr)

        return column_evrs_dict
