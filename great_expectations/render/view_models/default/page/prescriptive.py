import os
import random
import json
from collections import defaultdict

from jinja2 import (
    Template, Environment, BaseLoader, PackageLoader, select_autoescape
)

from .base import FullPageHtmlRenderer
from ..section import (
    PrescriptiveExpectationColumnSectionRenderer,
)


class PrescriptiveExpectationPageRenderer(FullPageHtmlRenderer):
    """Renders an Expectation Suite as a standalone HTML file."""
    @classmethod
    def _validate_input(cls, expectations_config):

        #!!! Add informative error messages here
        assert type(expectations_config) == dict

        keys = expectations_config.keys()
        assert 'expectations' in keys
        assert type(expectations_config["expectations"]) == list

        return True

    @classmethod
    def render(cls, expectations_config):
        cls._validate_input(expectations_config)

        # print(json.dumps(expectations_config, indent=2))
        expectations = expectations_config["expectations"]
        t = cls._get_template()

        grouped_expectations = cls._group_expectations_by_columns(
            expectations
        )

        sections = []
        for group, expectations in grouped_expectations.items():
            # section_renderer = PrescriptiveExpectationColumnSectionRenderer(
            #     group, expectations)
            sections.append(
                PrescriptiveExpectationColumnSectionRenderer().render(expectations)
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
