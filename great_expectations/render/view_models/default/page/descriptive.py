from collections import defaultdict

from .base import FullPageHtmlRenderer
from ..section import (
    DescriptiveEvrColumnSectionRenderer,
)


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
            sections.append(
                DescriptiveEvrColumnSectionRenderer().render(
                    evrs, group
                )
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
                column_evrs_dict["table"].append(evr)

        return column_evrs_dict
