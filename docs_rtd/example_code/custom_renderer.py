import altair as alt
import pandas as pd

from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.types import (
    RenderedBulletListContent,
    RenderedDocumentContent,
    RenderedGraphContent,
    RenderedHeaderContent,
    RenderedSectionContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    ValueListContent,
)


class CustomPageRenderer(Renderer):
    @classmethod
    def _get_header_content_block(cls, header="", subheader="", highlight=True):
        return RenderedHeaderContent(
            **{
                "content_block_type": "header",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": header,
                        },
                    }
                ),
                "subheader": subheader,
                "styling": {
                    "classes": ["col-12"],
                    "header": {
                        "classes": ["alert", "alert-secondary"] if highlight else []
                    },
                },
            }
        )

    @classmethod
    def _get_bullet_list_content_block(cls, header="", subheader="", col=12):
        return RenderedBulletListContent(
            **{
                "content_block_type": "bullet_list",
                "header": header,
                "subheader": subheader,
                "bullet_list": [
                    "Aenean porttitor turpis.",
                    "Curabitur ligula urna.",
                    cls._get_header_content_block(
                        header="nested header content block",
                        subheader="subheader",
                        highlight=False,
                    ),
                ],
                "styling": {
                    "classes": [f"col-{col}"],
                    "styles": {"margin-top": "20px"},
                },
            }
        )

    @classmethod
    def _get_table_content_block(cls, header="", subheader="", col=12):
        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header": header,
                "subheader": subheader,
                "table": [
                    ["", "column_1", "column_2"],
                    [
                        "row_1",
                        cls._get_bullet_list_content_block(
                            subheader="Nested Bullet List Content Block"
                        ),
                        "buffalo",
                    ],
                    ["row_2", "crayon", "derby"],
                ],
                "styling": {
                    "classes": [f"col-{col}", "table-responsive"],
                    "styles": {"margin-top": "20px"},
                    "body": {"classes": ["table", "table-sm"]},
                },
            }
        )

    @classmethod
    def _get_graph_content_block(cls, header="", subheader="", col=12):
        df = pd.DataFrame(
            {"value": [1, 2, 3, 4, 5, 6], "count": [123, 232, 543, 234, 332, 888]}
        )
        bars = (
            alt.Chart(df)
            .mark_bar(size=20)
            .encode(y="count:Q", x="value:O")
            .properties(height=200, width=200, autosize="fit")
        )
        chart = bars.to_json()

        return RenderedGraphContent(
            **{
                "content_block_type": "graph",
                "header": header,
                "subheader": subheader,
                "graph": chart,
                "styling": {
                    "classes": [f"col-{col}"],
                    "styles": {"margin-top": "20px"},
                },
            }
        )

    @classmethod
    def _get_tooltip_string_template_content_block(cls):
        return RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "This is a string template with tooltip, using a top-level custom tag.",
                    "tag": "code",
                    "tooltip": {"content": "This is the tooltip content."},
                },
                "styling": {
                    "classes": ["col-12"],
                    "styles": {"margin-top": "20px"},
                },
            }
        )

    @classmethod
    def _get_string_template_content_block(cls):
        return RenderedStringTemplateContent(
            **{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$icon This is a Font Awesome Icon, using a param-level custom tag\n$red_text\n$bold_serif",
                    "params": {
                        "icon": "",
                        "red_text": "And this is red text!",
                        "bold_serif": "And this is big, bold serif text using style attribute...",
                    },
                    "styling": {
                        "params": {
                            "icon": {
                                "classes": ["fas", "fa-check-circle", "text-success"],
                                "tag": "i",
                            },
                            "red_text": {"classes": ["text-danger"]},
                            "bold_serif": {
                                "styles": {
                                    "font-size": "22px",
                                    "font-weight": "bold",
                                    "font-family": "serif",
                                }
                            },
                        }
                    },
                },
                "styling": {
                    "classes": ["col-12"],
                    "styles": {"margin-top": "20px"},
                },
            }
        )

    @classmethod
    def _get_value_list_content_block(cls, header="", subheader="", col=12):
        return ValueListContent(
            **{
                "content_block_type": "value_list",
                "header": header,
                "subheader": subheader,
                "value_list": [
                    {
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {"value": value},
                            "styling": {
                                "default": {
                                    "classes": ["badge", "badge-info"],
                                },
                            },
                        },
                    }
                    for value in ["Andrew", "Elijah", "Matthew", "Cindy", "Pam"]
                ],
                "styling": {"classes": [f"col-{col}"]},
            }
        )

    @classmethod
    def render(cls, ge_dict=None):
        if ge_dict is None:
            ge_dict = {}

        return RenderedDocumentContent(
            **{
                "renderer_type": "CustomValidationResultsPageRenderer",
                "data_asset_name": "my_data_asset_name",
                "full_data_asset_identifier": "my_datasource/my_generator/my_data_asset_name",
                "page_title": "My Page Title",
                "sections": [
                    RenderedSectionContent(
                        **{
                            "section_name": "Header Content Block",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="Header Content Block", subheader="subheader"
                                )
                            ],
                        }
                    ),
                    RenderedSectionContent(
                        **{
                            "section_name": "Bullet List Content Block",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="Bullet List Content Block"
                                ),
                                cls._get_bullet_list_content_block(
                                    header="My Important List",
                                    subheader="Unremarkable Subheader",
                                ),
                            ],
                        }
                    ),
                    RenderedSectionContent(
                        **{
                            "section_name": "Table Content Block",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="Table Content Block"
                                ),
                                cls._get_table_content_block(
                                    header="My Big Data Table"
                                ),
                            ],
                        }
                    ),
                    RenderedSectionContent(
                        **{
                            "section_name": "Value List Content Block",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="Value List Content Block"
                                ),
                                cls._get_value_list_content_block(
                                    header="My Name Value List"
                                ),
                            ],
                        }
                    ),
                    RenderedSectionContent(
                        **{
                            "section_name": "Graph Content Block",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="Graph Content Block"
                                ),
                                cls._get_graph_content_block(
                                    header="My Big Data Graph"
                                ),
                            ],
                        }
                    ),
                    RenderedSectionContent(
                        **{
                            "section_name": "String Template Content Block With Icon",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="String Template Content Block With Icon"
                                ),
                                cls._get_string_template_content_block(),
                            ],
                        }
                    ),
                    RenderedSectionContent(
                        **{
                            "section_name": "String Template Content Block With Tooltip",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="String Template Content Block With Tooltip"
                                ),
                                cls._get_tooltip_string_template_content_block(),
                            ],
                        }
                    ),
                    RenderedSectionContent(
                        **{
                            "section_name": "Multiple Content Block Section",
                            "content_blocks": [
                                cls._get_header_content_block(
                                    header="Multiple Content Block Section"
                                ),
                                cls._get_graph_content_block(
                                    header="My col-4 Graph", col=4
                                ),
                                cls._get_graph_content_block(
                                    header="My col-4 Graph", col=4
                                ),
                                cls._get_graph_content_block(
                                    header="My col-4 Graph", col=4
                                ),
                                cls._get_table_content_block(
                                    header="My col-6 Table", col=6
                                ),
                                cls._get_bullet_list_content_block(
                                    header="My col-6 List", subheader="subheader", col=6
                                ),
                            ],
                        }
                    ),
                ],
            }
        )
