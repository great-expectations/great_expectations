import logging
import traceback
import urllib
from collections import OrderedDict

from great_expectations.render.renderer.call_to_action_renderer import (
    CallToActionRenderer,
)
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.types import (
    RenderedBulletListContent,
    RenderedDocumentContent,
    RenderedHeaderContent,
    RenderedSectionContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
)

logger = logging.getLogger(__name__)


# FIXME : This class needs to be rebuilt to accept SiteSectionIdentifiers as input.
# FIXME : This class needs tests.
class SiteIndexPageRenderer(Renderer):
    @classmethod
    def _generate_links_table_rows(cls, index_links_dict, link_list_keys_to_render):
        section_rows = []

        column_count = len(link_list_keys_to_render)
        validations_links = index_links_dict.get("validations_links")
        expectations_links = index_links_dict.get("expectations_links")

        if column_count:
            cell_width_pct = 100.0 / column_count

        if "expectations_links" in link_list_keys_to_render:
            for expectation_suite_link_dict in expectations_links:
                expectation_suite_row = []
                expectation_suite_name = expectation_suite_link_dict[
                    "expectation_suite_name"
                ]

                expectation_suite_link = RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$link_text",
                            "params": {"link_text": expectation_suite_name},
                            "tag": "a",
                            "styling": {
                                "attributes": {
                                    "href": urllib.parse.quote(
                                        expectation_suite_link_dict["filepath"]
                                    )
                                },
                                "classes": [
                                    "ge-index-page-table-expectation-suite-link"
                                ],
                            },
                        },
                        "styling": {
                            "parent": {
                                "styles": {"width": "{}%".format(cell_width_pct),}
                            }
                        },
                    }
                )
                expectation_suite_row.append(expectation_suite_link)

                if "validations_links" in link_list_keys_to_render:
                    sorted_validations_links = [
                        link_dict
                        for link_dict in sorted(
                            validations_links, key=lambda x: x["run_id"], reverse=True
                        )
                        if link_dict["expectation_suite_name"] == expectation_suite_name
                    ]
                    validation_link_bullets = [
                        RenderedStringTemplateContent(
                            **{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "${validation_success} $link_text",
                                    "params": {
                                        "link_text": link_dict["run_id"],
                                        "validation_success": "",
                                    },
                                    "tag": "a",
                                    "styling": {
                                        "attributes": {
                                            "href": urllib.parse.quote(
                                                link_dict["filepath"]
                                            )
                                        },
                                        "params": {
                                            "validation_success": {
                                                "tag": "i",
                                                "classes": [
                                                    "fas",
                                                    "fa-check-circle",
                                                    "text-success",
                                                ]
                                                if link_dict["validation_success"]
                                                else ["fas", "fa-times", "text-danger"],
                                            }
                                        },
                                        "classes": [
                                            "ge-index-page-table-validation-links-item"
                                        ],
                                    },
                                },
                                "styling": {
                                    "parent": {
                                        "classes": ["hide-succeeded-validation-target"]
                                        if link_dict["validation_success"]
                                        else []
                                    }
                                },
                            }
                        )
                        for link_dict in sorted_validations_links
                        if link_dict["expectation_suite_name"] == expectation_suite_name
                    ]
                    validation_link_bullet_list = RenderedBulletListContent(
                        **{
                            "content_block_type": "bullet_list",
                            "bullet_list": validation_link_bullets,
                            "styling": {
                                "parent": {
                                    "styles": {"width": "{}%".format(cell_width_pct)}
                                },
                                "body": {
                                    "classes": [
                                        "ge-index-page-table-validation-links-list"
                                    ]
                                },
                            },
                        }
                    )
                    expectation_suite_row.append(validation_link_bullet_list)

                section_rows.append(expectation_suite_row)

        if not expectations_links and "validations_links" in link_list_keys_to_render:
            sorted_validations_links = [
                link_dict
                for link_dict in sorted(
                    validations_links, key=lambda x: x["run_id"], reverse=True
                )
            ]
            validation_link_bullets = [
                RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "${validation_success} $link_text",
                            "params": {
                                "link_text": link_dict["run_id"],
                                "validation_success": "",
                            },
                            "tag": "a",
                            "styling": {
                                "attributes": {
                                    "href": urllib.parse.quote(link_dict["filepath"])
                                },
                                "params": {
                                    "validation_success": {
                                        "tag": "i",
                                        "classes": [
                                            "fas",
                                            "fa-check-circle",
                                            "text-success",
                                        ]
                                        if link_dict["validation_success"]
                                        else ["fas", "fa-times", "text-danger"],
                                    }
                                },
                                "classes": [
                                    "ge-index-page-table-validation-links-item"
                                ],
                            },
                        },
                        "styling": {
                            "parent": {
                                "classes": ["hide-succeeded-validation-target"]
                                if link_dict["validation_success"]
                                else []
                            }
                        },
                    }
                )
                for link_dict in sorted_validations_links
            ]
            validation_link_bullet_list = RenderedBulletListContent(
                **{
                    "content_block_type": "bullet_list",
                    "bullet_list": validation_link_bullets,
                    "styling": {
                        "parent": {"styles": {"width": "{}%".format(cell_width_pct)}},
                        "body": {
                            "classes": ["ge-index-page-table-validation-links-list"]
                        },
                    },
                }
            )
            section_rows.append([validation_link_bullet_list])

        return section_rows

    @classmethod
    def render(cls, index_links_dict):
        sections = []
        cta_object = index_links_dict.pop("cta_object", None)

        try:
            content_blocks = []
            # site name header
            site_name_header_block = RenderedHeaderContent(
                **{
                    "content_block_type": "header",
                    "header": RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "$title_prefix | $site_name",
                                "params": {
                                    "site_name": index_links_dict.get("site_name"),
                                    "title_prefix": "Data Docs",
                                },
                                "styling": {
                                    "params": {"title_prefix": {"tag": "strong"}}
                                },
                            },
                        }
                    ),
                    "styling": {
                        "classes": ["col-12", "ge-index-page-site-name-title"],
                        "header": {"classes": ["alert", "alert-secondary"]},
                    },
                }
            )
            content_blocks.append(site_name_header_block)

            table_rows = []
            table_header_row = []
            link_list_keys_to_render = []

            header_dict = OrderedDict(
                [
                    ["expectations_links", "Expectation Suite"],
                    ["validations_links", "Validation Results (run_id)"],
                ]
            )

            for link_lists_key, header in header_dict.items():
                if index_links_dict.get(link_lists_key):
                    class_header_str = link_lists_key.replace("_", "-")
                    class_str = "ge-index-page-table-{}-header".format(class_header_str)
                    header = RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": header,
                                "params": {},
                                "styling": {"classes": [class_str],},
                            },
                        }
                    )
                    table_header_row.append(header)
                    link_list_keys_to_render.append(link_lists_key)

            generator_table = RenderedTableContent(
                **{
                    "content_block_type": "table",
                    "header_row": table_header_row,
                    "table": table_rows,
                    "styling": {
                        "classes": ["col-12", "ge-index-page-table-container"],
                        "styles": {"margin-top": "10px"},
                        "body": {
                            "classes": [
                                "table",
                                "table-sm",
                                "ge-index-page-generator-table",
                            ]
                        },
                    },
                }
            )

            table_rows += cls._generate_links_table_rows(
                index_links_dict, link_list_keys_to_render=link_list_keys_to_render
            )

            content_blocks.append(generator_table)

            if index_links_dict.get("profiling_links"):
                profiling_table_rows = []
                for profiling_link_dict in index_links_dict.get("profiling_links"):
                    profiling_table_rows.append(
                        [
                            RenderedStringTemplateContent(
                                **{
                                    "content_block_type": "string_template",
                                    "string_template": {
                                        "template": "$link_text",
                                        "params": {
                                            "link_text": profiling_link_dict[
                                                "expectation_suite_name"
                                            ]
                                            + "."
                                            + profiling_link_dict["batch_identifier"]
                                        },
                                        "tag": "a",
                                        "styling": {
                                            "attributes": {
                                                "href": urllib.parse.quote(
                                                    profiling_link_dict["filepath"]
                                                )
                                            },
                                            "classes": [
                                                "ge-index-page-table-expectation-suite-link"
                                            ],
                                        },
                                    },
                                }
                            )
                        ]
                    )
                content_blocks.append(
                    RenderedTableContent(
                        **{
                            "content_block_type": "table",
                            "header_row": ["Profiling Results"],
                            "table": profiling_table_rows,
                            "styling": {
                                "classes": ["col-12", "ge-index-page-table-container"],
                                "styles": {"margin-top": "10px"},
                                "body": {
                                    "classes": [
                                        "table",
                                        "table-sm",
                                        "ge-index-page-generator-table",
                                    ]
                                },
                            },
                        }
                    )
                )

            section = RenderedSectionContent(
                **{
                    "section_name": index_links_dict.get("site_name"),
                    "content_blocks": content_blocks,
                }
            )
            sections.append(section)

            index_page_document = RenderedDocumentContent(
                **{
                    "renderer_type": "SiteIndexPageRenderer",
                    "utm_medium": "index-page",
                    "sections": sections,
                }
            )

            if cta_object:
                index_page_document.cta_footer = CallToActionRenderer.render(cta_object)

            return index_page_document

        except Exception as e:
            exception_message = f"""\
An unexpected Exception occurred during data docs rendering.  Because of this error, certain parts of data docs will \
not be rendered properly and/or may not appear altogether.  Please use the trace, included in this message, to \
diagnose and repair the underlying issue.  Detailed information follows:
            """
            exception_traceback = traceback.format_exc()
            exception_message += (
                f'{type(e).__name__}: "{str(e)}".  Traceback: "{exception_traceback}".'
            )
            logger.error(exception_message, e, exc_info=True)
