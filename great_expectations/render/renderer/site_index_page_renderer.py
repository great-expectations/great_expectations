from .renderer import Renderer
from great_expectations.render.types import (
    RenderedComponentContent,
    RenderedSectionContent,
    RenderedDocumentContent
)

class SiteIndexPageRenderer(Renderer):

    @classmethod
    def render(cls, index_links_dict):

        sections = []

        for source, generators in index_links_dict.items():
            content_blocks = []

            # datasource header
            source_header_block = RenderedComponentContent(**{
                "content_block_type": "header",
                "header": source,
                "styling": {
                    "classes": ["col-12"],
                    "header": {
                        "classes": ["alert", "alert-secondary"]
                    }
                }
            })
            content_blocks.append(source_header_block)

            # generator header
            for generator, data_assets in generators.items():
                generator_header_block = RenderedComponentContent(**{
                    "content_block_type": "header",
                    "header": generator,
                    "styling": {
                        "classes": ["col-12", "ml-4"],
                    }
                })
                content_blocks.append(generator_header_block)

                horizontal_rule = RenderedComponentContent(**{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "",
                        "params": {},
                        "tag": "hr"
                    },
                    "styling": {
                        "classes": ["col-12"],
                    }
                })
                content_blocks.append(horizontal_rule)

                # data_asset section
                for data_asset, link_lists in data_assets.items():
                    # data_asset header
                    data_asset_heading = RenderedComponentContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$data_asset",
                            "params": {
                                "data_asset": data_asset
                            },
                            "tag": "blockquote",
                            "styling": {
                                "params": {
                                    "data_asset": {
                                        "classes": ["blockquote"],
                                    }
                                }
                            }
                        },
                        "styling": {
                            "classes": ["col-sm-3", "col-xs-12", "pl-sm-5", "pl-xs-0"],
                            "styles": {
                                "margin-top": "10px",
                                "word-break": "break-all"
                            }
                        }
                    })
                    content_blocks.append(data_asset_heading)

                    # profiling_results links
                    profiling_results_links = link_lists["profiling_links"]
                    profiling_results_bullets = [
                        RenderedComponentContent(**{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "$link_text",
                                "params": {
                                    "link_text": link_dict["expectation_suite_name"] + "-ProfilingResults"
                                },
                                "tag": "a",
                                "styling": {
                                    "attributes": {
                                        "href": link_dict["filepath"]
                                    }
                                }
                            }
                        }) for link_dict in profiling_results_links
                    ]
                    profiling_results_bullet_list = RenderedComponentContent(**{
                        "content_block_type": "bullet_list",
                        "bullet_list": profiling_results_bullets
                    })
                    profiling_results_table = RenderedComponentContent(**{
                        "content_block_type": "table",
                        "subheader": "Profiling Results",
                        "table": [[profiling_results_bullet_list]],
                        "styling": {
                            "classes": ["col-sm-3", "col-xs-12"],
                            "styles": {
                                "margin-top": "10px"
                            },
                            "body": {
                                "classes": ["table", "table-sm", ],
                            }
                        },
                    })
                    content_blocks.append(profiling_results_table)
                    
                    # expectation_suite/validations table
                    expectation_suite_links = link_lists["expectation_suite_links"]
                    validation_links = link_lists["validation_links"]
                    expectation_suite_validation_table_rows = []
                    
                    for expectation_suite_link_dict in expectation_suite_links:
                        expectation_suite_name = expectation_suite_link_dict["expectation_suite_name"]
                        
                        table_row = [
                            RenderedComponentContent(**{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "$link_text",
                                    "params": {
                                        "link_text": expectation_suite_link_dict["expectation_suite_name"]
                                    },
                                    "tag": "a",
                                    "styling": {
                                        "attributes": {
                                            "href": expectation_suite_link_dict["filepath"]
                                        },
                                    }
                                },
                                "styling": {
                                    "parent": {
                                        "styles": {
                                            "width": "40%"
                                        }
                                    }
                                }
                            })
                        ]
                        validation_link_bullets = [
                            RenderedComponentContent(**{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": "$link_text",
                                    "params": {
                                        "link_text": link_dict["run_id"]
                                    },
                                    "tag": "a",
                                    "styling": {
                                        "attributes": {
                                            "href": link_dict["filepath"]
                                        }
                                    }
                                }
                            }) for link_dict in validation_links if link_dict["expectation_suite_name"] == expectation_suite_name
                        ]
                        validation_link_bullet_list = RenderedComponentContent(**{
                            "content_block_type": "bullet_list",
                            "bullet_list": validation_link_bullets,
                            "styling": {
                                "parent": {
                                    "styles": {
                                        "width": "60%"
                                    }
                                },
                                "body": {
                                    "styles": {
                                        "max-height": "15em",
                                        "overflow": "scroll"
                                    }
                                }
                            }
                        })
                        table_row.append(validation_link_bullet_list)
                        expectation_suite_validation_table_rows.append(table_row)
                        
                    expectation_suite_validation_table = RenderedComponentContent(**{
                        "content_block_type": "table",
                        "header_row": ["Expectation Suites", "Validation Results"],
                        "table": expectation_suite_validation_table_rows,
                        "styling": {
                            "classes": ["col-sm-6", "col-xs-12",],
                            "styles": {
                                "margin-top": "10px"
                            },
                            "body": {
                                "classes": ["table", "table-sm", ],
                            }
                        },
                    })
                    content_blocks.append(expectation_suite_validation_table)

            section = RenderedSectionContent(**{
                "section_name": source,
                "content_blocks": content_blocks
            })
            sections.append(section)

        return RenderedDocumentContent(**{
                "utm_medium": "index-page",
                "sections": sections
            })

