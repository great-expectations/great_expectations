from collections import OrderedDict

from .renderer import Renderer
from great_expectations.render.types import (
    RenderedComponentContent,
    RenderedSectionContent,
    RenderedDocumentContent
)

class SiteIndexPageRenderer(Renderer):

    @classmethod
    def _generate_data_asset_table_section(cls, data_asset_name, link_lists_dict):
        section_rows = []
        column_count = 1
        profiling_links = link_lists_dict["profiling_links"]
        if profiling_links: column_count += 1
        validation_links = link_lists_dict["validation_links"]
        if validation_links: column_count += 1
        expectation_suite_links = link_lists_dict["expectation_suite_links"]
        if expectation_suite_links: column_count += 1
        
        cell_width_pct = 100.0/column_count

        first_row = []
        rowspan = str(len(expectation_suite_links)) if expectation_suite_links else "1"
        
        data_asset_name = RenderedComponentContent(**{
            "content_block_type": "string_template",
            "string_template": {
                "template": "$data_asset",
                "params": {
                    "data_asset": data_asset_name
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
                },
                "parent": {
                    "styles": {
                        "width": "{}%".format(cell_width_pct)
                    },
                    "attributes": {
                        "rowspan": rowspan
                    }
                }
            }
        })
        first_row.append(data_asset_name)
        
        if profiling_links:
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
                }) for link_dict in profiling_links
            ]
            profiling_results_bullet_list = RenderedComponentContent(**{
                "content_block_type": "bullet_list",
                "bullet_list": profiling_results_bullets,
                "styling": {
                    "parent": {
                        "styles": {
                            "width": "{}%".format(cell_width_pct),
                        },
                        "attributes": {
                            "rowspan": rowspan
                        }
                    }
                }
            })
            first_row.append(profiling_results_bullet_list)
            
        if expectation_suite_links:
            expectation_suite_link_dict = expectation_suite_links[0]

            expectation_suite_name = expectation_suite_link_dict["expectation_suite_name"]

            expectation_suite_link = RenderedComponentContent(**{
                "content_block_type": "string_template",
                "string_template": {
                    "template": "$link_text",
                    "params": {
                        "link_text": expectation_suite_name
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
                            "width": "{}%".format(cell_width_pct),
                        }
                    }
                }
            })
            first_row.append(expectation_suite_link)
            
            if validation_links:
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
                    }) for link_dict in validation_links if
                    link_dict["expectation_suite_name"] == expectation_suite_name
                ]
                validation_link_bullet_list = RenderedComponentContent(**{
                    "content_block_type": "bullet_list",
                    "bullet_list": validation_link_bullets,
                    "styling": {
                        "parent": {
                            "styles": {
                                "width": "{}%".format(cell_width_pct)
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
                first_row.append(validation_link_bullet_list)

        if not expectation_suite_links and validation_links:
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
                }) for link_dict in validation_links
            ]
            validation_link_bullet_list = RenderedComponentContent(**{
                "content_block_type": "bullet_list",
                "bullet_list": validation_link_bullets,
                "styling": {
                    "parent": {
                        "styles": {
                            "width": "{}%".format(cell_width_pct)
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
            first_row.append(validation_link_bullet_list)
        
        section_rows.append(first_row)
        
        if len(expectation_suite_links) > 1:
            for expectation_suite_link_dict in expectation_suite_links[1:]:
                expectation_suite_row = []
                expectation_suite_name = expectation_suite_link_dict["expectation_suite_name"]
    
                expectation_suite_link = RenderedComponentContent(**{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "$link_text",
                        "params": {
                            "link_text": expectation_suite_name
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
                                "width": "{}%".format(cell_width_pct),
                            }
                        }
                    }
                })
                expectation_suite_row.append(expectation_suite_link)
    
                if validation_links:
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
                        }) for link_dict in validation_links if
                        link_dict["expectation_suite_name"] == expectation_suite_name
                    ]
                    validation_link_bullet_list = RenderedComponentContent(**{
                        "content_block_type": "bullet_list",
                        "bullet_list": validation_link_bullets,
                        "styling": {
                            "parent": {
                                "styles": {
                                    "width": "{}%".format(cell_width_pct)
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
                    expectation_suite_row.append(validation_link_bullet_list)
                    
                section_rows.append(expectation_suite_row)
            
        return section_rows
        
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

                generator_table_rows = []
                generator_table_header_row = ["Data Asset"]
                
                header_dict = OrderedDict([
                    ["profiling_links", "Profiling Results"],
                    ["expectation_suite_links", "Expectation Suite"],
                    ["validation_links", "Validation Results"]
                ])
                
                link_lists_example = list(data_assets.items())[0][1]
                
                for link_lists_key, header in header_dict.items():
                    if link_lists_example[link_lists_key]:
                        generator_table_header_row.append(header)
                
                generator_table = RenderedComponentContent(**{
                    "content_block_type": "table",
                    "header_row": generator_table_header_row,
                    "table": generator_table_rows,
                    "styling": {
                        "classes": ["col-12"],
                        "styles": {
                            "margin-top": "10px"
                        },
                        "body": {
                            "classes": ["table", "table-sm"]
                        }
                    }
                })
                # data_assets
                for data_asset, link_lists in data_assets.items():
                    generator_table_rows += cls._generate_data_asset_table_section(data_asset, link_lists)
                    
                content_blocks.append(generator_table)

            section = RenderedSectionContent(**{
                "section_name": source,
                "content_blocks": content_blocks
            })
            sections.append(section)

        return RenderedDocumentContent(**{
                "utm_medium": "index-page",
                "sections": sections
            })

