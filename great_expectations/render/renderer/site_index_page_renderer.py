from collections import OrderedDict

from .renderer import Renderer
from great_expectations.render.types import (
    RenderedSectionContent,
    RenderedDocumentContent,
    RenderedHeaderContent, RenderedStringTemplateContent, RenderedTableContent, RenderedBulletListContent
)

from .call_to_action_renderer import CallToActionRenderer

# FIXME : This class needs to be rebuilt to accept SiteSectionIdentifiers as input.
# FIXME : This class needs tests.
class SiteIndexPageRenderer(Renderer):

    @classmethod
    def _generate_data_asset_table_section(cls, data_asset_name, link_lists_dict, link_list_keys_to_render):
        section_rows = []

        column_count = len(link_list_keys_to_render)
        profiling_links = link_lists_dict.get("profiling_links")
        validations_links = link_lists_dict.get("validations_links")
        expectations_links = link_lists_dict.get("expectations_links")
        
        cell_width_pct = 100.0/(column_count + 1)

        first_row = []
        rowspan = str(len(expectations_links)) if expectations_links else "1"
        
        data_asset_name = RenderedStringTemplateContent(**{
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
                            "classes": ["h6", "ge-index-page-generator-table-data-asset-name"],
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
        
        if "profiling_links" in link_list_keys_to_render:
            profiling_results_bullets = [
                RenderedStringTemplateContent(**{
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
                            },
                            "classes": ["ge-index-page-generator-table-profiling-links-item"]
                        }
                    }
                }) for link_dict in profiling_links
            ]
            profiling_results_bullet_list = RenderedBulletListContent(**{
                "content_block_type": "bullet_list",
                "bullet_list": profiling_results_bullets,
                "styling": {
                    "parent": {
                        "styles": {
                            "width": "{}%".format(cell_width_pct),
                        },
                        "attributes": {
                            "rowspan": rowspan
                        },
                    },
                    "body": {
                        "classes": ["ge-index-page-generator-table-profiling-links-list"]
                    }
                }
            })
            first_row.append(profiling_results_bullet_list)
            
        if "expectations_links" in link_list_keys_to_render:
            if len(expectations_links) > 0:
                expectation_suite_link_dict = expectations_links[0]
            else:
                expectation_suite_link_dict = {
                    "expectation_suite_name": "",
                    "filepath": ""
                }

            expectation_suite_name = expectation_suite_link_dict["expectation_suite_name"]

            expectation_suite_link = RenderedStringTemplateContent(**{
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
                        "classes": ["ge-index-page-generator-table-expectation-suite-link"]
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
            
            if "validations_links" in link_list_keys_to_render and "expectations_links" in link_list_keys_to_render:
                sorted_validations_links = [
                    link_dict for link_dict in sorted(validations_links, key=lambda x: x["run_id"], reverse=True)
                    if link_dict["expectation_suite_name"] == expectation_suite_name
                ]
                validation_link_bullets = [
                    RenderedStringTemplateContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "${validation_success} $link_text",
                            "params": {
                                "link_text": link_dict["run_id"],
                                "validation_success": ""
                            },
                            "tag": "a",
                            "styling": {
                                "attributes": {
                                    "href": link_dict["filepath"]
                                },
                                "params": {
                                    "validation_success": {
                                        "tag": "i",
                                        "classes": ["fas", "fa-check-circle", "text-success"] if link_dict[
                                            "validation_success"] else ["fas", "fa-times", "text-danger"]
                                    }
                                },
                                "classes": ["ge-index-page-generator-table-validation-links-item"]
                            }
                        },
                        "styling": {
                            "parent": {
                                "classes": ["hide-succeeded-validation-target"] if not link_dict[
                                            "validation_success"] else []
                            }
                        }
                    }) for link_dict in sorted_validations_links if
                    link_dict["expectation_suite_name"] == expectation_suite_name
                ]
                validation_link_bullet_list = RenderedBulletListContent(**{
                    "content_block_type": "bullet_list",
                    "bullet_list": validation_link_bullets,
                    "styling": {
                        "parent": {
                            "styles": {
                                "width": "{}%".format(cell_width_pct)
                            }
                        },
                        "body": {
                            "classes": ["ge-index-page-generator-table-validation-links-list"]
                        }
                    }
                })
                first_row.append(validation_link_bullet_list)

        if not expectations_links and "validations_links" in link_list_keys_to_render:
            sorted_validations_links = [
                link_dict for link_dict in sorted(validations_links, key=lambda x: x["run_id"], reverse=True)
            ]
            validation_link_bullets = [
                RenderedStringTemplateContent(**{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "${validation_success} $link_text",
                        "params": {
                            "link_text": link_dict["run_id"],
                            "validation_success": ""
                        },
                        "tag": "a",
                        "styling": {
                            "attributes": {
                                "href": link_dict["filepath"]
                            },
                            "params": {
                                "validation_success": {
                                    "tag": "i",
                                    "classes": ["fas", "fa-check-circle", "text-success"] if link_dict[
                                        "validation_success"] else ["fas", "fa-times", "text-danger"]
                                }
                            },
                            "classes": ["ge-index-page-generator-table-validation-links-item"]
                        }
                    },
                    "styling": {
                        "parent": {
                            "classes": ["hide-succeeded-validation-target"] if not link_dict[
                                "validation_success"] else []
                        }
                    }
                }) for link_dict in sorted_validations_links
            ]
            validation_link_bullet_list = RenderedBulletListContent(**{
                "content_block_type": "bullet_list",
                "bullet_list": validation_link_bullets,
                "styling": {
                    "parent": {
                        "styles": {
                            "width": "{}%".format(cell_width_pct)
                        }
                    },
                    "body": {
                        "classes": ["ge-index-page-generator-table-validation-links-list"]
                    }
                }
            })
            first_row.append(validation_link_bullet_list)
        
        section_rows.append(first_row)
        
        if "expectations_links" in link_list_keys_to_render and len(expectations_links) > 1:
            for expectation_suite_link_dict in expectations_links[1:]:
                expectation_suite_row = []
                expectation_suite_name = expectation_suite_link_dict["expectation_suite_name"]
    
                expectation_suite_link = RenderedStringTemplateContent(**{
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
                            "classes": ["ge-index-page-generator-table-expectation-suite-link"]
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
    
                if "validations_links" in link_list_keys_to_render:
                    sorted_validations_links = [
                        link_dict for link_dict in sorted(validations_links, key=lambda x: x["run_id"], reverse=True)
                        if link_dict["expectation_suite_name"] == expectation_suite_name
                    ]
                    validation_link_bullets = [
                        RenderedStringTemplateContent(**{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "${validation_success} $link_text",
                                "params": {
                                    "link_text": link_dict["run_id"],
                                    "validation_success": ""
                                },
                                "tag": "a",
                                "styling": {
                                    "attributes": {
                                        "href": link_dict["filepath"]
                                    },
                                    "params": {
                                        "validation_success": {
                                            "tag": "i",
                                            "classes": ["fas", "fa-check-circle",  "text-success"] if link_dict["validation_success"] else ["fas", "fa-times", "text-danger"]
                                        }
                                    },
                                    "classes": ["ge-index-page-generator-table-validation-links-item"]
                                }
                            },
                            "styling": {
                                "parent": {
                                    "classes": ["hide-succeeded-validation-target"] if link_dict[
                                        "validation_success"] else []
                                }
                            }
                        }) for link_dict in sorted_validations_links if
                        link_dict["expectation_suite_name"] == expectation_suite_name
                    ]
                    validation_link_bullet_list = RenderedBulletListContent(**{
                        "content_block_type": "bullet_list",
                        "bullet_list": validation_link_bullets,
                        "styling": {
                            "parent": {
                                "styles": {
                                    "width": "{}%".format(cell_width_pct)
                                }
                            },
                            "body": {
                                "classes": ["ge-index-page-generator-table-validation-links-list"]
                            }
                        }
                    })
                    expectation_suite_row.append(validation_link_bullet_list)
                    
                section_rows.append(expectation_suite_row)
            
        return section_rows
        
    @classmethod
    def render(cls, index_links_dict):

        sections = []
        cta_object = index_links_dict.pop("cta_object", None)

        for source, generators in index_links_dict.items():
            content_blocks = []

            # datasource header
            source_header_block = RenderedHeaderContent(**{
                "content_block_type": "header",
                "header": {
                    "template": "$title_prefix | $source",
                    "params": {
                        "source": source,
                        "title_prefix": "Datasource"
                    },
                    "styling": {
                        "params": {
                            "title_prefix": {
                                "tag": "strong"
                            }
                        }
                    },
                },
                "styling": {
                    "classes": ["col-12", "ge-index-page-datasource-title"],
                    "header": {
                        "classes": ["alert", "alert-secondary"]
                    }
                }
            })
            content_blocks.append(source_header_block)

            # generator header
            for generator, data_assets in generators.items():
                generator_header_block = RenderedHeaderContent(**{
                    "content_block_type": "header",
                    "header": "",
                    "subheader": {
                        "template": "$title_prefix | $generator",
                        "params": {
                            "generator": generator,
                            "title_prefix": "Data Asset Generator"
                        },
                        "styling": {
                            "params": {
                                "title_prefix": {
                                    "tag": "strong"
                                }
                            }
                        },
                    },
                    "styling": {
                        "classes": ["col-12", "ml-4", "ge-index-page-generator-title"],
                    }
                })
                content_blocks.append(generator_header_block)

                generator_table_rows = []
                generator_table_header_row = [RenderedStringTemplateContent(**{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Data Asset",
                        "params": {},
                        "styling": {
                            "classes": ["ge-index-page-generator-table-data-asset-header"],
                        }
                    }
                })]
                link_list_keys_to_render = []
                
                header_dict = OrderedDict([
                    ["profiling_links", "Profiling Results"],
                    ["expectations_links", "Expectation Suite"],
                    ["validations_links", "Validation Results"]
                ])
                
                for link_lists_key, header in header_dict.items():
                    for data_asset, link_lists in data_assets.items():
                        if header in generator_table_header_row:
                            continue
                        if link_lists.get(link_lists_key):
                            class_header_str = link_lists_key.replace("_", "-")
                            class_str = "ge-index-page-generator-table-{}-header".format(class_header_str)
                            header = RenderedStringTemplateContent(**{
                                "content_block_type": "string_template",
                                "string_template": {
                                    "template": header,
                                    "params": {},
                                    "styling": {
                                        "classes": [class_str],
                                    }
                                }
                            })
                            generator_table_header_row.append(header)
                            link_list_keys_to_render.append(link_lists_key)
                
                generator_table = RenderedTableContent(**{
                    "content_block_type": "table",
                    "header_row": generator_table_header_row,
                    "table": generator_table_rows,
                    "styling": {
                        "classes": ["col-12", "ge-index-page-generator-table-container", "pl-5", "pr-4"],
                        "styles": {
                            "margin-top": "10px"
                        },
                        "body": {
                            "classes": ["table", "table-sm", "ge-index-page-generator-table"]
                        }
                    }
                })
                # data_assets
                for data_asset, link_lists in data_assets.items():
                    generator_table_rows += cls._generate_data_asset_table_section(data_asset, link_lists, link_list_keys_to_render=link_list_keys_to_render)
                    
                content_blocks.append(generator_table)

            section = RenderedSectionContent(**{
                "section_name": source,
                "content_blocks": content_blocks
            })
            sections.append(section)

        index_page_document = RenderedDocumentContent(**{
            "renderer_type": "SiteIndexPageRenderer",
            "utm_medium": "index-page",
            "sections": sections
            })

        if cta_object:
            index_page_document.cta_footer = CallToActionRenderer.render(cta_object)

        return index_page_document
