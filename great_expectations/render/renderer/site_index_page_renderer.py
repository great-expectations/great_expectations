from collections import OrderedDict
from .renderer import Renderer

class SiteIndexPageRenderer(Renderer):

    @classmethod
    def render(cls, index_links):



        index_links_dict = OrderedDict()

        for il in index_links:
            source, generator, asset = il["data_asset_name"].split('/')
            if not source in index_links_dict:
                index_links_dict[source] = OrderedDict()
            if not generator in index_links_dict[source]:
                index_links_dict[source][generator] = OrderedDict()
            if not asset in index_links_dict[source][generator]:
                index_links_dict[source][generator][asset] = {
                    'validation_links': [],
                    'expectation_suite_links': []
                }

            if "run_id" in il:
                index_links_dict[source][generator][asset]["validation_links"].append(
                    {
                        "full_data_asset_name": il["data_asset_name"],
                        "run_id": il["run_id"],
                        "expectation_suite_name": il["expectation_suite_name"],
                        "filepath": il["filepath"],
                        "source": source,
                        "generator": generator,
                        "asset": asset
                    }
                )
            else:
                index_links_dict[source][generator][asset]["expectation_suite_links"].append(
                    {
                        "full_data_asset_name": il["data_asset_name"],
                        "expectation_suite_name": il["expectation_suite_name"],
                        "filepath": il["filepath"],
                        "source": source,
                        "generator": generator,
                        "asset": asset
                    }
                )

        sections = []

        for source, generators in index_links_dict.items():
            content_blocks = []

            source_header_block = {
                "content_block_type": "header",
                "header": source,
                "styling": {
                    "classes": ["col-12"],
                    "header": {
                        "classes": ["alert", "alert-secondary"]
                    }
                }
            }
            content_blocks.append(source_header_block)

            for generator, data_assets in generators.items():
                generator_header_block = {
                    "content_block_type": "header",
                    "header": generator,
                    "styling": {
                        "classes": ["col-12", "ml-4"],
                    }
                }
                content_blocks.append(generator_header_block)

                horizontal_rule = {
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "",
                        "params": {},
                        "tag": "hr"
                    },
                    "styling": {
                        "classes": ["col-12"],
                    }
                }
                content_blocks.append(horizontal_rule)

                for data_asset, link_lists in data_assets.items():
                    data_asset_heading = {
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
                            "classes": ["col-sm-4", "col-xs-12", "pl-sm-5", "pl-xs-0"],
                            "styles": {
                                "margin-top": "10px",
                                "word-break": "break-all"
                            }
                        }
                    }
                    content_blocks.append(data_asset_heading)

                    expectation_suite_links = link_lists["expectation_suite_links"]
                    expectation_suite_link_table_rows = [
                        [{
                            "template": "$link_text",
                            "params": {
                                "link_text": link_dict["expectation_suite_name"]
                            },
                            "tag": "a",
                            "styling": {
                                "params": {
                                    "link_text": {
                                        "attributes": {
                                            "href": link_dict["filepath"]
                                        }
                                    }
                                }
                            }
                        }] for link_dict in expectation_suite_links
                    ]
                    expectation_suite_link_table = {
                        "content_block_type": "table",
                        "sub_header": "Expectation Suites",
                        "table_rows": expectation_suite_link_table_rows,
                        "styling": {
                            "classes": ["col-sm-4", "col-xs-12"],
                            "styles": {
                                "margin-top": "10px"
                            },
                            "body": {
                                "classes": ["table", "table-sm", ],
                            }
                        },
                    }
                    content_blocks.append(expectation_suite_link_table)

                    validation_links = link_lists["validation_links"]
                    validation_link_table_rows = [
                        [{
                            "template": "$link_text",
                            "params": {
                                "link_text": link_dict["run_id"] + "-" + link_dict["expectation_suite_name"]
                            },
                            "tag": "a",
                            "styling": {
                                "params": {
                                    "link_text": {
                                        "attributes": {
                                            "href": link_dict["filepath"]
                                        }
                                    }
                                }
                            }
                        }] for link_dict in validation_links
                    ]
                    validation_link_table = {
                        "content_block_type": "table",
                        "sub_header": "Batch Validations",
                        "table_rows": validation_link_table_rows,
                        "styling": {
                            "classes": ["col-sm-4", "col-xs-12"],
                            "styles": {
                                "margin-top": "10px"
                            },
                            "body": {
                                "classes": ["table", "table-sm", ],
                            }
                        },
                    }
                    content_blocks.append(validation_link_table)

            section = {
                "section_name": source,
                "content_blocks": content_blocks
            }
            sections.append(section)

        return sections
