import logging
logger = logging.getLogger(__name__)
import os
import json
from glob import glob
from collections import OrderedDict

from great_expectations.render.renderer import DescriptivePageRenderer, PrescriptivePageRenderer
from great_expectations.render.view import (
    DefaultJinjaPageView,
    DefaultJinjaIndexPageView,
)

from great_expectations.data_context.util import NormalizedDataAssetName, get_slack_callback, safe_mmkdir

class SiteBuilder():

    @classmethod
    def build(cls, data_context, site_config):
        index_links = []

        expectation_suite_writer = SiteBuilder.get_expectation_suite_writer(data_context, site_config)
        validation_writer = SiteBuilder.get_validation_writer(data_context, site_config)
        profiling_writer = SiteBuilder.get_profiling_writer(data_context, site_config)

        # profiling results

        for validation in SiteBuilder.get_profilings(data_context, site_config):

            run_id = validation['meta']['run_id']
            data_asset_name = validation['meta']['data_asset_name']
            expectation_suite_name = validation['meta']['expectation_suite_name']
            model = DescriptivePageRenderer.render(validation)
            profiling_writer.write(data_asset_name, expectation_suite_name, DefaultJinjaPageView.render(model))

            index_links.append({
                "data_asset_name": data_asset_name,
                "expectation_suite_name": expectation_suite_name,
                "run_id": run_id,
                "filepath": data_context._get_normalized_data_asset_name_filepath(
                        data_asset_name,
                        expectation_suite_name,
                        base_path = '',
                        file_extension=".html"
                        )
            })

        expectation_suite_filepaths = [y for x in os.walk(data_context.expectations_directory) for y in
                                       glob(os.path.join(x[0], '*.json'))]
        for expectation_suite in SiteBuilder.get_expectation_suites(data_context, site_config):
            data_asset_name = expectation_suite['data_asset_name']
            expectation_suite_name = expectation_suite['expectation_suite_name']
            model = PrescriptivePageRenderer.render(expectation_suite)
            expectation_suite_writer.write(data_asset_name, expectation_suite_name, DefaultJinjaPageView.render(model))

            index_links.append({
                "data_asset_name": data_asset_name,
                "expectation_suite_name": expectation_suite_name,
                "filepath": data_context._get_normalized_data_asset_name_filepath(
                    data_asset_name,
                    expectation_suite_name,
                    file_extension=".html"
                )
            })

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



        index_page_output = DefaultJinjaIndexPageView.render({
            "utm_medium": "index-page",
            "sections": sections
        })

        if site_config['site_store']['type'] == 's3':
            raise NotImplementedError("site_store.type = s3")
        elif site_config['site_store']['type'] == 'filesystem':
            index_page_path = os.path.join(data_context.get_absolute_path(site_config['site_store']['base_directory']), "index.html")
            with open(index_page_path, "w") as writer:
                writer.write(index_page_output)
        else:
            raise ValueError("Unrecognized site_store.type: " + site_config['site_store']['type'])


    @classmethod
    def get_validations(cls, data_context, site_config):
        validations = []

        if site_config['validations_store']['type'] == 's3':
            raise NotImplementedError("validations_store.type = s3")
        elif site_config['validations_store']['type'] == 'filesystem':
            base_directory = site_config['validations_store']['base_directory']
            validation_filepaths = [y for x in os.walk(data_context.get_absolute_path(base_directory)) for y in
                                    glob(os.path.join(x[0], '*.json'))]
            for validation_filepath in validation_filepaths:
                logger.debug("Loading validation from: %s" % validation_filepath)
                with open(validation_filepath, "r") as infile:
                    validations.append(json.load(infile))

        else:
            raise ValueError("Unrecognized validations_store.type: " + site_config['validations_store']['type'])

        return validations

    @classmethod
    def get_profilings(cls, data_context, site_config):
        validations = []

        if site_config['profiling_store']['type'] == 's3':
            raise NotImplementedError("profiling_store.type = s3")
        elif site_config['profiling_store']['type'] == 'filesystem':
            base_directory = site_config['profiling_store']['base_directory']
            validation_filepaths = [y for x in os.walk(data_context.get_absolute_path(base_directory)) for y in
                                    glob(os.path.join(x[0], '*.json'))]
            for validation_filepath in validation_filepaths:
                logger.debug("Loading validation from: %s" % validation_filepath)
                with open(validation_filepath, "r") as infile:
                    validations.append(json.load(infile))

        else:
            raise ValueError("Unrecognized profiling_store.type: " + site_config['profiling_store']['type'])

        return validations

    @classmethod
    def get_expectation_suites(cls, data_context, site_config):
        expectation_suites = []

        if site_config['expectations_store']['type'] == 's3':
            raise NotImplementedError("expectations_store.type = s3")
        elif site_config['expectations_store']['type'] == 'filesystem':
            base_directory = site_config['expectations_store']['base_directory']
            expectation_suite_filepaths = [y for x in os.walk(data_context.get_absolute_path(base_directory)) for y in
                                           glob(os.path.join(x[0], '*.json'))]
            for expectation_suite_filepath in expectation_suite_filepaths:
                with open(expectation_suite_filepath, "r") as infile:
                    expectation_suites.append(json.load(infile))

        else:
            raise ValueError("Unrecognized expectations_store.type: " + site_config['expectations_store']['type'])

        return expectation_suites


    @classmethod
    def get_expectation_suite_writer(cls, data_context, site_config):
        if site_config['expectations_store']['type'] == 'filesystem':
            return FilesystemValidationWriter(data_context, site_config)
        elif site_config['expectations_store']['type'] == 's3':
            raise NotImplementedError("expectations_store.type = s3")
        else:
            raise ValueError("Unrecognized expectations_store.type: " + site_config['expectations_store']['type'])

    @classmethod
    def get_validation_writer(cls, data_context, site_config):
        if site_config['validations_store']['type'] == 'filesystem':
            return FilesystemValidationWriter(data_context, site_config)
        elif site_config['validations_store']['type'] == 's3':
            raise NotImplementedError("validations_store.type = s3")
        else:
            raise ValueError("Unrecognized validations_store.type: " + site_config['validations_store']['type'])

    @classmethod
    def get_profiling_writer(cls, data_context, site_config):
        if site_config['profiling_store']['type'] == 'filesystem':
            return FilesystemProfilingWriter(data_context, site_config)
        elif site_config['profiling_store']['type'] == 's3':
            raise NotImplementedError("profiling_store.type = s3")
        else:
            raise ValueError("Unrecognized profiling_store.type: " + site_config['profiling_store']['type'])



class FilesystemExpectationSuiteWriter(object):

    def __init__(self, data_context, site_config):
        self.data_context = data_context
        self.site_config = site_config
        self.base_directory = data_context.get_absolute_path(site_config['expectations_store']['base_directory'])

    def write(self, data_asset_name, expectation_suite_name, data):
        out_filepath = self.data_context._get_normalized_data_asset_name_filepath(
            data_asset_name,
            expectation_suite_name,
            base_path=self.base_directory,
            file_extension=".html"
        )

        safe_mmkdir(os.path.dirname(out_filepath))

        with open(out_filepath, 'w') as writer:
            writer.write(data)


class FilesystemValidationWriter(object):

    def __init__(self, data_context, site_config):
        self.data_context = data_context
        self.site_config = site_config
        self.base_directory = data_context.get_absolute_path(site_config['validations_store']['base_directory'])

    def write(self, data_asset_name, expectation_suite_name, data):
        out_filepath = self.data_context._get_normalized_data_asset_name_filepath(
            data_asset_name,
            expectation_suite_name,
            base_path=self.base_directory,
            file_extension=".html"
        )

        safe_mmkdir(os.path.dirname(out_filepath))

        with open(out_filepath, 'w') as writer:
            writer.write(data)


class FilesystemProfilingWriter(object):

    def __init__(self, data_context, site_config):
        self.data_context = data_context
        self.site_config = site_config
        self.base_directory = data_context.get_absolute_path(site_config['profiling_store']['base_directory'])

    def write(self, data_asset_name, expectation_suite_name, data):

        out_filepath = self.data_context._get_normalized_data_asset_name_filepath(
            data_asset_name,
            expectation_suite_name,
            base_path=self.base_directory,
            file_extension=".html"
        )

        safe_mmkdir(os.path.dirname(out_filepath))

        with open(out_filepath, 'w') as writer:
            writer.write(data)

