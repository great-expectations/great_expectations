import logging

from marshmallow import ValidationError
from six import string_types

import pypandoc

from great_expectations.core import DataAssetIdentifier, dataAssetIdentifierSchema
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.render.util import num_to_str

from .renderer import Renderer
from ..types import (
    RenderedDocumentContent,
    RenderedSectionContent,
    RenderedHeaderContent, RenderedTableContent, TextContent, RenderedStringTemplateContent)
from collections import OrderedDict

logger = logging.getLogger(__name__)


class ValidationResultsPageRenderer(Renderer):

    def __init__(self, column_section_renderer=None):
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ValidationResultsColumnSectionRenderer"
            }
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_config={},
            config_defaults={
                "module_name": column_section_renderer.get(
                    "module_name", "great_expectations.render.renderer.column_section_renderer")
            }
        )

    def render(self, validation_results):
        run_id = validation_results.meta['run_id']
        full_data_asset_identifier = validation_results.meta['data_asset_name'] or "//"
        if not isinstance(full_data_asset_identifier, DataAssetIdentifier):
            try:
                # This will cause a validation error if the data asset name is not properly formatted
                full_data_asset_identifier = dataAssetIdentifierSchema.load(full_data_asset_identifier).data
            except ValidationError:
                if isinstance(full_data_asset_identifier, string_types):
                    logger.warning("Rendering validation results using incomplete data_asset_identifier.")
                else:
                    raise
        if isinstance(full_data_asset_identifier, string_types):
            short_data_asset_name = full_data_asset_identifier
        else:
            short_data_asset_name = full_data_asset_identifier.generator_asset
        expectation_suite_name = validation_results.meta['expectation_suite_name']
    
        # Group EVRs by column
        columns = {}
        for evr in validation_results.results:
            if "column" in evr.expectation_config.kwargs:
                column = evr.expectation_config.kwargs["column"]
            else:
                column = "Table-Level Expectations"
        
            if column not in columns:
                columns[column] = []
            columns[column].append(evr)
    
        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
    
        overview_content_blocks = [
            self._render_validation_header(),
            self._render_validation_info(validation_results=validation_results),
            self._render_validation_statistics(validation_results=validation_results),
        ]

        if validation_results["meta"].get("batch_id"):
            overview_content_blocks.insert(
                2,
                self._render_nested_table_from_dict(
                    input_dict=validation_results["meta"].get("batch_id"),
                    header="Batch ID"
                )
            )

        if validation_results["meta"].get("batch_kwargs"):
            overview_content_blocks.insert(
                3,
                self._render_nested_table_from_dict(
                    input_dict=validation_results["meta"].get("batch_kwargs"),
                    header="Batch Kwargs"
                )
            )

        if "data_asset_name" in validation_results.meta and validation_results.meta["data_asset_name"]:
            data_asset_name = short_data_asset_name
        else:
            data_asset_name = None
    
        sections = [
            RenderedSectionContent(**{
                "section_name": "Overview",
                "content_blocks": overview_content_blocks
            })
        ]

        if "Table-Level Expectations" in columns:
            sections += [
                self._column_section_renderer.render(
                    validation_results=columns["Table-Level Expectations"]
                )
            ]

        sections += [
            self._column_section_renderer.render(
                validation_results=columns[column],
            ) for column in ordered_columns
        ]
    
        return RenderedDocumentContent(**{
            "renderer_type": "ValidationResultsPageRenderer",
            "data_asset_name": data_asset_name,
            "full_data_asset_identifier": full_data_asset_identifier,
            "page_title": run_id + "-" + expectation_suite_name + "-ValidationResults",
            "sections": sections,
            "utm_medium": "validation-results-page",
        })
    
    @classmethod
    def _render_validation_header(cls):
        return RenderedHeaderContent(**{
            "content_block_type": "header",
            "header": "Validation Overview",
            "styling": {
                "classes": ["col-12"],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        })
    
    @classmethod
    def _render_validation_info(cls, validation_results):
        run_id = validation_results.meta['run_id']
        full_data_asset_identifier = validation_results.meta['data_asset_name'] or ""
        if not isinstance(full_data_asset_identifier, DataAssetIdentifier):
            try:
                # This will cause a validation error if the data asset name is not properly formatted
                full_data_asset_identifier = dataAssetIdentifierSchema.load(full_data_asset_identifier).data
            except ValidationError:
                if isinstance(full_data_asset_identifier, string_types):
                    logger.warning("Rendering validation results using incomplete data_asset_identifier.")
                else:
                    raise
        expectation_suite_name = validation_results.meta['expectation_suite_name']
        ge_version = validation_results.meta["great_expectations.__version__"]
        success = validation_results.success
        if success:
            success = '<i class="fas fa-check-circle text-success" aria-hidden="true"></i> Succeeded'
        else:
            success = '<i class="fas fa-times text-danger" aria-hidden="true"></i> Failed'

        return RenderedTableContent(**{
            "content_block_type": "table",
            "header": "Info",
            "table": [
                ["Full Data Asset Identifier", full_data_asset_identifier.to_path()],
                ["Expectation Suite Name", expectation_suite_name],
                ["Great Expectations Version", ge_version],
                ["Run ID", run_id],
                ["Validation Status", success]
            ],
            "styling": {
                "classes": ["col-12", "table-responsive"],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })
    
    @classmethod
    def _render_nested_table_from_dict(cls, input_dict, header=None, sub_table=False):
        table_rows = []

        for kwarg, value in input_dict.items():
            if not isinstance(value, (dict, OrderedDict)):
                table_row = [
                    RenderedStringTemplateContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {
                                "value": str(kwarg)
                            },
                            "styling": {
                                "default": {
                                    "styles": {
                                        "word-break": "break-all"
                                    }
                                },
                            }
                        },
                        "styling": {
                            "parent": {
                                "classes": ["pr-3"],
                            }
                        }
                    }),
                    RenderedStringTemplateContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {
                                "value": str(value)
                            },
                            "styling": {
                                "default": {
                                    "styles": {
                                        "word-break": "break-all"
                                    }
                                },
                            }
                        },
                        "styling": {
                            "parent": {
                                "classes": [],
                            }
                        }
                    })
                ]
            else:
                table_row = [
                    RenderedStringTemplateContent(**{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "$value",
                            "params": {
                                "value": str(kwarg)
                            },
                            "styling": {
                                "default": {
                                    "styles": {
                                        "word-break": "break-all"
                                    }
                                },
                            }
                        },
                        "styling": {
                            "parent": {
                                "classes": ["pr-3"],
                            }
                        }
                    }),
                    cls._render_nested_table_from_dict(value, sub_table=True)
                ]
            table_rows.append(table_row)

        table_rows.sort(key=lambda row: row[0].string_template["params"]["value"])

        if sub_table:
            return RenderedTableContent(**{
                "content_block_type": "table",
                "table": table_rows,
                "styling": {
                    "classes": ["col-12", "table-responsive"],
                    "body": {
                        "classes": ["table", "table-sm", "m-0"]
                    },
                    "parent": {
                        "classes": ["pt-0", "pl-0", "border-top-0"]
                    }
                },
            })
        else:
            return RenderedTableContent(**{
                "content_block_type": "table",
                "header": header,
                "table": table_rows,
                "styling": {
                    "classes": ["col-12", "table-responsive"],
                    "styles": {
                        "margin-top": "20px"
                    },
                    "body": {
                        "classes": ["table", "table-sm"]
                    }
                },
            })

    @classmethod
    def _render_validation_statistics(cls, validation_results):
        statistics = validation_results["statistics"]
        statistics_dict = OrderedDict([
            ("evaluated_expectations", "Evaluated Expectations"),
            ("successful_expectations", "Successful Expectations"),
            ("unsuccessful_expectations", "Unsuccessful Expectations"),
            ("success_percent", "Success Percent")
        ])
        table_rows = []
        for key, value in statistics_dict.items():
            if statistics.get(key) is not None:
                if key == "success_percent":
                    # table_rows.append([value, "{0:.2f}%".format(statistics[key])])
                    table_rows.append([value, num_to_str(statistics[key], precision=4) + "%"])
                else:
                    table_rows.append([value, statistics[key]])
        
        return RenderedTableContent(**{
            "content_block_type": "table",
            "header": "Statistics",
            "table": table_rows,
            "styling": {
                "classes": ["col-6", "table-responsive"],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })


class ExpectationSuitePageRenderer(Renderer):

    def __init__(self, column_section_renderer=None):
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ExpectationSuiteColumnSectionRenderer"
            }
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_config={},
            config_defaults={
                "module_name": column_section_renderer.get(
                    "module_name", "great_expectations.render.renderer.column_section_renderer")
            }
        )

    def render(self, expectations):
        columns, ordered_columns = self._group_and_order_expectations_by_column(expectations)
        full_data_asset_identifier = expectations.data_asset_name
        if not isinstance(full_data_asset_identifier, DataAssetIdentifier):
            try:
                # This will cause a validation error if the data asset name is not properly formatted
                full_data_asset_identifier = dataAssetIdentifierSchema.load(full_data_asset_identifier).data
            except ValidationError:
                if isinstance(full_data_asset_identifier, string_types):
                    logger.warning("Rendering validation results using incomplete data_asset_identifier.")
                else:
                    raise
        expectation_suite_name = expectations.expectation_suite_name

        overview_content_blocks = [
            self._render_asset_header(expectations),
            self._render_asset_info(expectations)
        ]
        
        table_level_expectations_content_block = self._render_table_level_expectations(columns)
        if table_level_expectations_content_block is not None:
            overview_content_blocks.append(table_level_expectations_content_block)
        
        asset_notes_content_block = self._render_asset_notes(expectations)
        if asset_notes_content_block is not None:
            overview_content_blocks.append(asset_notes_content_block)
        
        sections = [
            RenderedSectionContent(**{
                "section_name": "Overview",
                "content_blocks": overview_content_blocks,
            })
        ]
        
        sections += [
            self._column_section_renderer.render(expectations=columns[column]) for column in ordered_columns if column != "_nocolumn"
        ]
        return RenderedDocumentContent(**{
            # "data_asset_name": short_data_asset_name,
            "renderer_type": "ExpectationSuitePageRenderer",
            "full_data_asset_identifier": full_data_asset_identifier,
            "page_title": expectation_suite_name,
            "utm_medium": "expectation-suite-page",
            "sections": sections
        })

    def _render_table_level_expectations(self, columns):
        table_level_expectations = columns.get("_nocolumn")
        if not table_level_expectations:
            return None
        else:
            expectation_bullet_list = self._column_section_renderer.render(
                expectations=table_level_expectations).content_blocks[1]
            expectation_bullet_list.header = "Table-Level Expectations"
            return expectation_bullet_list
        
    @classmethod
    def _render_asset_header(cls, expectations):
        return RenderedHeaderContent(**{
            "content_block_type": "header",
            "header": "Expectation Suite Overview",
            "styling": {
                "classes": ["col-12"],
                "header": {
                    "classes": ["alert", "alert-secondary"]
                }
            }
        })
      
    @classmethod
    def _render_asset_info(cls, expectations):
        full_data_asset_identifier = expectations.data_asset_name
        if not isinstance(full_data_asset_identifier, DataAssetIdentifier):
            try:
                # This will cause a validation error if the data asset name is not properly formatted
                full_data_asset_identifier = dataAssetIdentifierSchema.load(full_data_asset_identifier).data
            except ValidationError:
                if isinstance(full_data_asset_identifier, string_types):
                    logger.warning("Rendering validation results using incomplete data_asset_identifier.")
                else:
                    raise
        data_asset_type = expectations.data_asset_type
        expectation_suite_name = expectations.expectation_suite_name
        ge_version = expectations.meta["great_expectations.__version__"]

        return RenderedTableContent(**{
            "content_block_type": "table",
            "header": "Info",
            "table": [
                ["Full Data Asset Identifier", full_data_asset_identifier.to_path()],
                ["Data Asset Type", data_asset_type],
                ["Expectation Suite Name", expectation_suite_name],
                ["Great Expectations Version", ge_version]
            ],
            "styling": {
                "classes": ["col-12", "table-responsive"],
                "styles": {
                    "margin-top": "20px",
                    "margin-bottom": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })

    @classmethod
    def _render_asset_notes(cls, expectations):
        
        content = []

        total_expectations = len(expectations.expectations)
        columns = []
        for exp in expectations.expectations:
            if "column" in exp.kwargs:
                columns.append(exp.kwargs["column"])
        total_columns = len(set(columns))

        content = content + [
            # TODO: Leaving these two paragraphs as placeholders for later development.
            # "This Expectation suite was first generated by {BasicDatasetProfiler} on {date}, using version {xxx} of Great Expectations.",
            # "{name}, {name}, and {name} have also contributed additions and revisions.",
            "This Expectation suite currently contains %d total Expectations across %d columns." % (
                total_expectations,
                total_columns,
            ),
        ]
        
        if "notes" in expectations.meta:
            notes = expectations.meta["notes"]
            note_content = None
            
            if isinstance(notes, string_types):
                note_content = [notes]
            
            elif isinstance(notes, list):
                note_content = notes
            
            elif isinstance(notes, dict):
                if "format" in notes:
                    if notes["format"] == "string":
                        if isinstance(notes["content"], string_types):
                            note_content = [notes["content"]]
                        elif isinstance(notes["content"], list):
                            note_content = notes["content"]
                        else:
                            logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")
                    
                    elif notes["format"] == "markdown":
                        # ???: Should converting to markdown be the renderer's job, or the view's job?
                        # Renderer is easier, but will end up mixing HTML strings with content_block info.
                        if isinstance(notes["content"], string_types):
                            try:
                                note_content = [pypandoc.convert_text(notes["content"], format='md', to="html")]
                            except OSError:
                                note_content = [notes["content"]]
                        
                        elif isinstance(notes["content"], list):
                            try:
                                note_content = [pypandoc.convert_text(note, format='md', to="html") for note in
                                            notes["content"]]
                            except OSError:
                                note_content = [note for note in notes["content"]]
                        
                        else:
                            logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")
                
                else:
                    logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")
            
            if note_content is not None:
                content = content + note_content

        return TextContent(**{
            "content_block_type": "text",
            "header": "Notes",
            "text": content,
            "styling": {
                "classes": ["col-12", "table-responsive"],
                "styles": {
                    "margin-top": "20px"
                },
                "body": {
                    "classes": ["table", "table-sm"]
                }
            },
        })


class ProfilingResultsPageRenderer(Renderer):

    def __init__(self, overview_section_renderer=None, column_section_renderer=None):
        if overview_section_renderer is None:
            overview_section_renderer = {
                "class_name": "ProfilingResultsOverviewSectionRenderer"
            }
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ProfilingResultsColumnSectionRenderer"
            }
        self._overview_section_renderer = instantiate_class_from_config(
            config=overview_section_renderer,
            runtime_config={},
            config_defaults={
                "module_name": overview_section_renderer.get(
                    "module_name", "great_expectations.render.renderer.other_section_renderer")
            }
        )
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_config={},
            config_defaults={
                "module_name": column_section_renderer.get(
                    "module_name", "great_expectations.render.renderer.column_section_renderer")
            }
        )

    def render(self, validation_results):
        run_id = validation_results.meta['run_id']
        full_data_asset_identifier = validation_results.meta['data_asset_name'] or "//"
        if not isinstance(full_data_asset_identifier, DataAssetIdentifier):
            try:
                # This will cause a validation error if the data asset name is not properly formatted
                full_data_asset_identifier = dataAssetIdentifierSchema.load(full_data_asset_identifier).data
            except ValidationError:
                if isinstance(full_data_asset_identifier, string_types):
                    logger.warning("Rendering validation results using incomplete data_asset_identifier.")
                else:
                    raise
        if isinstance(full_data_asset_identifier, string_types):
            short_data_asset_name = full_data_asset_identifier
        else:
            short_data_asset_name = full_data_asset_identifier.generator_asset
        expectation_suite_name = validation_results.meta['expectation_suite_name']

        # Group EVRs by column
        #TODO: When we implement a ValidationResultSuite class, this method will move there.
        columns = self._group_evrs_by_column(validation_results)

        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
        column_types = self._overview_section_renderer._get_column_types(validation_results)

        if "data_asset_name" in validation_results.meta and validation_results.meta["data_asset_name"]:
            data_asset_name = short_data_asset_name
        else:
            data_asset_name = None

        return RenderedDocumentContent(**{
            "renderer_type": "ProfilingResultsPageRenderer",
            "data_asset_name": data_asset_name,
            "full_data_asset_identifier": full_data_asset_identifier,
            "page_title": run_id + "-" + expectation_suite_name + "-ProfilingResults",
            "utm_medium": "profiling-results-page",
            "sections":
                [
                    self._overview_section_renderer.render(
                        validation_results,
                        section_name="Overview"
                    )
                ] +
                [
                    self._column_section_renderer.render(
                        columns[column],
                        section_name=column,
                        column_type=column_types.get(column),
                    ) for column in ordered_columns
                ]
        })
