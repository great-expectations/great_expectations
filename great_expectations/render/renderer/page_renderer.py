import logging
logger = logging.getLogger(__name__)

import pypandoc

from .renderer import Renderer
from .column_section_renderer import (
    ProfilingResultsColumnSectionRenderer,
    ExpectationSuiteColumnSectionRenderer,
    ValidationResultsColumnSectionRenderer
)
from .other_section_renderer import (
    ProfilingResultsOverviewSectionRenderer,
)
from ..types import (
    RenderedDocumentContent,
    RenderedSectionContent,
    RenderedComponentContent,
)

class ValidationResultsPageRenderer(Renderer):
    @classmethod
    def render(cls, validation_results={}):
        run_id = validation_results['meta']['run_id']
        full_data_asset_identifier = validation_results['meta']['data_asset_name'] or ""
        expectation_suite_name = validation_results['meta']['expectation_suite_name']
        short_data_asset_name = full_data_asset_identifier.split('/')[-1]
    
        # Group EVRs by column
        columns = {}
        for evr in validation_results["results"]:
            if "column" in evr["expectation_config"]["kwargs"]:
                column = evr["expectation_config"]["kwargs"]["column"]
            else:
                column = "Table-level Expectations"
        
            if column not in columns:
                columns[column] = []
            columns[column].append(evr)
    
        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
    
        overview_content_blocks = [
            cls._render_validation_header(),
            cls._render_validation_info(validation_results=validation_results),
            cls._render_validation_statistics(validation_results=validation_results)
        ]
    
        if "data_asset_name" in validation_results["meta"] and validation_results["meta"]["data_asset_name"]:
            data_asset_name = short_data_asset_name
        else:
            data_asset_name = None
    
        sections = [
            RenderedSectionContent(**{
                "section_name": "Overview",
                "content_blocks": overview_content_blocks
            })
        ]
    
        sections += [
            ValidationResultsColumnSectionRenderer.render(
                validation_results=columns[column],
            ) for column in ordered_columns
        ]
    
        return RenderedDocumentContent(**{
            "renderer_type": "ValidationResultsColumnSectionRenderer",
            "data_asset_name": data_asset_name,
            "full_data_asset_identifier": full_data_asset_identifier,
            "page_title": run_id + "-" + expectation_suite_name + "-ProfilingResults",
            "sections": sections
        })
    
    @classmethod
    def _render_validation_header(cls):
        return RenderedComponentContent(**{
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
        run_id = validation_results['meta']['run_id']
        full_data_asset_identifier = validation_results['meta']['data_asset_name'] or ""
        expectation_suite_name = validation_results['meta']['expectation_suite_name']
        ge_version = validation_results["meta"]["great_expectations.__version__"]
        success = validation_results["success"]
        
        return RenderedComponentContent(**{
            "content_block_type": "table",
            "header": "Info",
            "table": [
                ["Full Data Asset Identifier", full_data_asset_identifier],
                ["Expectation Suite Name", expectation_suite_name],
                ["Great Expectations Version", ge_version],
                ["Run ID", run_id],
                ["Validation Succeeded", success]
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
    def _render_validation_statistics(cls, validation_results):
        statistics = validation_results["statistics"]
        return RenderedComponentContent(**{
            "content_block_type": "table",
            "header": "Statistics",
            "table": [
                ["Evaluated Expectations", statistics["evaluated_expectations"]],
                ["Successful Expectations", statistics["successful_expectations"]],
                ["Unsuccessful Expectations", statistics["unsuccessful_expectations"]],
                ["Success Percent", "{0:.2f}%".format(statistics["success_percent"])],
            ],
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
    @classmethod
    def render(cls, expectations):
        columns, ordered_columns = cls._group_and_order_expectations_by_column(expectations)
        full_data_asset_identifier = expectations.get("data_asset_name") or ""
        expectation_suite_name = cls._get_expectation_suite_name(expectations)

        overview_content_blocks = [
            cls._render_asset_header(expectations),
            cls._render_asset_info(expectations)
        ]
        
        asset_notes_content_block = cls._render_asset_notes(expectations)
        if asset_notes_content_block != None:
            overview_content_blocks.append(asset_notes_content_block)
            # import json
            # print(json.dumps(overview_content_blocks, indent=2))
        
        sections = [
            RenderedSectionContent(**{
                "section_name": "Overview",
                "content_blocks": overview_content_blocks,
            })
        ]
        
        sections += [
            ExpectationSuiteColumnSectionRenderer.render(expectations=columns[column]) for column in ordered_columns
        ]
        return RenderedDocumentContent(**{
            # "data_asset_name": short_data_asset_name,
            "full_data_asset_identifier": full_data_asset_identifier,
            "page_title": expectation_suite_name,
            "utm_medium": "expectation-suite-page",
            "sections": sections
        })

    @classmethod
    def _render_asset_header(cls, expectations):
        return RenderedComponentContent(**{
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
        full_data_asset_identifier = expectations.get("data_asset_name") or ""
        data_asset_type = expectations.get("data_asset_type")
        expectation_suite_name = expectations.get("expectation_suite_name")
        ge_version = expectations["meta"]["great_expectations.__version__"]

        return RenderedComponentContent(**{
            "content_block_type": "table",
            "header": "Info",
            "table": [
                ["Full Data Asset Identifier", full_data_asset_identifier],
                ["Data Asset Type", data_asset_type],
                ["Expectation Suite Name", expectation_suite_name],
                ["Great Expectations Version", ge_version]
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
    def _render_asset_notes(cls, expectations):
        
        content = []
        
        if "expectations" in expectations:
            # This if statement is a precaution in case the expectation suite doesn't contain expectations.
            # Once we have more strongly typed classes for suites, this shouldn't be necessary.
            
            total_expectations = len(expectations["expectations"])
            columns = []
            for exp in expectations["expectations"]:
                if "column" in exp["kwargs"]:
                    columns.append(exp["kwargs"]["column"])
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
        
        if "notes" in expectations["meta"]:
            notes = expectations["meta"]["notes"]
            note_content = None
            
            if type(notes) == str:
                note_content = [notes]
            
            elif type(notes) == list:
                note_content = notes
            
            elif type(notes) == dict:
                if "format" in notes:
                    if notes["format"] == "string":
                        if type(notes["content"]) == str:
                            note_content = [notes["content"]]
                        elif type(notes["content"]) == list:
                            note_content = notes["content"]
                        else:
                            logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")
                    
                    elif notes["format"] == "markdown":
                        # ???: Should converting to markdown be the renderer's job, or the view's job?
                        # Renderer is easier, but will end up mixing HTML strings with content_block info.
                        if type(notes["content"]) == str:
                            try:
                                note_content = [pypandoc.convert_text(notes["content"], format='md', to="html")]
                            except OSError:
                                note_content = [notes["content"]]
                        
                        elif type(notes["content"]) == list:
                            try:
                                note_content = [pypandoc.convert_text(note, format='md', to="html") for note in
                                            notes["content"]]
                            except OSError:
                                note_content = [note for note in notes["content"]]
                        
                        else:
                            logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")
                
                else:
                    logger.warning("Unrecognized Expectation suite notes format. Skipping rendering.")
            
            if note_content != None:
                content = content + note_content

        return RenderedComponentContent(**{
            "content_block_type": "text",
            "header": "Notes",
            "content": content,
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

    @classmethod
    def render(cls, validation_results):
        run_id = validation_results['meta']['run_id']
        full_data_asset_identifier = validation_results['meta']['data_asset_name'] or ""
        expectation_suite_name = validation_results['meta']['expectation_suite_name']
        short_data_asset_name = full_data_asset_identifier.split('/')[-1]

        # Group EVRs by column
        #TODO: When we implement a ValidationResultSuite class, this method will move there.
        columns = cls._group_evrs_by_column(validation_results)

        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
        column_types = ProfilingResultsOverviewSectionRenderer._get_column_types(validation_results)

        if "data_asset_name" in validation_results["meta"] and validation_results["meta"]["data_asset_name"]:
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
                    ProfilingResultsOverviewSectionRenderer.render(
                        validation_results,
                        section_name="Overview"
                    )
                ] +
                [
                    ProfilingResultsColumnSectionRenderer.render(
                        columns[column],
                        section_name=column,
                        column_type=column_types.get(column),
                    ) for column in ordered_columns
                ]
        })
