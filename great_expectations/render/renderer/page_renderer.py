import logging
import os
from collections import OrderedDict, defaultdict
from typing import Dict, List, Tuple, Union

from dateutil.parser import parse

from great_expectations.core import ExpectationSuite
from great_expectations.core.expectation_validation_result import (
    ExpectationSuiteValidationResult,
)
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.exceptions import ClassInstantiationError
from great_expectations.render import (
    CollapseContent,
    LegacyDiagnosticRendererType,
    RenderedComponentContent,
    RenderedDocumentContent,
    RenderedHeaderContent,
    RenderedMarkdownContent,
    RenderedSectionContent,
    RenderedStringTemplateContent,
    RenderedTableContent,
    TextContent,
)
from great_expectations.render.renderer.renderer import Renderer
from great_expectations.render.util import num_to_str
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)

logger = logging.getLogger(__name__)


class ValidationResultsPageRenderer(Renderer):
    def __init__(
        self,
        column_section_renderer=None,
        run_info_at_end: bool = False,
        data_context=None,
    ) -> None:
        """
        Args:
            column_section_renderer:
            run_info_at_end: Move the run info (Info, Batch Markers, Batch Kwargs) to the end
                of the rendered output rather than after Statistics.
        """
        super().__init__()
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ValidationResultsColumnSectionRenderer"
            }
        module_name = "great_expectations.render.renderer.column_section_renderer"
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": column_section_renderer.get("module_name", module_name)
            },
        )
        if not self._column_section_renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=column_section_renderer["class_name"],
            )
        self.run_info_at_end = run_info_at_end
        self._data_context = data_context

    def render_validation_operator_result(
        self, validation_operator_result: ValidationOperatorResult
    ) -> List[RenderedDocumentContent]:
        """
        Render a ValidationOperatorResult which can have multiple ExpectationSuiteValidationResult

        Args:
            validation_operator_result: ValidationOperatorResult

        Returns:
            List[RenderedDocumentContent]
        """
        return [
            self.render(validation_result)
            for validation_result in validation_operator_result.list_validation_results()
        ]

    # TODO: deprecate dual batch api support in 0.14
    def render(
        self,
        validation_results: ExpectationSuiteValidationResult,
        evaluation_parameters=None,
    ):
        # Gather run identifiers
        run_name, run_time = self._parse_run_values(validation_results)
        expectation_suite_name = validation_results.meta["expectation_suite_name"]
        batch_kwargs = (
            validation_results.meta.get("batch_kwargs", {})
            or validation_results.meta.get("batch_spec", {})
            or {}
        )

        # Add datasource key to batch_kwargs if missing
        if "datasource" not in batch_kwargs:
            # Check if expectation_suite_name follows datasource.batch_kwargs_generator.data_asset_name.suite_name pattern
            if len(expectation_suite_name.split(".")) == 4:  # noqa: PLR2004
                batch_kwargs["datasource"] = expectation_suite_name.split(".")[0]

        columns = self._group_evrs_by_column(validation_results, expectation_suite_name)
        overview_content_blocks = [
            self._render_validation_header(validation_results),
            self._render_validation_statistics(validation_results=validation_results),
        ]

        collapse_content_blocks = [
            self._render_validation_info(validation_results=validation_results)
        ]
        collapse_content_block = self._generate_collapse_content_block(
            collapse_content_blocks, validation_results
        )

        if not self.run_info_at_end:
            overview_content_blocks.append(collapse_content_block)

        sections = self._collect_rendered_document_content_sections(
            validation_results,
            overview_content_blocks,
            collapse_content_blocks,
            columns,
        )

        # Determine whether we have a custom run_name
        data_asset_name = batch_kwargs.get("data_asset_name", "")
        page_title = self._determine_page_title(
            run_name, run_time, data_asset_name, expectation_suite_name
        )

        return RenderedDocumentContent(
            **{
                "renderer_type": "ValidationResultsPageRenderer",
                "page_title": page_title,
                "batch_kwargs": batch_kwargs
                if "batch_kwargs" in validation_results.meta
                else None,
                "batch_spec": batch_kwargs
                if "batch_spec" in validation_results.meta
                else None,
                "expectation_suite_name": expectation_suite_name,
                "sections": sections,
                "utm_medium": "validation-results-page",
            }
        )

    def _parse_run_values(
        self, validation_results: ExpectationSuiteValidationResult
    ) -> Tuple[str, str]:
        run_id: Union[str, dict, RunIdentifier] = validation_results.meta["run_id"]
        if isinstance(run_id, str):
            try:
                run_time = parse(run_id).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except (ValueError, TypeError):
                run_time = "__none__"
            run_name = run_id
        elif isinstance(run_id, dict):
            run_name = run_id.get("run_name") or "__none__"
            try:
                t = run_id.get("run_time", "")
                run_time = parse(t).strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, TypeError):
                run_time = "__none__"
        elif isinstance(run_id, RunIdentifier):
            run_name = run_id.run_name or "__none__"
            run_time = run_id.run_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        return run_name, run_time

    def _group_evrs_by_column(
        self,
        validation_results: ExpectationSuiteValidationResult,
        expectation_suite_name: str,
    ) -> Dict[str, list]:
        columns = defaultdict(list)
        try:
            suite_meta = (
                self._data_context.get_expectation_suite(expectation_suite_name).meta
                if self._data_context is not None
                else None
            )
        except Exception:
            suite_meta = None
        meta_properties_to_render = self._get_meta_properties_notes(suite_meta)
        for evr in validation_results.results:
            if meta_properties_to_render is not None:
                evr.expectation_config.kwargs[
                    "meta_properties_to_render"
                ] = meta_properties_to_render
            if "column" in evr.expectation_config.kwargs:
                column = evr.expectation_config.kwargs["column"]
            else:
                column = "Table-Level Expectations"

            columns[column].append(evr)

        return columns

    def _generate_collapse_content_block(
        self,
        collapse_content_blocks: List[RenderedTableContent],
        validation_results: ExpectationSuiteValidationResult,
    ) -> CollapseContent:
        attrs = [
            ("batch_markers", "Batch Markers"),
            ("batch_kwargs", "Batch Kwargs"),
            ("batch_parameters", "Batch Parameters"),
            ("batch_spec", "Batch Spec"),
            ("batch_request", "Batch Definition"),
        ]

        for attr, header in attrs:
            if validation_results.meta.get(attr):
                table = self._render_nested_table_from_dict(
                    input_dict=validation_results.meta.get(attr),
                    header=header,
                )
                collapse_content_blocks.append(table)

        collapse_content_block = CollapseContent(
            **{
                "collapse_toggle_link": "Show more info...",
                "collapse": collapse_content_blocks,
                "styling": {
                    "body": {"classes": ["card", "card-body"]},
                    "classes": ["col-12", "p-1"],
                },
            }
        )

        return collapse_content_block

    def _collect_rendered_document_content_sections(
        self,
        validation_results: ExpectationSuiteValidationResult,
        overview_content_blocks: List[RenderedComponentContent],
        collapse_content_blocks: List[RenderedTableContent],
        columns: Dict[str, list],
    ) -> List[RenderedSectionContent]:
        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
        sections = [
            RenderedSectionContent(
                **{
                    "section_name": "Overview",
                    "content_blocks": overview_content_blocks,
                }
            )
        ]

        if "Table-Level Expectations" in columns:
            sections += [
                self._column_section_renderer.render(
                    validation_results=columns["Table-Level Expectations"],
                    evaluation_parameters=validation_results.evaluation_parameters,
                )
            ]

        sections += [
            self._column_section_renderer.render(
                validation_results=columns[column],
                evaluation_parameters=validation_results.evaluation_parameters,
            )
            for column in ordered_columns
        ]
        if self.run_info_at_end:
            sections += [
                RenderedSectionContent(
                    **{
                        "section_name": "Run Info",
                        "content_blocks": collapse_content_blocks,
                    }
                )
            ]

        return sections

    def _determine_page_title(
        self,
        run_name: str,
        run_time: str,
        data_asset_name: str,
        expectation_suite_name: str,
    ) -> str:
        try:
            run_name_as_time = parse(run_name)
        except ValueError:
            run_name_as_time = None
        try:
            run_time_datetime = parse(run_time)
        except ValueError:
            run_time_datetime = None

        include_run_name: bool = False
        if run_name_as_time != run_time_datetime and run_name_as_time != "__none__":
            include_run_name = True

        page_title = f"Validations / {expectation_suite_name}"
        if data_asset_name:
            page_title += f" / {data_asset_name}"
        if include_run_name:
            page_title += f" / {run_name}"
        page_title += f" / {run_time}"

        return page_title

    @classmethod
    def _get_meta_properties_notes(cls, suite_meta):
        """
        This method is used for fetching the custom meta to be added at the suite level
        "notes": {
            "content": {
                "dimension": "properties.dimension",
                "severity": "properties.severity"
            },
            "format": LegacyDiagnosticRendererType.META_PROPERTIES
        }
        expectation level
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {
                "column": "city"
            },
            "meta": {
                "attributes": {
                    "properties": {
                        "dimension": "completeness",
                        "severity": "P3"
                    },
                    "user_meta": {
                        "notes": ""
                    }
                }
            }
        }
        This will fetch dimension and severity values which are in the expectation meta.

        """
        if (
            suite_meta is not None
            and "notes" in suite_meta
            and "format" in suite_meta["notes"]
            and suite_meta["notes"]["format"]
            == LegacyDiagnosticRendererType.META_PROPERTIES
        ):
            return suite_meta["notes"]["content"]
        else:
            return None

    @classmethod
    def _render_validation_header(cls, validation_results):
        success = validation_results.success
        expectation_suite_name = validation_results.meta["expectation_suite_name"]
        expectation_suite_path_components = (
            [".." for _ in range(len(expectation_suite_name.split(".")) + 3)]
            + ["expectations"]
            + str(expectation_suite_name).split(".")
        )
        expectation_suite_path = (
            f"{os.path.join(*expectation_suite_path_components)}.html"  # noqa: PTH118
        )
        # TODO: deprecate dual batch api support in 0.14
        batch_kwargs = (
            validation_results.meta.get("batch_kwargs", {})
            or validation_results.meta.get("batch_spec", {})
            or {}
        )
        data_asset_name = batch_kwargs.get("data_asset_name")

        if success:
            success = "Succeeded"
            html_success_icon = (
                '<i class="fas fa-check-circle text-success" aria-hidden="true"></i>'
            )
        else:
            success = "Failed"
            html_success_icon = (
                '<i class="fas fa-times text-danger" aria-hidden="true"></i>'
            )

        return RenderedHeaderContent(
            **{
                "content_block_type": "header",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Overview",
                            "tag": "h5",
                            "styling": {"classes": ["m-0"]},
                        },
                    }
                ),
                "subheader": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "${suite_title} ${expectation_suite_name}\n ${data_asset} ${data_asset_name}\n ${status_title} ${html_success_icon} ${success}",
                            "params": {
                                "suite_title": "Expectation Suite:",
                                "data_asset": "Data asset:",
                                "data_asset_name": data_asset_name,
                                "status_title": "Status:",
                                "expectation_suite_name": expectation_suite_name,
                                "success": success,
                                "html_success_icon": html_success_icon,
                            },
                            "styling": {
                                "params": {
                                    "suite_title": {"classes": ["h6"]},
                                    "status_title": {"classes": ["h6"]},
                                    "expectation_suite_name": {
                                        "tag": "a",
                                        "attributes": {"href": expectation_suite_path},
                                    },
                                },
                                "classes": ["mb-0", "mt-1"],
                            },
                        },
                    }
                ),
                "styling": {
                    "classes": ["col-12", "p-0"],
                    "header": {"classes": ["alert", "alert-secondary"]},
                },
            }
        )

    @classmethod
    def _render_validation_info(cls, validation_results):
        run_id = validation_results.meta["run_id"]
        if isinstance(run_id, str):
            try:
                run_time = parse(run_id).strftime("%Y-%m-%dT%H:%M:%SZ")
            except (ValueError, TypeError):
                run_time = "__none__"
            run_name = run_id
        elif isinstance(run_id, dict):
            run_name = run_id.get("run_name") or "__none__"
            try:
                run_time = str(
                    parse(run_id.get("run_time")).strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            except (ValueError, TypeError):
                run_time = "__none__"
        elif isinstance(run_id, RunIdentifier):
            run_name = run_id.run_name or "__none__"
            run_time = run_id.run_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        # TODO: Deprecate "great_expectations.__version__"
        ge_version = validation_results.meta.get(
            "great_expectations_version"
        ) or validation_results.meta.get("great_expectations.__version__")

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Info",
                            "tag": "h6",
                            "styling": {"classes": ["m-0"]},
                        },
                    }
                ),
                "table": [
                    ["Great Expectations Version", ge_version],
                    ["Run Name", run_name],
                    ["Run Time", run_time],
                ],
                "styling": {
                    "classes": ["col-12", "table-responsive", "mt-1"],
                    "body": {
                        "classes": ["table", "table-sm"],
                        "styles": {
                            "margin-bottom": "0.5rem !important",
                            "margin-top": "0.5rem !important",
                        },
                    },
                },
            }
        )

    @classmethod
    def _render_nested_table_from_dict(cls, input_dict, header=None, sub_table=False):
        table_rows = []
        for kwarg, value in input_dict.items():
            if not isinstance(value, (dict, OrderedDict)):
                table_row = [
                    RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "$value",
                                "params": {"value": str(kwarg)},
                                "styling": {
                                    "default": {"styles": {"word-break": "break-all"}},
                                },
                            },
                            "styling": {
                                "parent": {
                                    "classes": ["pr-3"],
                                }
                            },
                        }
                    ),
                    RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "$value",
                                "params": {"value": str(value)},
                                "styling": {
                                    "default": {"styles": {"word-break": "break-all"}},
                                },
                            },
                            "styling": {
                                "parent": {
                                    "classes": [],
                                }
                            },
                        }
                    ),
                ]
            else:
                table_row = [
                    RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": "$value",
                                "params": {"value": str(kwarg)},
                                "styling": {
                                    "default": {"styles": {"word-break": "break-all"}},
                                },
                            },
                            "styling": {
                                "parent": {
                                    "classes": ["pr-3"],
                                }
                            },
                        }
                    ),
                    cls._render_nested_table_from_dict(value, sub_table=True),
                ]
            table_rows.append(table_row)

        table_rows.sort(key=lambda row: row[0].string_template["params"]["value"])

        if sub_table:
            return RenderedTableContent(
                **{
                    "content_block_type": "table",
                    "table": table_rows,
                    "styling": {
                        "classes": ["col-6", "table-responsive"],
                        "body": {"classes": ["table", "table-sm", "m-0"]},
                        "parent": {"classes": ["pt-0", "pl-0", "border-top-0"]},
                    },
                }
            )
        else:
            return RenderedTableContent(
                **{
                    "content_block_type": "table",
                    "header": RenderedStringTemplateContent(
                        **{
                            "content_block_type": "string_template",
                            "string_template": {
                                "template": header,
                                "tag": "h6",
                                "styling": {"classes": ["m-0"]},
                            },
                        }
                    ),
                    "table": table_rows,
                    "styling": {
                        "body": {
                            "classes": ["table", "table-sm"],
                            "styles": {
                                "margin-bottom": "0.5rem !important",
                                "margin-top": "0.5rem !important",
                            },
                        }
                    },
                }
            )

    @classmethod
    def _render_validation_statistics(cls, validation_results):
        statistics = validation_results.statistics
        statistics_dict = OrderedDict(
            [
                ("evaluated_expectations", "Evaluated Expectations"),
                ("successful_expectations", "Successful Expectations"),
                ("unsuccessful_expectations", "Unsuccessful Expectations"),
                ("success_percent", "Success Percent"),
            ]
        )
        table_rows = []
        for key, value in statistics_dict.items():
            if statistics.get(key) is not None:
                if key == "success_percent":
                    # table_rows.append([value, "{0:.2f}%".format(statistics[key])])
                    table_rows.append(
                        [value, f"{num_to_str(statistics[key], precision=4)}%"]
                    )
                else:
                    table_rows.append([value, statistics[key]])

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Statistics",
                            "tag": "h6",
                            "styling": {"classes": ["m-0"]},
                        },
                    }
                ),
                "table": table_rows,
                "styling": {
                    "classes": ["col-6", "table-responsive", "mt-1", "p-1"],
                    "body": {
                        "classes": ["table", "table-sm"],
                        "styles": {
                            "margin-bottom": "0.5rem !important",
                            "margin-top": "0.5rem !important",
                        },
                    },
                },
            }
        )


class ExpectationSuitePageRenderer(Renderer):
    def __init__(self, column_section_renderer=None) -> None:
        super().__init__()
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ExpectationSuiteColumnSectionRenderer"
            }
        module_name = "great_expectations.render.renderer.column_section_renderer"
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": column_section_renderer.get("module_name", module_name)
            },
        )
        if not self._column_section_renderer:
            raise ClassInstantiationError(
                module_name=column_section_renderer,
                package_name=None,
                class_name=column_section_renderer["class_name"],
            )

    def render(self, expectations):
        if isinstance(expectations, dict):
            expectations = ExpectationSuite(**expectations, data_context=None)
        (
            columns,
            ordered_columns,
        ) = expectations.get_grouped_and_ordered_expectations_by_column()
        expectation_suite_name = expectations.expectation_suite_name

        overview_content_blocks = [
            self._render_expectation_suite_header(),
            self._render_expectation_suite_info(expectations),
        ]

        table_level_expectations_content_block = self._render_table_level_expectations(
            columns
        )
        if table_level_expectations_content_block is not None:
            overview_content_blocks.append(table_level_expectations_content_block)

        asset_notes_content_block = self._render_expectation_suite_notes(expectations)
        if asset_notes_content_block is not None:
            overview_content_blocks.append(asset_notes_content_block)

        sections = [
            RenderedSectionContent(
                **{
                    "section_name": "Overview",
                    "content_blocks": overview_content_blocks,
                }
            )
        ]

        sections += [
            self._column_section_renderer.render(expectations=columns[column])
            for column in ordered_columns
            if column != "_nocolumn"
        ]
        return RenderedDocumentContent(
            **{
                "renderer_type": "ExpectationSuitePageRenderer",
                "page_title": f"Expectations / {str(expectation_suite_name)}",
                "expectation_suite_name": expectation_suite_name,
                "utm_medium": "expectation-suite-page",
                "sections": sections,
            }
        )

    def _render_table_level_expectations(self, columns):
        table_level_expectations = columns.get("_nocolumn")
        if not table_level_expectations:
            return None
        else:
            expectation_bullet_list = self._column_section_renderer.render(
                expectations=table_level_expectations
            ).content_blocks[1]
            expectation_bullet_list.header = RenderedStringTemplateContent(
                **{
                    "content_block_type": "string_template",
                    "string_template": {
                        "template": "Table-Level Expectations",
                        "tag": "h6",
                        "styling": {"classes": ["m-0"]},
                    },
                }
            )
            return expectation_bullet_list

    @classmethod
    def _render_expectation_suite_header(cls):
        return RenderedHeaderContent(
            **{
                "content_block_type": "header",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Overview",
                            "tag": "h5",
                            "styling": {"classes": ["m-0"]},
                        },
                    }
                ),
                "styling": {
                    "classes": ["col-12"],
                    "header": {"classes": ["alert", "alert-secondary"]},
                },
            }
        )

    @classmethod
    def _render_expectation_suite_info(cls, expectations):
        expectation_suite_name = expectations.expectation_suite_name
        # TODO: Deprecate "great_expectations.__version__"
        ge_version = expectations.meta.get(
            "great_expectations_version"
        ) or expectations.meta.get("great_expectations.__version__")

        return RenderedTableContent(
            **{
                "content_block_type": "table",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Info",
                            "tag": "h6",
                            "styling": {"classes": ["m-0"]},
                        },
                    }
                ),
                "table": [
                    ["Expectation Suite Name", expectation_suite_name],
                    ["Great Expectations Version", ge_version],
                ],
                "styling": {
                    "classes": ["col-12", "table-responsive", "mt-1"],
                    "body": {
                        "classes": ["table", "table-sm"],
                        "styles": {
                            "margin-bottom": "0.5rem !important",
                            "margin-top": "0.5rem !important",
                        },
                    },
                },
            }
        )

    # TODO: Update tests
    @classmethod
    def _render_expectation_suite_notes(cls, expectations):  # noqa: PLR0912
        content = []

        total_expectations = len(expectations.expectations)
        columns = []
        for exp in expectations.expectations:
            if "column" in exp.kwargs:
                columns.append(exp.kwargs["column"])
        total_columns = len(set(columns))

        content += [
            # TODO: Leaving these two paragraphs as placeholders for later development.
            # "This Expectation suite was first generated by {BasicDatasetProfiler} on {date}, using version {xxx} of Great Expectations.",
            # "{name}, {name}, and {name} have also contributed additions and revisions.",
            f"This Expectation suite currently contains {total_expectations} total Expectations across {total_columns} columns.",
        ]

        if "notes" in expectations.meta:
            notes = expectations.meta["notes"]
            note_content = None

            if isinstance(notes, str):
                note_content = [notes]

            elif isinstance(notes, list):
                note_content = notes

            elif isinstance(notes, dict):
                if "format" in notes:
                    if notes["format"] == "string":
                        if isinstance(notes["content"], str):
                            note_content = [notes["content"]]
                        elif isinstance(notes["content"], list):
                            note_content = notes["content"]
                        else:
                            logger.warning(
                                "Unrecognized Expectation suite notes format. Skipping rendering."
                            )

                    elif notes["format"] == "markdown":
                        if isinstance(notes["content"], str):
                            note_content = [
                                RenderedMarkdownContent(
                                    **{
                                        "content_block_type": "markdown",
                                        "markdown": notes["content"],
                                        "styling": {"parent": {}},
                                    }
                                )
                            ]
                        elif isinstance(notes["content"], list):
                            note_content = [
                                RenderedMarkdownContent(
                                    **{
                                        "content_block_type": "markdown",
                                        "markdown": note,
                                        "styling": {"parent": {}},
                                    }
                                )
                                for note in notes["content"]
                            ]
                        else:
                            logger.warning(
                                "Unrecognized Expectation suite notes format. Skipping rendering."
                            )
                else:
                    logger.warning(
                        "Unrecognized Expectation suite notes format. Skipping rendering."
                    )

            if note_content is not None:
                content += note_content

        return TextContent(
            **{
                "content_block_type": "text",
                "header": RenderedStringTemplateContent(
                    **{
                        "content_block_type": "string_template",
                        "string_template": {
                            "template": "Notes",
                            "tag": "h6",
                            "styling": {"classes": ["m-0"]},
                        },
                    }
                ),
                "text": content,
                "styling": {
                    "classes": ["col-12", "table-responsive", "mt-1"],
                    "body": {"classes": ["table", "table-sm"]},
                },
            }
        )


class ProfilingResultsPageRenderer(Renderer):
    def __init__(
        self, overview_section_renderer=None, column_section_renderer=None
    ) -> None:
        super().__init__()
        if overview_section_renderer is None:
            overview_section_renderer = {
                "class_name": "ProfilingResultsOverviewSectionRenderer"
            }
        if column_section_renderer is None:
            column_section_renderer = {
                "class_name": "ProfilingResultsColumnSectionRenderer"
            }
        module_name = "great_expectations.render.renderer.profiling_results_overview_section_renderer"
        self._overview_section_renderer = instantiate_class_from_config(
            config=overview_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": overview_section_renderer.get("module_name", module_name)
            },
        )
        if not self._overview_section_renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=overview_section_renderer["class_name"],
            )
        module_name = "great_expectations.render.renderer.column_section_renderer"
        self._column_section_renderer = instantiate_class_from_config(
            config=column_section_renderer,
            runtime_environment={},
            config_defaults={
                "module_name": column_section_renderer.get("module_name", module_name)
            },
        )
        if not self._column_section_renderer:
            raise ClassInstantiationError(
                module_name=module_name,
                package_name=None,
                class_name=column_section_renderer["class_name"],
            )

    def render(self, validation_results):  # noqa: PLR0912
        run_id = validation_results.meta["run_id"]
        if isinstance(run_id, str):
            try:
                run_time = parse(run_id).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            except (ValueError, TypeError):
                run_time = "__none__"
            run_name = run_id
        elif isinstance(run_id, dict):
            run_name = run_id.get("run_name") or "__none__"
            run_time = run_id.get("run_time") or "__none__"
        elif isinstance(run_id, RunIdentifier):
            run_name = run_id.run_name or "__none__"
            run_time = run_id.run_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        expectation_suite_name = validation_results.meta["expectation_suite_name"]
        batch_kwargs = validation_results.meta.get(
            "batch_kwargs", {}
        ) or validation_results.meta.get("batch_spec", {})

        # add datasource key to batch_kwargs if missing
        if "datasource" not in batch_kwargs and "datasource" not in batch_kwargs:
            # check if expectation_suite_name follows datasource.batch_kwargs_generator.data_asset_name.suite_name pattern
            if len(expectation_suite_name.split(".")) == 4:  # noqa: PLR2004
                if "batch_kwargs" in validation_results.meta:
                    batch_kwargs["datasource"] = expectation_suite_name.split(".")[0]
                else:
                    batch_kwargs["datasource"] = expectation_suite_name.split(".")[0]

        # Group EVRs by column
        # TODO: When we implement a ValidationResultSuite class, this method will move there.
        columns = self._group_evrs_by_column(validation_results)

        ordered_columns = Renderer._get_column_list_from_evrs(validation_results)
        column_types = self._overview_section_renderer._get_column_types(
            validation_results
        )

        data_asset_name = batch_kwargs.get("data_asset_name")
        # Determine whether we have a custom run_name
        try:
            run_name_as_time = parse(run_name)
        except ValueError:
            run_name_as_time = None
        try:
            run_time_datetime = parse(run_time)
        except ValueError:
            run_time_datetime = None

        include_run_name: bool = False
        if run_name_as_time != run_time_datetime and run_name_as_time != "__none__":
            include_run_name = True

        page_title = f"Profiling Results / {str(expectation_suite_name)}"
        if data_asset_name:
            page_title += f" / {str(data_asset_name)}"
        if include_run_name:
            page_title += f" / {str(run_name)}"
        page_title += f" / {str(run_time)}"

        return RenderedDocumentContent(
            **{
                "renderer_type": "ProfilingResultsPageRenderer",
                "page_title": page_title,
                "expectation_suite_name": expectation_suite_name,
                "utm_medium": "profiling-results-page",
                "batch_kwargs": batch_kwargs
                if "batch_kwargs" in validation_results.meta
                else None,
                "batch_spec": batch_kwargs
                if "batch_spec" in validation_results.meta
                else None,
                "sections": [
                    self._overview_section_renderer.render(
                        validation_results, section_name="Overview"
                    )
                ]
                + [
                    self._column_section_renderer.render(
                        columns[column],
                        section_name=column,
                        column_type=column_types.get(column),
                    )
                    for column in ordered_columns
                ],
            }
        )
