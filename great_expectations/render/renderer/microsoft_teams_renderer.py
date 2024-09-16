from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from great_expectations.compatibility.typing_extensions import override
from great_expectations.core import RunIdentifier
from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        ValidationResultIdentifier,
    )

logger = logging.getLogger(__name__)


class MicrosoftTeamsRenderer(Renderer):
    MICROSOFT_TEAMS_SCHEMA_URL = "http://adaptivecards.io/schemas/adaptive-card.json"

    @override
    def render(
        self,
        checkpoint_result: CheckpointResult,
        data_docs_pages: dict[ValidationResultIdentifier, dict[str, str]] | None = None,
    ) -> dict:
        checkpoint_blocks: list[list[dict[str, str]]] = []
        for result_identifier, result in checkpoint_result.run_results.items():
            validation_blocks = self._render_validation_result(
                validation_result=result, validation_result_suite_identifier=result_identifier
            )
            checkpoint_blocks.append(validation_blocks)

        data_docs_block = self._render_data_docs_links(data_docs_pages=data_docs_pages)
        return self._build_payload(
            checkpoint_result=checkpoint_result,
            checkpoint_blocks=checkpoint_blocks,
            data_docs_block=data_docs_block,
        )

    def _render_validation_result(
        self,
        validation_result: ExpectationSuiteValidationResult,
        validation_result_suite_identifier: ValidationResultIdentifier,
    ) -> list[dict[str, str]]:
        return [
            self._render_status(validation_result=validation_result),
            self._render_asset_name(validation_result=validation_result),
            self._render_suite_name(validation_result=validation_result),
            self._render_run_name(
                validation_result_suite_identifier=validation_result_suite_identifier
            ),
            self._render_batch_id(validation_result=validation_result),
            self._render_summary(validation_result=validation_result),
        ]

    def _render_status(self, validation_result: ExpectationSuiteValidationResult) -> dict[str, str]:
        status = "Success !!!" if validation_result.success else "Failure :("
        return self._render_validation_result_element(
            key="Batch Validation Status",
            value=status,
            validation_result=validation_result,
        )

    def _render_asset_name(
        self, validation_result: ExpectationSuiteValidationResult
    ) -> dict[str, str]:
        data_asset_name = validation_result.asset_name or "__no_data_asset_name__"
        return self._render_validation_result_element(key="Data Asset Name", value=data_asset_name)

    def _render_suite_name(
        self, validation_result: ExpectationSuiteValidationResult
    ) -> dict[str, str]:
        expectation_suite_name = validation_result.suite_name
        return self._render_validation_result_element(
            key="Expectation Suite Name", value=expectation_suite_name
        )

    def _render_run_name(
        self, validation_result_suite_identifier: ValidationResultIdentifier
    ) -> dict[str, str]:
        run_id = validation_result_suite_identifier.run_id
        run_name = run_id.run_name if isinstance(run_id, RunIdentifier) else run_id
        return self._render_validation_result_element(key="Run Name", value=run_name)

    def _render_batch_id(
        self, validation_result: ExpectationSuiteValidationResult
    ) -> dict[str, str]:
        return self._render_validation_result_element(
            key="Batch ID", value=validation_result.batch_id
        )

    def _render_summary(
        self, validation_result: ExpectationSuiteValidationResult
    ) -> dict[str, str]:
        n_checks_succeeded = validation_result.statistics["successful_expectations"]
        n_checks = validation_result.statistics["evaluated_expectations"]
        check_details_text = f"*{n_checks_succeeded}* of *{n_checks}* expectations were met"
        return self._render_validation_result_element(key="Summary", value=check_details_text)

    def _render_data_docs_links(
        self, data_docs_pages: dict[ValidationResultIdentifier, dict[str, str]] | None
    ) -> list[dict[str, str]] | None:
        if not data_docs_pages:
            return None

        elements: list[dict[str, str]] = []
        for data_docs_page in data_docs_pages.values():
            for docs_link_key, docs_link in data_docs_page.items():
                if docs_link_key == "class":
                    continue
                report_element = self._get_report_element(docs_link)
                elements.append(report_element)

        return elements

    def _concatenate_blocks(
        self, title_block: dict, checkpoint_blocks: list[list[dict]]
    ) -> list[dict]:
        containers: list[dict] = [title_block]
        for block in checkpoint_blocks:
            validation_container = {
                "type": "Container",
                "height": "auto",
                "separator": "true",
                "items": [
                    {
                        "type": "TextBlock",
                        "text": block,
                        "horizontalAlignment": "left",
                    }
                ],
            }
            containers.append(validation_container)

        return containers

    def _build_payload(
        self,
        checkpoint_result: CheckpointResult,
        checkpoint_blocks: list[list[dict[str, str]]],
        data_docs_block: list[dict[str, str]] | None,
    ) -> dict:
        checkpoint_name = checkpoint_result.checkpoint_config.name
        status = "Success !!!" if checkpoint_result.success else "Failure :("

        title_block = {
            "type": "Container",
            "height": "auto",
            "separator": "true",
            "items": [
                {
                    "type": "ColumnSet",
                    "columns": [
                        {
                            "type": "Column",
                            "width": "stretch",
                            "items": [
                                {
                                    "type": "TextBlock",
                                    "text": f"Checkpoint Result: {checkpoint_name} ({status})",
                                    "weight": "bolder",
                                    "size": "large",
                                    "wrap": "true",
                                },
                            ],
                        }
                    ],
                },
            ],
        }

        return {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": self.MICROSOFT_TEAMS_SCHEMA_URL,
                        "type": "AdaptiveCard",
                        "version": "1.0",
                        "body": self._concatenate_blocks(
                            title_block=title_block, checkpoint_blocks=checkpoint_blocks
                        ),
                        "actions": data_docs_block or [],
                    },
                }
            ],
        }

    @staticmethod
    def _get_report_element(docs_link):
        report_element = {
            "type": "Action.OpenUrl",
            "title": "Open data docs",
            "url": docs_link,
        }
        return report_element

    @staticmethod
    def _render_validation_result_element(key, value, validation_result=None):
        validation_result_element = {
            "type": "TextBlock",
            "text": f"**{key}:** {value}",
            "horizontalAlignment": "left",
        }
        if validation_result and validation_result.success:
            validation_result_element["color"] = "good"
        elif validation_result and not validation_result.success:
            validation_result_element["color"] = "attention"
        return validation_result_element
