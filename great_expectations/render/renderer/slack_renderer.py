from __future__ import annotations

import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any

logger = logging.getLogger(__name__)

from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.core import RunIdentifier
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )
    from great_expectations.data_context.types.resource_identifiers import (
        ValidationResultIdentifier,
    )


class SlackRenderer(Renderer):
    def render(
        self,
        validation_result: ExpectationSuiteValidationResult,
        data_docs_pages: dict[ValidationResultIdentifier, dict[str, str]] | None = None,
        notify_with: list[str] | None = None,
        validation_result_urls: list[str] | None = None,
    ) -> list[dict]:
        data_docs_pages = data_docs_pages or {}
        notify_with = notify_with or []
        validation_result_urls = validation_result_urls or []
        blocks: list[dict] = []

        description_block = self._build_description_block(
            validation_result=validation_result,
            validation_result_urls=validation_result_urls,
        )
        blocks.append(description_block)

        for data_docs_page in data_docs_pages.values():
            report_element_block = self._build_report_element_block(
                data_docs_page=data_docs_page, notify_with=notify_with
            )
            if report_element_block:
                blocks.append(report_element_block)

        return blocks

    def _build_description_block(
        self,
        validation_result: ExpectationSuiteValidationResult,
        validation_result_urls: list[str],
    ) -> dict:
        status = "Failed :x:"
        if validation_result.success:
            status = "Success :tada:"

        validation_link = None
        summary_text = ""
        if validation_result_urls:
            if len(validation_result_urls) == 1:
                validation_link = validation_result_urls[0]
            else:
                title_hlink = "*Validation Results*"
                batch_validation_status_hlinks = "".join(
                    f"*<{validation_result_url} | {status}>*"
                    for validation_result_url in validation_result_urls
                )
                summary_text += f"""{title_hlink}
    {batch_validation_status_hlinks}
                """

        expectation_suite_name = validation_result.suite_name
        data_asset_name = validation_result.asset_name or "__no_data_asset_name__"
        summary_text += f"*Asset*: {data_asset_name}  "
        # Slack does not allow links to local files due to security risks
        # DataDocs links will be added in a block after this summary text when applicable
        if validation_link and "file://" not in validation_link:
            summary_text += (
                f"*Expectation Suite*: {expectation_suite_name}  <{validation_link}|View Results>"
            )
        else:
            summary_text += f"*Expectation Suite*: {expectation_suite_name}"

        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": summary_text,
            },
        }

    def concatenate_text_blocks(
        self,
        action_name: str,
        text_blocks: list[dict],
        success: bool,
        checkpoint_name: str,
        run_id: RunIdentifier,
    ) -> dict:
        all_blocks = [
            self._build_header(name=action_name, success=success, checkpoint_name=checkpoint_name)
        ]
        all_blocks.append(self._build_run_time_block(run_id=run_id))
        for block in text_blocks:
            all_blocks.append(block)
        all_blocks.append(self._build_divider())

        return {"blocks": all_blocks}

    def _build_header(self, name: str, success: bool, checkpoint_name: str) -> dict:
        status = "Success :white_check_mark:" if success else "Failure :no_entry:"
        return {
            "type": "header",
            "text": {"type": "plain_text", "text": f"{name} - {checkpoint_name} - {status}"},
        }

    def _build_run_time_block(self, run_id: RunIdentifier) -> dict:
        if run_id is not None:
            run_time = datetime.fromisoformat(str(run_id.run_time))
            formatted_run_time = run_time.strftime("%Y/%m/%d %I:%M %p")
        return {
            "type": "section",
            "text": {"type": "plain_text", "text": f"Runtime: {formatted_run_time}"},
        }

    def _build_divider(self) -> dict:
        return {"type": "divider"}

    def _build_footer(self) -> dict:
        documentation_url = "https://docs.greatexpectations.io/docs/terms/data_docs"
        return {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Learn how to review validation results in Data Docs: {documentation_url}",  # noqa: E501
                }
            ],
        }

    def _get_report_element(self, docs_link: str) -> dict[str, Any] | None:
        report_element = None
        if docs_link:
            try:
                # Slack does not allow links to local files due to security risks
                if "file://" in docs_link:
                    report_element = {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*DataDocs* can be found here: `{docs_link}` \n (Please copy and paste link into "  # noqa: E501
                            f"a browser to view)\n",
                        },
                    }
                else:
                    report_element = {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*DataDocs* can be found here: <{docs_link}|{docs_link}>",
                        },
                    }
            except Exception as e:
                logger.warning(
                    f"""SlackRenderer had a problem with generating the docs link.
                    link used to generate the docs link is: {docs_link} and is of type: {type(docs_link)}.
                    Error: {e}"""  # noqa: E501
                )
                return
        else:
            logger.warning("No docs link found. Skipping data docs link in Slack message.")
        return report_element

    def _build_report_element_block(
        self, data_docs_page: dict[str, str], notify_with: list[str]
    ) -> dict | None:
        if not data_docs_page:
            return None

        if notify_with:
            for docs_link_key in notify_with:
                if docs_link_key in data_docs_page:
                    docs_link = data_docs_page[docs_link_key]
                    report_element = self._get_report_element(docs_link)
                else:
                    logger.critical(
                        f"*ERROR*: Slack is trying to provide a link to the following DataDocs: `"
                        f"{docs_link_key!s}`, but it is not configured under `data_docs_sites` in the "  # noqa: E501
                        f"`great_expectations.yml`\n"
                    )
                    report_element = {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": f"*ERROR*: Slack is trying to provide a link to the following DataDocs: "  # noqa: E501
                            f"`{docs_link_key!s}`, but it is not configured under "
                            f"`data_docs_sites` in the `great_expectations.yml`\n",
                        },
                    }
                if report_element:
                    return report_element
        else:
            for docs_link_key in data_docs_page:
                if docs_link_key == "class":
                    continue
                docs_link = data_docs_page[docs_link_key]
                report_element = self._get_report_element(docs_link)
                return report_element

        return None
