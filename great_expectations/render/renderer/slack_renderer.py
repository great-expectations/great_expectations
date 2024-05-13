from __future__ import annotations

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

from great_expectations.render.renderer.renderer import Renderer

if TYPE_CHECKING:
    from great_expectations.core.expectation_validation_result import (
        ExpectationSuiteValidationResult,
    )


class SlackRenderer(Renderer):
    def render(  # noqa: PLR0913
        self,
        validation_result: ExpectationSuiteValidationResult,
        data_docs_pages: list[dict] | None = None,
        notify_with: list[str] | None = None,
        show_failed_expectations: bool = False,
        validation_result_urls: list[str] | None = None,
    ) -> list[dict]:
        data_docs_pages = data_docs_pages or []
        notify_with = notify_with or []
        validation_result_urls = validation_result_urls or []

        blocks: list[dict] = []

        title_block = self._build_title_block(
            validation_result=validation_result,
            show_failed_expectations=show_failed_expectations,
            validation_result_urls=validation_result_urls,
        )
        blocks.append(title_block)

        report_element_block = self._build_report_element_block(
            data_docs_pages=data_docs_pages, notify_with=notify_with
        )
        if report_element_block:
            blocks.append(report_element_block)

        return blocks

    def _build_title_block(
        self,
        validation_result: ExpectationSuiteValidationResult,
        show_failed_expectations: bool,
        validation_result_urls: list[str],
    ) -> dict:
        expectation_suite_name = validation_result.suite_name
        data_asset_name = validation_result.asset_name or "__no_data_asset_name__"
        n_checks_succeeded = validation_result.statistics["successful_expectations"]
        n_checks = validation_result.statistics["evaluated_expectations"]
        run_id = validation_result.meta.get("run_id", "__no_run_id__")
        batch_id = validation_result.batch_id
        check_details_text = f"*{n_checks_succeeded}* of *{n_checks}* expectations were met"

        failed_expectations_text = ""

        status = "Failed :x:"
        if validation_result.success:
            status = "Success :tada:"
        else:  # noqa: PLR5501
            if show_failed_expectations:
                failed_expectations_text = self.create_failed_expectations_text(
                    validation_result["results"]
                )

        summary_text = ""
        if validation_result_urls:
            # This adds hyperlinks for defined URL
            if len(validation_result_urls) == 1:
                title_hlink = f"*<{validation_result_urls[0]} | Validation Result>*"
            else:
                title_hlink = "*Validation Result*"
            batch_validation_status_hlinks = "".join(
                f"*Batch Validation Status*: *<{validation_result_url} | {status}>*"
                for validation_result_url in validation_result_urls
            )
            summary_text += f"""{title_hlink}
{batch_validation_status_hlinks}
            """
        else:
            summary_text += f"*Batch Validation Status*: {status}"

        summary_text += f"""
*Expectation Suite Name*: `{expectation_suite_name}`
*Data Asset Name*: `{data_asset_name}`
*Run ID*: `{run_id}`
*Batch ID*: `{batch_id}`
*Summary*: {check_details_text}"""

        if failed_expectations_text:
            summary_text += failed_expectations_text

        return {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": summary_text,
            },
        }

    def _build_report_element_block(
        self, data_docs_pages: list[dict], notify_with: list[str]
    ) -> dict | None:
        if not data_docs_pages:
            return None

        if notify_with:
            for docs_link_key in notify_with:
                if docs_link_key in data_docs_pages:
                    docs_link = data_docs_pages[docs_link_key]
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
            for docs_link_key in data_docs_pages:
                if docs_link_key == "class":
                    continue
                docs_link = data_docs_pages[docs_link_key]
                report_element = self._get_report_element(docs_link)
                return report_element

        return None

    def concatenate_text_blocks(self, text_blocks: list[dict], name: str, success: bool) -> dict:
        all_blocks = [self._build_header(name=name, success=success), self._build_divider()]
        for block in text_blocks:
            all_blocks.append(block)
            all_blocks.append(self._build_divider())
        all_blocks.append(self._build_footer())

        return {"blocks": all_blocks, "text": name}

    def _build_header(self, name: str, success: bool) -> dict:
        status = "Success :tada:" if success else "Failed :x:"
        return {"type": "header", "text": {"type": "plain_text", "text": f"{name} - {status}"}}

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

    def _get_report_element(self, docs_link):
        report_element = None
        if docs_link:
            try:
                if "file://" in docs_link:
                    # handle special case since Slack does not render these links
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

    def create_failed_expectations_text(self, validation_results: list[dict]) -> str:
        failed_expectations_str = "\n*Failed Expectations*:\n"
        for expectation in validation_results:
            if not expectation["success"]:
                expectation_name = expectation["expectation_config"]["expectation_type"]
                expectation_kwargs = expectation["expectation_config"]["kwargs"]
                failed_expectations_str += self.create_failed_expectation_text(
                    expectation_kwargs, expectation_name
                )
        return failed_expectations_str

    def create_failed_expectation_text(self, expectation_kwargs, expectation_name) -> str:
        expectation_entity = self.get_failed_expectation_domain(
            expectation_name, expectation_kwargs
        )
        if expectation_entity:
            return f":x:{expectation_name} ({expectation_entity})\n"
        return f":x:{expectation_name}\n"

    @staticmethod
    def get_failed_expectation_domain(expectation_name, expectation_config_kwargs: dict) -> str:
        if "expect_table_" in expectation_name:
            return "Table"

        column_name, column_a, column_b, column_list = (
            expectation_config_kwargs.get("column"),
            expectation_config_kwargs.get("column_A"),
            expectation_config_kwargs.get("column_B"),
            expectation_config_kwargs.get("column_list"),
        )
        if column_name:
            return column_name
        elif column_a and column_b:
            return f"{column_a}, {column_b}"
        elif column_list:
            return str(column_list)
