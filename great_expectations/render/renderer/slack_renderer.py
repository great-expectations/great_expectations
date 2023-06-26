from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

from great_expectations.core.id_dict import BatchKwargs
from great_expectations.render.renderer.renderer import Renderer


class SlackRenderer(Renderer):
    def __init__(self) -> None:
        super().__init__()

    def render(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        validation_result=None,
        data_docs_pages=None,
        notify_with=None,
        show_failed_expectations: bool = False,
        validation_result_urls: list[str] | None = None,
    ):
        if validation_result_urls is None:
            validation_result_urls = []

        default_text = (
            "No validation occurred. Please ensure you passed a validation_result."
        )
        status = "Failed :x:"

        failed_expectations_text = ""

        title_block = {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": default_text,
            },
        }

        query = {
            "blocks": [title_block],
            # this abbreviated root level "text" will show up in the notification and not the message
            "text": default_text,
        }

        if validation_result:
            expectation_suite_name = validation_result.meta.get(
                "expectation_suite_name", "__no_expectation_suite_name__"
            )

            if "batch_kwargs" in validation_result.meta:
                data_asset_name = validation_result.meta["batch_kwargs"].get(
                    "data_asset_name", "__no_data_asset_name__"
                )
            elif "active_batch_definition" in validation_result.meta:
                data_asset_name = (
                    validation_result.meta["active_batch_definition"].data_asset_name
                    if validation_result.meta["active_batch_definition"].data_asset_name
                    else "__no_data_asset_name__"
                )
            else:
                data_asset_name = "__no_data_asset_name__"

            n_checks_succeeded = validation_result.statistics["successful_expectations"]
            n_checks = validation_result.statistics["evaluated_expectations"]
            run_id = validation_result.meta.get("run_id", "__no_run_id__")
            batch_id = BatchKwargs(
                validation_result.meta.get("batch_kwargs", {})
            ).to_id()
            check_details_text = (
                f"*{n_checks_succeeded}* of *{n_checks}* expectations were met"
            )

            if validation_result.success:
                status = "Success :tada:"

            else:
                if show_failed_expectations:  # noqa: PLR5501
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
*Expectation suite name*: `{expectation_suite_name}`
*Data asset name*: `{data_asset_name}`
*Run ID*: `{run_id}`
*Batch ID*: `{batch_id}`
*Summary*: {check_details_text}"""

            if failed_expectations_text:
                summary_text += failed_expectations_text

            query["blocks"][0]["text"]["text"] = summary_text
            # this abbreviated root level "text" will show up in the notification and not the message
            query["text"] = f"{expectation_suite_name}: {status}"

            if data_docs_pages:
                if notify_with is not None:
                    for docs_link_key in notify_with:
                        if docs_link_key in data_docs_pages.keys():
                            docs_link = data_docs_pages[docs_link_key]
                            report_element = self._get_report_element(docs_link)
                        else:
                            logger.critical(
                                f"*ERROR*: Slack is trying to provide a link to the following DataDocs: `"
                                f"{str(docs_link_key)}`, but it is not configured under `data_docs_sites` in the "
                                f"`great_expectations.yml`\n"
                            )
                            report_element = {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": f"*ERROR*: Slack is trying to provide a link to the following DataDocs: "
                                    f"`{str(docs_link_key)}`, but it is not configured under "
                                    f"`data_docs_sites` in the `great_expectations.yml`\n",
                                },
                            }
                        if report_element:
                            query["blocks"].append(report_element)
                else:
                    for docs_link_key in data_docs_pages.keys():
                        if docs_link_key == "class":
                            continue
                        docs_link = data_docs_pages[docs_link_key]
                        report_element = self._get_report_element(docs_link)
                        if report_element:
                            query["blocks"].append(report_element)

            if "result_reference" in validation_result.meta:
                result_reference = validation_result.meta["result_reference"]
                report_element = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"- *Validation Report*: {result_reference}",
                    },
                }
                query["blocks"].append(report_element)

            if "dataset_reference" in validation_result.meta:
                dataset_reference = validation_result.meta["dataset_reference"]
                dataset_element = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"- *Validation data asset*: {dataset_reference}",
                    },
                }
                query["blocks"].append(dataset_element)

        documentation_url = "https://docs.greatexpectations.io/docs/terms/data_docs"
        footer_section = {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Learn how to review validation results in Data Docs: {documentation_url}",
                }
            ],
        }

        divider_block = {"type": "divider"}
        query["blocks"].append(divider_block)
        query["blocks"].append(footer_section)
        return query

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
                            "text": f"*DataDocs* can be found here: `{docs_link}` \n (Please copy and paste link into "
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
                    Error: {e}"""
                )
                return
        else:
            logger.warning(
                "No docs link found. Skipping data docs link in Slack message."
            )
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

    def create_failed_expectation_text(
        self, expectation_kwargs, expectation_name
    ) -> str:
        expectation_entity = self.get_failed_expectation_domain(
            expectation_name, expectation_kwargs
        )
        if expectation_entity:
            return f":x:{expectation_name} ({expectation_entity})\n"
        return f":x:{expectation_name}\n"

    @staticmethod
    def get_failed_expectation_domain(
        expectation_name, expectation_config_kwargs: dict
    ) -> str:
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
