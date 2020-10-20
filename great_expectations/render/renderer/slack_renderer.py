import logging

from great_expectations.exceptions import InvalidKeyError

logger = logging.getLogger(__name__)

from ...core.id_dict import BatchKwargs
from .renderer import Renderer


class SlackRenderer(Renderer):
    def __init__(self):
        super().__init__()

    def render(
        self, validation_result=None, data_docs_pages=None, notify_with=None,
    ):

        default_text = (
            "No validation occurred. Please ensure you passed a validation_result."
        )
        status = "Failed :x:"

        title_block = {
            "type": "section",
            "text": {"type": "mrkdwn", "text": default_text,},
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
            else:
                data_asset_name = "__no_data_asset_name__"

            n_checks_succeeded = validation_result.statistics["successful_expectations"]
            n_checks = validation_result.statistics["evaluated_expectations"]
            run_id = validation_result.meta.get("run_id", "__no_run_id__")
            batch_id = BatchKwargs(
                validation_result.meta.get("batch_kwargs", {})
            ).to_id()
            check_details_text = "*{}* of *{}* expectations were met".format(
                n_checks_succeeded, n_checks
            )

            if validation_result.success:
                status = "Success :tada:"

            summary_text = """*Batch Validation Status*: {}
*Expectation suite name*: `{}`
*Data asset name*: `{}`
*Run ID*: `{}`
*Batch ID*: `{}`
*Summary*: {}""".format(
                status,
                expectation_suite_name,
                data_asset_name,
                run_id,
                batch_id,
                check_details_text,
            )
            query["blocks"][0]["text"]["text"] = summary_text
            # this abbreviated root level "text" will show up in the notification and not the message
            query["text"] = "{}: {}".format(expectation_suite_name, status)

            if data_docs_pages:
                if notify_with is not None:
                    for docs_link_key in notify_with:
                        if docs_link_key in data_docs_pages.keys():
                            docs_link = data_docs_pages[docs_link_key]
                            report_element = self._get_report_element(docs_link)
                        else:
                            logger.critical(
                                f"*ERROR*: Slack is trying to provide a link to the following DataDocs: `{str(docs_link_key)}`, but it is not configured under `data_docs_sites` in the `great_expectations.yml`\n"
                            )
                            report_element = {
                                "type": "section",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": f"*ERROR*: Slack is trying to provide a link to the following DataDocs: `{str(docs_link_key)}`, but it is not configured under `data_docs_sites` in the `great_expectations.yml`\n",
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
                report_element = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "- *Validation Report*: {}".format(
                            validation_result.meta["result_reference"]
                        ),
                    },
                }
                query["blocks"].append(report_element)

            if "dataset_reference" in validation_result.meta:
                dataset_element = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "- *Validation data asset*: {}".format(
                            validation_result.meta["dataset_reference"]
                        ),
                    },
                }
                query["blocks"].append(dataset_element)

        custom_blocks = self._custom_blocks(evr=validation_result)
        if custom_blocks:
            query["blocks"].append(custom_blocks)

        documentation_url = "https://docs.greatexpectations.io/en/latest/guides/tutorials/getting_started/set_up_data_docs.html"
        footer_section = {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Learn how to review validation results in Data Docs: {}".format(
                        documentation_url
                    ),
                }
            ],
        }

        divider_block = {"type": "divider"}
        query["blocks"].append(divider_block)
        query["blocks"].append(footer_section)
        return query

    def _custom_blocks(self, evr):
        return None

    def _get_report_element(self, docs_link):
        if "file:///" in docs_link:
            # handle special case since Slack does not render these links
            report_element = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*DataDocs* can be found here: `{}` \n (Please copy and paste link into a browser to view)\n".format(
                        docs_link
                    ),
                },
            }
        else:
            report_element = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*DataDocs* can be found here: <{}|{}>".format(
                        docs_link, docs_link
                    ),
                },
            }
        return report_element
