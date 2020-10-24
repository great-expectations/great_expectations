import logging

logger = logging.getLogger(__name__)

from ...core.id_dict import BatchKwargs
from .renderer import Renderer


class MicrosoftTeamsRenderer(Renderer):
    def __init__(self):
        super().__init__()

    def render(
        self, validation_result=None, data_docs_pages=None,
    ):

        status = "Failed :("

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
            run_name = validation_result.meta["run_id"].get(
                "run_name", "__no_run_name_"
            )
            run_time = validation_result.meta["run_id"].get(
                "run_time", "__no_run_time_"
            )
            batch_id = BatchKwargs(
                validation_result.meta.get("batch_kwargs", {})
            ).to_id()
            check_details_text = "*{}* of *{}* expectations were met".format(
                n_checks_succeeded, n_checks
            )

            if validation_result.success:
                status = "Success !!!"

            summary_text = """*Batch validation status*: {}
*Data asset name*: `{}`
*Expectation suite name*: `{}`
*Run name*: `{}`
*Batch ID*: `{}`
*Summary*: {}""".format(
                status,
                data_asset_name,
                expectation_suite_name,
                run_name,
                batch_id,
                check_details_text,
            )

            query = {
                "type": "message",
                "attachments": [
                    {
                        "contentType": "application/vnd.microsoft.card.adaptive",
                        "content": {
                            "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                            "type": "AdaptiveCard",
                            "version": "1.0",
                            "body": [
                                {
                                    "type": "Container",
                                    "separator": True,
                                    "items": [
                                        {
                                            "type": "TextBlock",
                                            "text": "Great expectations validation results",
                                            "weight": "bolder",
                                            "size": "medium",
                                        },
                                        {
                                            "type": "ColumnSet",
                                            "columns": [
                                                {
                                                    "type": "Column",
                                                    "width": "stretch",
                                                    "items": [
                                                        {
                                                            "type": "TextBlock",
                                                            "text": "Validation results",
                                                            "weight": "bolder",
                                                            "size": "medium",
                                                        },
                                                        {
                                                            "type": "TextBlock",
                                                            "spacing": "none",
                                                            "text": "{{DATE(2017-02-14T06:08:39Z, SHORT)}}",
                                                            "isSubtle": True,
                                                            "wrap": True,
                                                        },
                                                    ],
                                                }
                                            ],
                                        },
                                    ],
                                },
                                {"type": "Container", "separator": True, "items": [{}, ], },
                            ],
                            "actions": [],
                        },
                    }
                ],
            }

            if data_docs_pages:
                for docs_link_key in data_docs_pages.keys():
                    if docs_link_key == "class":
                        continue
                    docs_link = data_docs_pages[docs_link_key]
                    report_element = self._get_report_element(docs_link)
                    if report_element:
                        query["attachments"][0]["content"]["actions"].append(
                            report_element
                        )

        return query

    @staticmethod
    def _get_report_element(docs_link):
        report_element = {
            "type": "Action.OpenUrl",
            "title": "Open data docs",
            "url": docs_link,
        }
        return report_element
