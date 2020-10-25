import logging

logger = logging.getLogger(__name__)

from ...core.id_dict import BatchKwargs
from .renderer import Renderer


class MicrosoftTeamsRenderer(Renderer):

    MICROSOFT_TEAMS_SCHEMA_URL = "http://adaptivecards.io/schemas/adaptive-card.json"

    def __init__(self):
        super().__init__()

    def render(
        self, validation_result=None, data_docs_pages=None,
    ):
        default_text = (
            "No validation occurred. Please ensure you passed a validation_result."
        )

        status = "Failed :("

        query = {
            "type": "message",
            "attachments": [
                {
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": {
                        "$schema": self.MICROSOFT_TEAMS_SCHEMA_URL,
                        "type": "AdaptiveCard",
                        "version": "1.0",
                        "body": [
                            {
                                "type": "Container",
                                "height": "auto",
                                "separator": True,
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
                                                        "text": "Validation results",
                                                        "weight": "bolder",
                                                        "size": "large",
                                                        "wrap": True,
                                                    },
                                                ],
                                            }
                                        ],
                                    },
                                ],
                            },
                            {
                                "type": "Container",
                                "height": "auto",
                                "separator": True,
                                "items": [
                                    {
                                        "type": "TextBlock",
                                        "text": default_text,
                                        "horizontalAlignment": "left",
                                    }
                                ],
                            },
                        ],
                        "actions": [],
                    },
                }
            ],
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

            status_element = self._get_validation_result_element(
                key="Batch validation status",
                value=status,
                validation_result=validation_result,
            )
            data_asset_name_element = self._get_validation_result_element(
                key="Data asset name", value=data_asset_name
            )
            expectation_suite_name_element = self._get_validation_result_element(
                key="Expectation suite name", value=expectation_suite_name
            )
            run_name_element = self._get_validation_result_element(
                key="Run name", value=run_name
            )
            batch_id_element = self._get_validation_result_element(
                key="Batch ID", value=batch_id
            )
            check_details_text_element = self._get_validation_result_element(
                key="Summary", value=check_details_text
            )

            query["attachments"][0]["content"]["body"][0]["items"][0]["columns"][0][
                "items"
            ][1] = {
                "type": "TextBlock",
                "spacing": "none",
                "text": "{{DATE({run_time}, SHORT)}}".format(run_time=run_time),
                "isSubtle": True,
                "wrap": True,
            }

            query["attachments"][0]["content"]["body"][1]["items"] = [
                status_element,
                data_asset_name_element,
                expectation_suite_name_element,
                run_name_element,
                batch_id_element,
                check_details_text_element,
            ]

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

    @staticmethod
    def _get_validation_result_element(key, value, validation_result=None):

        if validation_result and validation_result.success:
            validation_result_element = {
                "type": "TextBlock",
                "text": "**{key}}:** {value}".format(key=key, value=value),
                "horizontalAlignment": "left",
                "color": "good",
            }
        elif validation_result and not validation_result.success:
            validation_result_element = {
                "type": "TextBlock",
                "text": "**{key}}:** {value}".format(key=key, value=value),
                "horizontalAlignment": "left",
                "color": "attention",
            }
        else:
            validation_result_element = {
                "type": "TextBlock",
                "text": "**{key}}:** {value}".format(key=key, value=value),
                "horizontalAlignment": "left",
            }

        return validation_result_element
