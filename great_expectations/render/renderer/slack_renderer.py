import datetime

from ...core.id_dict import BatchKwargs
from .renderer import Renderer


class SlackRenderer(Renderer):
    def __init__(self):
        super().__init__()

    def render(self, validation_result=None):
        # Defaults
        timestamp = datetime.datetime.strftime(datetime.datetime.now(), "%x %X")
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

        # TODO improve this nested logic
        if validation_result:
            expectation_suite_name = validation_result.meta.get(
                "expectation_suite_name", "__no_expectation_suite_name__"
            )

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
*Run ID*: `{}`
*Batch ID*: `{}`
*Timestamp*: `{}`
*Summary*: {}""".format(
                status,
                expectation_suite_name,
                run_id,
                batch_id,
                timestamp,
                check_details_text,
            )
            query["blocks"][0]["text"]["text"] = summary_text
            # this abbreviated root level "text" will show up in the notification and not the message
            query["text"] = "{}: {}".format(expectation_suite_name, status)

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

        documentation_url = "https://docs.greatexpectations.io/en/latest/features/validation.html#reviewing-validation-results"
        footer_section = {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Learn how to review validation results: {}".format(
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
