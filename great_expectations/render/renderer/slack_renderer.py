import datetime

from .renderer import Renderer


class SlackRenderer(Renderer):
    
    def __init__(self):
        pass
    
    def render(self, validation_json=None):
        # Defaults
        timestamp = datetime.datetime.strftime(datetime.datetime.now(), "%x %X")
        default_text = "No validation occurred. Please ensure you passed a validation_json."
        status = "Failed :x:"

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
            "text": default_text
        }

        # TODO improve this nested logic
        expectation_suite_name = None
        data_asset_name = None
        if validation_json:
            if "meta" in validation_json:
                data_asset_name = validation_json["meta"].get(
                    "data_asset_name",
                    "no_name_provided_" + datetime.datetime.utcnow().isoformat().replace(":", "") + "Z"
                )
                expectation_suite_name = validation_json["meta"].get("expectation_suite_name", "default")
        
            n_checks_succeeded = validation_json["statistics"]["successful_expectations"]
            n_checks = validation_json["statistics"]["evaluated_expectations"]
            run_id = validation_json["meta"].get("run_id", None)
            check_details_text = "*{}* of *{}* expectations were met".format(
                n_checks_succeeded, n_checks)
        
            if validation_json["success"]:
                status = "Success :tada:"

            summary_text = """*Batch Validation Status*: {}
*Data Asset:* `{}`
*Expectation suite name*: `{}`
*Run ID*: `{}`
*Timestamp*: `{}`
*Summary*: {}""".format(
                status,
                data_asset_name,
                expectation_suite_name,
                run_id,
                timestamp,
                check_details_text
            )
            query["blocks"][0]["text"]["text"] = summary_text
            # this abbreviated root level "text" will show up in the notification and not the message
            query["text"] = "{}: {}".format(data_asset_name, status)

            if "result_reference" in validation_json["meta"]:
                report_element = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "- *Validation Report*: {}".format(validation_json["meta"]["result_reference"])},
                }
                query["blocks"].append(report_element)
        
            if "dataset_reference" in validation_json["meta"]:
                dataset_element = {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "- *Validation data asset*: {}".format(validation_json["meta"]["dataset_reference"])
                    },
                }
                query["blocks"].append(dataset_element)

        custom_blocks = self._custom_blocks(evr=validation_json)
        if custom_blocks:
            query["blocks"].append(custom_blocks)

        documentation_url = "https://docs.greatexpectations.io/en/latest/features/validation.html#reviewing-validation-results"
        footer_section = {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Learn how to review validation results: {}".format(documentation_url),
                }
            ],
        }

        divider_block = {"type": "divider"}
        query["blocks"].append(divider_block)
        query["blocks"].append(footer_section)
        return query

    def _custom_blocks(self, evr):
        return None
