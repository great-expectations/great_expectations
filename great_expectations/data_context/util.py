import requests
import datetime
import uuid
import logging
import os
import json
import errno
from collections import namedtuple

logger = logging.getLogger(__name__)

NormalizedDataAssetName = namedtuple("NormalizedDataAssetName", [
    "datasource",
    "generator",
    "generator_asset"
])


def build_slack_notification_request(validation_json=None):
    # Defaults
    timestamp = datetime.datetime.strftime(datetime.datetime.now(), "%x %X")
    status = "Failed :x:"
    run_id = None

    title_block = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "No validation occurred. Please ensure you passed a validation_json.",
        },
    }

    query = {"blocks": [title_block]}

    if validation_json:
        if "meta" in validation_json:
            data_asset_name = validation_json["meta"].get(
                "data_asset_name", "no_name_provided_" + datetime.datetime.utcnow().isoformat().replace(":", "") + "Z"
            )
            expectation_suite_name = validation_json["meta"].get("expectation_suite_name", "default")

        n_checks_succeeded = validation_json["statistics"]["successful_expectations"]
        n_checks = validation_json["statistics"]["evaluated_expectations"]
        run_id = validation_json["meta"].get("run_id", None)
        check_details_text = "{} of {} expectations were met\n\n".format(
            n_checks_succeeded, n_checks)

        if validation_json["success"]:
            status = "Success :tada:"

        query["blocks"][0]["text"]["text"] = "*Validated batch from data asset:* `{}`\n*Status: {}*\n{}".format(
            data_asset_name, status, check_details_text)
        if "batch_kwargs" in validation_json["meta"]:
            batch_kwargs = {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Batch kwargs: {}".format(
                json.dumps(validation_json["meta"]["batch_kwargs"], indent=2))
                }
            }
            query["blocks"].append(batch_kwargs)

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

    footer_section = {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "Great Expectations run id {} ran at {}".format(run_id, timestamp),
            }
        ],
    }
    query["blocks"].append(footer_section)
    return query


def get_slack_callback(webhook):
    def send_slack_notification(validation_json=None):
        """Post a slack notification."""
        session = requests.Session()
        query = build_slack_notification_request(validation_json)

        try:
            response = session.post(url=webhook, json=query)
        except requests.ConnectionError:
            logger.warning(
                'Failed to connect to Slack webhook at {url} '
                'after {max_retries} retries.'.format(
                    url=webhook, max_retries=10))
        except Exception as e:
            logger.error(str(e))
        else:
            if response.status_code != 200:
                logger.warning(
                    'Request to Slack webhook at {url} '
                    'returned error {status_code}: {text}'.format(
                        url=webhook,
                        status_code=response.status_code,
                        text=response.text))
    return send_slack_notification


def safe_mmkdir(directory, exist_ok=True):
    """Simple wrapper since exist_ok is not available in python 2"""
    if not exist_ok:
        raise ValueError(
            "This wrapper should only be used for exist_ok=True; it is designed to make porting easier later")
    try:
        os.makedirs(directory)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
