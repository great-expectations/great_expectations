from __future__ import annotations

import copy
import json
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Union

import requests

import great_expectations.exceptions as gx_exceptions
from great_expectations.compatibility import aws
from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    RuntimeBatchRequest,
    get_batch_request_as_dict,
    materialize_batch_request,
)
from great_expectations.core.util import nested_update
from great_expectations.data_context.types.base import CheckpointValidationConfig
from great_expectations.types import DictDot

logger = logging.getLogger(__name__)


def send_slack_notification(
    query, slack_webhook=None, slack_channel=None, slack_token=None
):
    session = requests.Session()
    url = slack_webhook
    query = query
    headers = None

    # Slack doc about overwritting the channel when using the legacy Incoming Webhooks
    # https://api.slack.com/legacy/custom-integrations/messaging/webhooks
    # ** Since it is legacy, it could be deprecated or removed in the future **
    if slack_channel:
        query["channel"] = slack_channel

    if not slack_webhook:
        url = "https://slack.com/api/chat.postMessage"
        headers = {"Authorization": f"Bearer {slack_token}"}

    try:
        response = session.post(url=url, headers=headers, json=query)
        if slack_webhook:
            ok_status = response.text == "ok"
        else:
            ok_status = response.json()["ok"]
    except requests.ConnectionError:
        logger.warning(f"Failed to connect to Slack webhook after {10} retries.")
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 200 or not ok_status:  # noqa: PLR2004
            logger.warning(
                "Request to Slack webhook "
                f"returned error {response.status_code}: {response.text}"
            )

        else:
            return "Slack notification succeeded."


# noinspection SpellCheckingInspection
def send_opsgenie_alert(query, suite_name, settings):
    """Creates an alert in Opsgenie."""
    if settings["region"] is not None:
        url = f"https://api.{settings['region']}.opsgenie.com/v2/alerts"  # accommodate for Europeans
    else:
        url = "https://api.opsgenie.com/v2/alerts"

    headers = {"Authorization": f"GenieKey {settings['api_key']}"}
    payload = {
        "message": f"Great Expectations suite {suite_name} failed",
        "description": query,
        "priority": settings["priority"],  # allow this to be modified in settings
        "tags": settings["tags"],
    }

    session = requests.Session()

    try:
        response = session.post(url, headers=headers, json=payload)
    except requests.ConnectionError:
        logger.warning("Failed to connect to Opsgenie")
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 202:  # noqa: PLR2004
            logger.warning(
                "Request to Opsgenie API "
                f"returned error {response.status_code}: {response.text}"
            )
        else:
            return "success"
    return "error"


def send_microsoft_teams_notifications(query, microsoft_teams_webhook):
    session = requests.Session()
    try:
        response = session.post(url=microsoft_teams_webhook, json=query)
    except requests.ConnectionError:
        logger.warning("Failed to connect to Microsoft Teams webhook after 10 retries.")

    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 200:  # noqa: PLR2004
            logger.warning(
                "Request to Microsoft Teams webhook "
                f"returned error {response.status_code}: {response.text}"
            )
            return
        else:
            return "Microsoft Teams notification succeeded."


def send_webhook_notifications(query, webhook, target_platform):
    session = requests.Session()
    try:
        response = session.post(url=webhook, json=query)
    except requests.ConnectionError:
        logger.warning(
            f"Failed to connect to {target_platform} webhook after 10 retries."
        )
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 200:  # noqa: PLR2004
            logger.warning(
                f"Request to {target_platform} webhook "
                f"returned error {response.status_code}: {response.text}"
            )
        else:
            return f"{target_platform} notification succeeded."


# noinspection SpellCheckingInspection
def send_email(  # noqa: PLR0913
    title,
    html,
    smtp_address,
    smtp_port,
    sender_login,
    sender_password,
    sender_alias,
    receiver_emails_list,
    use_tls,
    use_ssl,
):
    msg = MIMEMultipart()
    msg["From"] = sender_alias
    msg["To"] = ", ".join(receiver_emails_list)
    msg["Subject"] = title
    msg.attach(MIMEText(html, "html"))
    try:
        if use_ssl:
            if use_tls:
                logger.warning("Please choose between SSL or TLS, will default to SSL")
            context = ssl.create_default_context()
            mailserver = smtplib.SMTP_SSL(smtp_address, smtp_port, context=context)
        elif use_tls:
            mailserver = smtplib.SMTP(smtp_address, smtp_port)
            context = ssl.create_default_context()
            mailserver.starttls(context=context)
        else:
            logger.warning("Not using TLS or SSL to send an email is not secure")
            mailserver = smtplib.SMTP(smtp_address, smtp_port)
        mailserver.login(sender_login, sender_password)
        mailserver.sendmail(sender_alias, receiver_emails_list, msg.as_string())
        mailserver.quit()
    except smtplib.SMTPConnectError:
        logger.error(f"Failed to connect to the SMTP server at address: {smtp_address}")
    except smtplib.SMTPAuthenticationError:
        logger.error(
            f"Failed to authenticate to the SMTP server at address: {smtp_address}"
        )
    except Exception as e:
        logger.error(str(e))
    else:
        return "success"


def get_substituted_validation_dict(
    substituted_runtime_config: dict, validation_dict: dict
) -> dict:
    substituted_validation_dict = {
        "batch_request": get_substituted_batch_request(
            substituted_runtime_config=substituted_runtime_config,
            validation_batch_request=validation_dict.get("batch_request"),
        ),
        "expectation_suite_name": validation_dict.get("expectation_suite_name")
        or substituted_runtime_config.get("expectation_suite_name"),
        "expectation_suite_ge_cloud_id": validation_dict.get(
            "expectation_suite_ge_cloud_id"
        )
        or substituted_runtime_config.get("expectation_suite_ge_cloud_id"),
        "action_list": get_updated_action_list(
            base_action_list=substituted_runtime_config.get("action_list", []),
            other_action_list=validation_dict.get("action_list", {}),
        ),
        "evaluation_parameters": nested_update(
            substituted_runtime_config.get("evaluation_parameters") or {},
            validation_dict.get("evaluation_parameters", {}),
            dedup=True,
        ),
        "runtime_configuration": nested_update(
            substituted_runtime_config.get("runtime_configuration") or {},
            validation_dict.get("runtime_configuration", {}),
            dedup=True,
        ),
        "include_rendered_content": validation_dict.get("include_rendered_content")
        or substituted_runtime_config.get("include_rendered_content")
        or None,
    }

    for attr in ("name", "id"):
        if validation_dict.get(attr) is not None:
            substituted_validation_dict[attr] = validation_dict[attr]

    return substituted_validation_dict


# TODO: <Alex>A common utility function should be factored out from DataContext.get_batch_list() for any purpose.</Alex>
def get_substituted_batch_request(
    substituted_runtime_config: dict,
    validation_batch_request: Optional[Union[BatchRequestBase, dict]] = None,
) -> Optional[Union[BatchRequest, RuntimeBatchRequest]]:
    substituted_runtime_batch_request = substituted_runtime_config.get("batch_request")

    if substituted_runtime_batch_request is None and validation_batch_request is None:
        return None

    if substituted_runtime_batch_request is None:
        substituted_runtime_batch_request = {}

    if validation_batch_request is None:
        validation_batch_request = {}

    validation_batch_request = get_batch_request_as_dict(
        batch_request=validation_batch_request
    )
    substituted_runtime_batch_request = get_batch_request_as_dict(
        batch_request=substituted_runtime_batch_request
    )

    for key, value in validation_batch_request.items():
        substituted_value = substituted_runtime_batch_request.get(key)
        if value is not None and substituted_value is not None:
            raise gx_exceptions.CheckpointError(
                f'BatchRequest attribute "{key}" was specified in both validation and top-level CheckpointConfig.'
            )

    effective_batch_request: dict = dict(
        **substituted_runtime_batch_request, **validation_batch_request
    )

    return materialize_batch_request(batch_request=effective_batch_request)  # type: ignore[return-value] # see materialize_batch_request


def substitute_template_config(  # noqa: PLR0912
    source_config: dict, template_config: dict
) -> dict:
    if not (template_config and any(template_config.values())):
        return source_config

    dest_config: dict = copy.deepcopy(template_config)

    # replace
    if source_config.get("name") is not None:
        dest_config["name"] = source_config["name"]
    if source_config.get("module_name") is not None:
        dest_config["module_name"] = source_config["module_name"]
    if source_config.get("class_name") is not None:
        dest_config["class_name"] = source_config["class_name"]
    if source_config.get("run_name_template") is not None:
        dest_config["run_name_template"] = source_config["run_name_template"]
    if source_config.get("expectation_suite_name") is not None:
        dest_config["expectation_suite_name"] = source_config["expectation_suite_name"]
    if source_config.get("expectation_suite_ge_cloud_id") is not None:
        dest_config["expectation_suite_ge_cloud_id"] = source_config[
            "expectation_suite_ge_cloud_id"
        ]

    # update
    if source_config.get("batch_request") is not None:
        batch_request = dest_config.get("batch_request") or {}
        updated_batch_request = nested_update(
            batch_request,
            source_config["batch_request"],
            dedup=True,
        )
        dest_config["batch_request"] = updated_batch_request
    if source_config.get("action_list") is not None:
        action_list = dest_config.get("action_list") or []
        dest_config["action_list"] = get_updated_action_list(
            base_action_list=action_list,
            other_action_list=source_config["action_list"],
        )
    if source_config.get("evaluation_parameters") is not None:
        evaluation_parameters = dest_config.get("evaluation_parameters") or {}
        updated_evaluation_parameters = nested_update(
            evaluation_parameters,
            source_config["evaluation_parameters"],
            dedup=True,
        )
        dest_config["evaluation_parameters"] = updated_evaluation_parameters
    if source_config.get("runtime_configuration") is not None:
        runtime_configuration = dest_config.get("runtime_configuration") or {}
        updated_runtime_configuration = nested_update(
            runtime_configuration,
            source_config["runtime_configuration"],
            dedup=True,
        )
        dest_config["runtime_configuration"] = updated_runtime_configuration
    if source_config.get("validations") is not None:
        validations = dest_config.get("validations") or []
        existing_validations = template_config.get("validations") or []
        validations.extend(
            filter(
                lambda v: v not in existing_validations, source_config["validations"]
            )
        )
        dest_config["validations"] = validations
    if source_config.get("profilers") is not None:
        profilers = dest_config.get("profilers") or []
        existing_profilers = template_config.get("profilers") or []
        profilers.extend(
            filter(lambda v: v not in existing_profilers, source_config["profilers"])
        )
        dest_config["profilers"] = profilers

    return dest_config


def substitute_runtime_config(source_config: dict, runtime_kwargs: dict) -> dict:
    if not (runtime_kwargs and any(runtime_kwargs.values())):
        return source_config

    dest_config: dict = copy.deepcopy(source_config)

    # replace
    if runtime_kwargs.get("template_name") is not None:
        dest_config["template_name"] = runtime_kwargs["template_name"]
    if runtime_kwargs.get("run_name_template") is not None:
        dest_config["run_name_template"] = runtime_kwargs["run_name_template"]
    if runtime_kwargs.get("expectation_suite_name") is not None:
        dest_config["expectation_suite_name"] = runtime_kwargs["expectation_suite_name"]
    if runtime_kwargs.get("expectation_suite_ge_cloud_id") is not None:
        dest_config["expectation_suite_ge_cloud_id"] = runtime_kwargs[
            "expectation_suite_ge_cloud_id"
        ]
    # update
    if runtime_kwargs.get("batch_request") is not None:
        batch_request = dest_config.get("batch_request") or {}
        batch_request_from_runtime_kwargs = runtime_kwargs["batch_request"]
        batch_request_from_runtime_kwargs = get_batch_request_as_dict(
            batch_request=batch_request_from_runtime_kwargs
        )

        # If "batch_request" has Fluent Datasource form, "options" must be overwritten for DataAsset type compatibility.
        updated_batch_request = copy.deepcopy(batch_request)
        if batch_request_from_runtime_kwargs and "options" in updated_batch_request:
            updated_batch_request["options"] = {}

        updated_batch_request = nested_update(
            updated_batch_request,
            batch_request_from_runtime_kwargs,
            dedup=True,
        )
        dest_config["batch_request"] = updated_batch_request
    if runtime_kwargs.get("action_list") is not None:
        action_list = dest_config.get("action_list") or []
        dest_config["action_list"] = get_updated_action_list(
            base_action_list=action_list,
            other_action_list=runtime_kwargs["action_list"],
        )
    if runtime_kwargs.get("evaluation_parameters") is not None:
        evaluation_parameters = dest_config.get("evaluation_parameters") or {}
        updated_evaluation_parameters = nested_update(
            evaluation_parameters,
            runtime_kwargs["evaluation_parameters"],
            dedup=True,
        )
        dest_config["evaluation_parameters"] = updated_evaluation_parameters
    if runtime_kwargs.get("runtime_configuration") is not None:
        runtime_configuration = dest_config.get("runtime_configuration") or {}
        updated_runtime_configuration = nested_update(
            runtime_configuration,
            runtime_kwargs["runtime_configuration"],
            dedup=True,
        )
        dest_config["runtime_configuration"] = updated_runtime_configuration
    if runtime_kwargs.get("validations") is not None:
        validations = dest_config.get("validations") or []
        existing_validations = source_config.get("validations") or []
        validations.extend(
            filter(
                lambda v: v not in existing_validations,
                runtime_kwargs["validations"],
            )
        )
        dest_config["validations"] = validations
    if runtime_kwargs.get("profilers") is not None:
        profilers = dest_config.get("profilers") or []
        existing_profilers = source_config.get("profilers") or []
        profilers.extend(
            filter(lambda v: v not in existing_profilers, runtime_kwargs["profilers"])
        )
        dest_config["profilers"] = profilers

    return dest_config


def get_updated_action_list(
    base_action_list: list, other_action_list: list
) -> List[dict]:
    if base_action_list is None:
        base_action_list = []

    base_action_list_dict = {action["name"]: action for action in base_action_list}

    for other_action in other_action_list:
        other_action_name = other_action["name"]
        if other_action_name in base_action_list_dict:
            if other_action["action"]:
                nested_update(
                    base_action_list_dict[other_action_name],
                    other_action,
                    dedup=True,
                )
            else:
                base_action_list_dict.pop(other_action_name)
        else:
            base_action_list_dict[other_action_name] = other_action

    for other_action in other_action_list:
        other_action_name = other_action["name"]
        if other_action_name in base_action_list_dict:
            if not other_action["action"]:
                base_action_list_dict.pop(other_action_name)

    return list(base_action_list_dict.values())


def does_batch_request_in_validations_contain_batch_data(
    validations: Optional[List[dict]] = None,
) -> bool:
    if validations is not None:
        for val in validations:
            if (
                val.get("batch_request") is not None
                and isinstance(val.get("batch_request"), (dict, DictDot))
                and val["batch_request"].get("runtime_parameters") is not None
                and val["batch_request"]["runtime_parameters"].get("batch_data")
                is not None
            ):
                return True

    return False


def get_validations_with_batch_request_as_dict(
    validations: list[dict] | list[CheckpointValidationConfig] | None = None,
) -> list[dict] | None:
    if not validations:
        return None

    validations = [
        v.to_dict() if isinstance(v, CheckpointValidationConfig) else v
        for v in validations
    ]
    for value in validations:
        if "batch_request" in value:
            value["batch_request"] = get_batch_request_as_dict(
                batch_request=value["batch_request"]
            )

    return validations


def validate_validation_dict(
    validation_dict: dict, batch_request_required: bool = True
) -> None:
    if batch_request_required and validation_dict.get("batch_request") is None:
        raise gx_exceptions.CheckpointError("validation batch_request cannot be None")

    if not validation_dict.get("expectation_suite_name"):
        raise gx_exceptions.CheckpointError(
            "validation expectation_suite_name must be specified"
        )

    if not validation_dict.get("action_list"):
        raise gx_exceptions.CheckpointError("validation action_list cannot be empty")


def send_sns_notification(
    sns_topic_arn: str, sns_subject: str, validation_results: str, **kwargs
) -> str:
    """
    Send JSON results to an SNS topic with a schema of:


    :param sns_topic_arn:  The SNS Arn to publish messages to
    :param sns_subject: : The SNS Message Subject - defaults to expectation_suite_identifier.expectation_suite_name
    :param validation_results:  The results of the validation ran
    :param kwargs:  Keyword arguments to pass to the boto3 Session
    :return:  Message ID that was published or error message

    """
    if not aws.boto3:
        logger.warning("boto3 is not installed")
        return "boto3 is not installed"

    message_dict = {
        "TopicArn": sns_topic_arn,
        "Subject": sns_subject,
        "Message": json.dumps(validation_results),
        "MessageAttributes": {
            "String": {"DataType": "String.Array", "StringValue": "ValidationResults"},
        },
        "MessageStructure": "json",
    }
    session = aws.boto3.Session(**kwargs)
    sns = session.client("sns")
    try:
        response = sns.publish(**message_dict)
    except sns.exceptions.InvalidParameterException:
        error_msg = f"Received invalid for message: {validation_results}"
        logger.error(error_msg)
        return error_msg
    else:
        return f"Successfully posted results to {response['MessageId']} with Subject {sns_subject}"
