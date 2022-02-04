import copy
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional, Union

import requests

import great_expectations.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchRequest,
    RuntimeBatchRequest,
    get_batch_request_as_dict,
)
from great_expectations.core.util import nested_update, safe_deep_copy
from great_expectations.util import filter_properties_dict

logger = logging.getLogger(__name__)


def send_slack_notification(
    query, slack_webhook=None, slack_channel=None, slack_token=None
):
    session = requests.Session()
    url = slack_webhook
    query = query
    headers = None

    if not slack_webhook:
        url = "https://slack.com/api/chat.postMessage"
        headers = {"Authorization": f"Bearer {slack_token}"}
        query["channel"] = slack_channel

    try:
        response = session.post(url=url, headers=headers, json=query)
    except requests.ConnectionError:
        logger.warning(
            "Failed to connect to Slack webhook at {url} "
            "after {max_retries} retries.".format(url=slack_webhook, max_retries=10)
        )
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 200:
            logger.warning(
                "Request to Slack webhook at {url} "
                "returned error {status_code}: {text}".format(
                    url=slack_webhook,
                    status_code=response.status_code,
                    text=response.text,
                )
            )

        else:
            return "Slack notification succeeded."


# noinspection SpellCheckingInspection
def send_opsgenie_alert(query, suite_name, settings):
    """Creates an alert in Opsgenie."""
    if settings["region"] is not None:
        url = "https://api.{region}.opsgenie.com/v2/alerts".format(
            region=settings["region"]
        )  # accommodate for Europeans
    else:
        url = "https://api.opsgenie.com/v2/alerts"

    headers = {
        "Authorization": "GenieKey {api_key}".format(api_key=settings["api_key"])
    }
    payload = {
        "message": "Great Expectations suite {suite_name} failed".format(
            suite_name=suite_name
        ),
        "description": query,
        "priority": settings["priority"],  # allow this to be modified in settings
    }

    session = requests.Session()

    try:
        response = session.post(url, headers=headers, json=payload)
    except requests.ConnectionError:
        logger.warning("Failed to connect to Opsgenie")
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 202:
            logger.warning(
                "Request to Opsgenie API at {url} "
                "returned error {status_code}: {text}".format(
                    url=url,
                    status_code=response.status_code,
                    text=response.text,
                )
            )
        else:
            return "success"
    return "error"


def send_microsoft_teams_notifications(query, microsoft_teams_webhook):
    session = requests.Session()
    try:
        response = session.post(url=microsoft_teams_webhook, json=query)
    except requests.ConnectionError:
        logger.warning(
            "Failed to connect to Microsoft Teams webhook at {url} "
            "after {max_retries} retries.".format(
                url=microsoft_teams_webhook, max_retries=10
            )
        )
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 200:
            logger.warning(
                "Request to Microsoft Teams webhook at {url} "
                "returned error {status_code}: {text}".format(
                    url=microsoft_teams_webhook,
                    status_code=response.status_code,
                    text=response.text,
                )
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
            "Failed to connect to {target_platform} webhook at {url} "
            "after {max_retries} retries.".format(
                url=webhook,
                max_retries=10,
                target_platform=target_platform,
            )
        )
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 200:
            logger.warning(
                "Request to {target_platform} webhook at {url} "
                "returned error {status_code}: {text}".format(
                    url=webhook,
                    status_code=response.status_code,
                    target_platform=target_platform,
                    text=response.text,
                )
            )
        else:
            return "{target_platform} notification succeeded.".format(
                target_platform=target_platform
            )


# noinspection SpellCheckingInspection
def send_email(
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
            base_action_list=substituted_runtime_config.get("action_list"),
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
    }
    if validation_dict.get("name") is not None:
        substituted_validation_dict["name"] = validation_dict["name"]
    validate_validation_dict(substituted_validation_dict)
    return substituted_validation_dict


# TODO: <Alex>A common utility function should be factored out from DataContext.get_batch_list() for any purpose.</Alex>
def get_substituted_batch_request(
    substituted_runtime_config: dict,
    validation_batch_request: Optional[
        Union[BatchRequest, RuntimeBatchRequest, dict]
    ] = None,
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

    effective_batch_request: dict = dict(
        **substituted_runtime_batch_request, **validation_batch_request
    )

    batch_request_class: type

    if "runtime_parameters" in effective_batch_request:
        batch_request_class = RuntimeBatchRequest
    else:
        batch_request_class = BatchRequest

    for key, value in validation_batch_request.items():
        substituted_value = substituted_runtime_batch_request.get(key)
        if value is not None and substituted_value is not None:
            raise ge_exceptions.CheckpointError(
                f'BatchRequest attribute "{key}" was specified in both validation and top-level CheckpointConfig.'
            )

    # noinspection PyUnresolvedReferences
    filter_properties_dict(
        properties=effective_batch_request,
        keep_fields=batch_request_class.field_names,
        clean_nulls=False,
        inplace=True,
    )

    return batch_request_class(**effective_batch_request)


def substitute_template_config(source_config: dict, template_config: dict) -> dict:
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
        batch_request_from_runtime_kwargs = safe_deep_copy(
            data=batch_request_from_runtime_kwargs
        )
        if isinstance(
            batch_request_from_runtime_kwargs, (BatchRequest, RuntimeBatchRequest)
        ):
            # noinspection PyUnresolvedReferences
            batch_request_from_runtime_kwargs = (
                batch_request_from_runtime_kwargs.to_dict()
            )
        updated_batch_request = nested_update(
            batch_request,
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


def batch_request_contains_batch_data(
    batch_request: Optional[Union[BatchRequest, RuntimeBatchRequest, dict]] = None
) -> bool:
    return (
        batch_request is not None
        and batch_request.get("runtime_parameters") is not None
        and batch_request["runtime_parameters"].get("batch_data") is not None
    )


def batch_request_in_validations_contains_batch_data(
    validations: Optional[List[dict]] = None,
) -> bool:
    if validations is not None:
        for idx, val in enumerate(validations):
            if (
                val.get("batch_request") is not None
                and val["batch_request"].get("runtime_parameters") is not None
                and val["batch_request"]["runtime_parameters"].get("batch_data")
                is not None
            ):
                return True

    return False


def get_validations_with_batch_request_as_dict(
    validations: Optional[list] = None,
) -> Optional[list]:
    if validations:
        for value in validations:
            if "batch_request" in value:
                value["batch_request"] = get_batch_request_as_dict(
                    batch_request=value["batch_request"]
                )

    return validations


def validate_validation_dict(validation_dict: dict):
    if validation_dict.get("batch_request") is None:
        raise ge_exceptions.CheckpointError("validation batch_request cannot be None")
    if not validation_dict.get("expectation_suite_name"):
        raise ge_exceptions.CheckpointError(
            "validation expectation_suite_name must be specified"
        )
    if not validation_dict.get("action_list"):
        raise ge_exceptions.CheckpointError("validation action_list cannot be empty")


def send_cloud_notification(url: str, headers: dict):
    """
    Post a CloudNotificationAction to GE Cloud Backend for processing.
    """
    session = requests.Session()

    try:
        response = session.post(url=url, headers=headers)
    except requests.ConnectionError:
        logger.error(
            f"Failed to connect to Cloud backend at {url} " f"after {10} retries."
        )
    except Exception as e:
        logger.error(str(e))
    else:
        if response.status_code != 200:
            message = f"Cloud Notification request at {url} returned error {response.status_code}: {response.text}"
            logger.error(message)
            return {"cloud_notification_result": message}
        else:
            return {"cloud_notification_result": "Cloud notification succeeded."}
