from __future__ import annotations

import json
import logging
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import requests

from great_expectations.compatibility import aws

logger = logging.getLogger(__name__)


def send_slack_notification(
    payload: dict,
    slack_webhook: str | None = None,
    slack_channel: str | None = None,
    slack_token: str | None = None,
) -> str | None:
    session = requests.Session()
    url = slack_webhook
    headers = None

    # Slack doc about overwritting the channel when using the legacy Incoming Webhooks
    # https://api.slack.com/legacy/custom-integrations/messaging/webhooks
    # ** Since it is legacy, it could be deprecated or removed in the future **
    if slack_channel:
        payload["channel"] = slack_channel

    if not slack_webhook:
        url = "https://slack.com/api/chat.postMessage"
        headers = {"Authorization": f"Bearer {slack_token}"}

    if not url:
        raise ValueError("No Slack webhook URL provided.")  # noqa: TRY003

    try:
        response = session.post(url=url, headers=headers, json=payload)
        response.raise_for_status()
    except requests.ConnectionError:
        logger.warning(f"Failed to connect to Slack webhook after {10} retries.")
        return None
    except requests.HTTPError:
        logger.warning(
            "Request to Slack webhook " f"returned error {response.status_code}: {response.text}"  # type: ignore[possibly-undefined] # ok for httperror
        )
        return None

    return "Slack notification succeeded."


# noinspection SpellCheckingInspection
def send_opsgenie_alert(query: str, message: str, settings: dict) -> bool:
    """Creates an alert in Opsgenie."""
    if settings["region"] is not None:
        url = (
            f"https://api.{settings['region']}.opsgenie.com/v2/alerts"  # accommodate for Europeans
        )
    else:
        url = "https://api.opsgenie.com/v2/alerts"

    headers = {"Authorization": f"GenieKey {settings['api_key']}"}
    payload = {
        "message": message,
        "description": query,
        "priority": settings["priority"],  # allow this to be modified in settings
        "tags": settings["tags"],
    }

    session = requests.Session()

    try:
        response = session.post(url, headers=headers, json=payload)
        response.raise_for_status()
    except requests.ConnectionError as e:
        logger.warning(f"Failed to connect to Opsgenie: {e}")
        return False
    except requests.HTTPError as e:
        logger.warning(f"Request to Opsgenie API returned error {response.status_code}: {e}")  # type: ignore[possibly-undefined] # ok for httperror
        return False
    return True


def send_microsoft_teams_notifications(payload: dict, microsoft_teams_webhook: str) -> str | None:
    session = requests.Session()
    try:
        response = session.post(url=microsoft_teams_webhook, json=payload)
        response.raise_for_status()
    except requests.ConnectionError:
        logger.warning("Failed to connect to Microsoft Teams webhook after 10 retries.")
        return None
    except requests.HTTPError as e:
        logger.warning(f"Request to Microsoft Teams API returned error {response.status_code}: {e}")  # type: ignore[possibly-undefined] # ok for httperror
        return None

    return "Microsoft Teams notification succeeded."


def send_webhook_notifications(query, webhook, target_platform):
    session = requests.Session()
    try:
        response = session.post(url=webhook, json=query)
    except requests.ConnectionError:
        logger.warning(f"Failed to connect to {target_platform} webhook after 10 retries.")
    except Exception as e:
        logger.error(str(e))  # noqa: TRY400
    else:
        if response.status_code != 200:  # noqa: PLR2004
            logger.warning(
                f"Request to {target_platform} webhook "
                f"returned error {response.status_code}: {response.text}"
            )
        else:
            return f"{target_platform} notification succeeded."


# noinspection SpellCheckingInspection
def send_email(  # noqa: C901, PLR0913
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
        if sender_login is not None and sender_password is not None:
            mailserver.login(sender_login, sender_password)
        elif not (sender_login is None and sender_password is None):
            logger.error(
                "Please specify both sender_login and sender_password or specify both as None"
            )
        mailserver.sendmail(sender_alias, receiver_emails_list, msg.as_string())
        mailserver.quit()
    except smtplib.SMTPConnectError:
        logger.error(f"Failed to connect to the SMTP server at address: {smtp_address}")  # noqa: TRY400
    except smtplib.SMTPAuthenticationError:
        logger.error(f"Failed to authenticate to the SMTP server at address: {smtp_address}")  # noqa: TRY400
    except Exception as e:
        logger.error(str(e))  # noqa: TRY400
    else:
        return "success"


def send_sns_notification(
    sns_topic_arn: str, sns_subject: str, validation_results: str, **kwargs
) -> str:
    """
    Send JSON results to an SNS topic with a schema of:


    :param sns_topic_arn:  The SNS Arn to publish messages to
    :param sns_subject: : The SNS Message Subject - defaults to expectation_suite_identifier.name
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
        logger.error(error_msg)  # noqa: TRY400
        return error_msg
    else:
        return f"Successfully posted results to {response['MessageId']} with Subject {sns_subject}"
