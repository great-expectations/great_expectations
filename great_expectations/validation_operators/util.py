import logging

import requests

logger = logging.getLogger(__name__)


def send_slack_notification(query, slack_webhook):
    session = requests.Session()

    try:
        response = session.post(url=slack_webhook, json=query)
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


def send_opsgenie_alert(query, suite_name, settings):
    """Creates an alert in Opsgenie."""
    if settings["region"] != None:
        url = "https://api.{region}.opsgenie.com/v2/alerts".format(
            region=settings["region"]
        )  # accomodate for Europeans
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
