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
                url=webhook, max_retries=10, target_platform=target_platform,
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
            return "{target_platform} notification succeeded.".format(target_platform=target_platform)
