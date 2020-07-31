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
