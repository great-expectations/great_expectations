from typing import List

import requests

from tests.integration.usage_statistics.test_usage_statistics_messages import (
    valid_usage_statistics_messages,
)


def get_usage_stats_example_events() -> List:
    msg: dict = valid_usage_statistics_messages

    # remove top-level items from the messages
    keys: List = [k for k in msg.keys()]
    msg_list: List = []
    for key in keys:
        for item in msg[key]:
            msg_list.append(item)

    return msg_list


def get_gx_version() -> str:
    git_raw = "https://raw.githubusercontent.com"
    git_repo = "great-expectations"
    git_file = "great_expectations/develop/great_expectations/deployment_version"
    path = f"{git_raw}/{git_repo}/{git_file}"

    return requests.get(path).text.replace(".", "").replace("\n", "")
