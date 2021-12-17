from typing import List

from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatsExceptionPrefix,
)


def assert_no_usage_stats_exceptions(messages: List[str]) -> None:
    """
    Since the usage stats functionality does not raise exceptions but merely logs them, we need to check the logs for errors.
    """

    assert not any(
        [
            UsageStatsExceptionPrefix.EMIT_EXCEPTION.value in message
            for message in messages
        ]
    ), "Exception caught when processing or sending a usage stats event"


def usage_stats_invalid_messages_exist(messages: List[str]) -> bool:
    """
    Since the usage stats functionality does not raise exceptions but merely logs them, we need to check the logs for errors.
    """

    return any(
        [
            UsageStatsExceptionPrefix.INVALID_MESSAGE.value in message
            for message in messages
        ]
    )
