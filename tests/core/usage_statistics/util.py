from great_expectations.core.usage_statistics.usage_statistics import (
    EMIT_EXCEPTION_PREFIX,
)


def assert_no_usage_stats_exceptions(messages) -> None:
    """
    Since the usage stats functionality does not raise exceptions but merely logs them, we need to check the logs for errors.
    """

    assert not any(
        [EMIT_EXCEPTION_PREFIX in message for message in messages]
    ), "Exception caught when processing or sending a usage stats event"
