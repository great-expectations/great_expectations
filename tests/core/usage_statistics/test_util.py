import pytest

from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatsExceptionPrefix,
)
from tests.core.usage_statistics.util import (
    assert_no_usage_stats_exceptions,
    usage_stats_invalid_messages_exist,
)


@pytest.mark.parametrize(
    "test_input",
    [
        pytest.param(
            ["just", "some", "logger", "messages"], id="list_without_exceptions"
        ),
        pytest.param([], id="empty_list"),
    ],
)
def test_assert_no_usage_stats_exceptions_passing(test_input):
    assert_no_usage_stats_exceptions(messages=test_input)


@pytest.mark.parametrize(
    "test_input",
    [
        pytest.param(
            [
                "just",
                "some",
                "logger",
                "messages",
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value} some error message",
            ],
            id="list_with_exceptions",
        ),
        pytest.param(
            [
                "just",
                "some",
                "logger",
                "messages",
                f"{UsageStatsExceptionPrefix.EMIT_EXCEPTION.value}some error message",
            ],
            id="list_with_exceptions_no_whitespace",
        ),
    ],
)
def test_assert_no_usage_stats_exceptions_failing(test_input):

    with pytest.raises(AssertionError):
        assert_no_usage_stats_exceptions(messages=test_input)


@pytest.mark.parametrize(
    "test_input,test_output",
    [
        pytest.param(
            ["just", "some", "logger", "messages"],
            False,
            id="list_without_invalid_messages",
        ),
        pytest.param(
            [
                "just",
                "some",
                "logger",
                "messages",
                f"{UsageStatsExceptionPrefix.INVALID_MESSAGE.value} some invalid message",
            ],
            True,
            id="list_with_invalid_message",
        ),
        pytest.param(
            [
                "just",
                "some",
                "logger",
                "messages",
                f"{UsageStatsExceptionPrefix.INVALID_MESSAGE.value}some invalid message",
            ],
            True,
            id="list_with_invalid_message_no_whitespace",
        ),
        pytest.param([], False, id="empty_list"),
    ],
)
def test_usage_stats_invalid_messages_exist(test_input, test_output):
    assert usage_stats_invalid_messages_exist(messages=test_input) == test_output
