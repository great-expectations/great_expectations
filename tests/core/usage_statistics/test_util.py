import pytest

from great_expectations.core.usage_statistics.usage_statistics import (
    UsageStatsExceptionPrefix,
)
from tests.core.usage_statistics.util import assert_no_usage_stats_exceptions


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
