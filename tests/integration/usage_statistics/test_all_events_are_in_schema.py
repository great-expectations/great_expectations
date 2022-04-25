import itertools
from typing import List

import pytest

from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.core.usage_statistics.schemas import (
    anonymized_usage_statistics_record_schema,
)


@pytest.fixture
def all_events_from_anonymized_usage_statistics_record_schema() -> List[str]:
    """Pull all unique event names from our main anonymized usage stats record schema.

    Note: This assumes all events are of "enum" type (they should be).

    Returns:
        Unique event names
    """
    event_type_defs: List[dict] = anonymized_usage_statistics_record_schema["oneOf"]
    event_lists: List[List[str]] = [
        event_type_def["properties"]["event"]["enum"]
        for event_type_def in event_type_defs
    ]

    unique_events: List[str] = sorted(list(set(list(itertools.chain(*event_lists)))))

    return unique_events


def test_all_events_are_in_event_validation_schema(
    all_events_from_anonymized_usage_statistics_record_schema,
):
    """What does this test and why?

    This test ensures that all of the usage stats events that we are sending are accounted for in our
    event validation schema.
    """
    events_in_schema: List[
        str
    ] = all_events_from_anonymized_usage_statistics_record_schema

    assert set(events_in_schema) == set(UsageStatsEvents.get_all_event_names())
