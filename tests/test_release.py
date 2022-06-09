import datetime as dt
import json
from typing import Any, Dict, Optional, cast

import dateutil.parser
import pytest
import pytz
from packaging import version
from pytz.tzinfo import DstTzInfo

from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def release_file() -> str:
    path: str = file_relative_path(__file__, "../.github/release_schedule.json")
    return path


@pytest.fixture
def timezone() -> DstTzInfo:
    # Since releases are cut based on an EST schedule, let's ensure our validation matches that
    tz: DstTzInfo = cast(DstTzInfo, pytz.timezone("US/Eastern"))
    return tz


@pytest.fixture
def release_schedule(
    release_file: str, timezone: DstTzInfo
) -> Dict[dt.datetime, version.Version]:
    with open(release_file) as f:
        release_schedule: Dict[str, str] = json.loads(f.read())

    parsed_schedule: Dict[dt.datetime, version.Version] = {}
    for date, release_version in release_schedule.items():
        parsed_date = dateutil.parser.parse(date)
        parsed_date = timezone.localize(parsed_date)
        parsed_version = cast(version.Version, version.parse(release_version))
        parsed_schedule[parsed_date] = parsed_version

    return parsed_schedule


def test_release_schedule_adheres_to_schema(
    release_schedule: Dict[dt.datetime, version.Version],
    timezone: DstTzInfo,
) -> None:
    today: dt.datetime = dt.datetime.now(timezone)
    prev_date: Optional[dt.datetime] = None
    prev_version: Optional[version.Version] = None

    for timestamp, release_version in release_schedule.items():

        # No old releases should exist within the release schedule (deleted during release process)
        assert timestamp.date() >= today.date()

        if prev_date and prev_version:

            # Each date should be greater than the prior one
            assert timestamp > prev_date

            # Each release occurs on a Thursday
            assert timestamp.weekday() == 3

            curr_minor: int = release_version.minor
            curr_patch: int = release_version.micro
            prev_minor: int = prev_version.minor
            prev_patch: int = prev_version.micro

            # If incrementing a minor version number, the patch should be 0 (ex: 0.15.7 -> 0.16.0)
            if curr_minor > prev_minor:
                assert curr_minor - prev_minor == 1 and curr_patch == 0
            # If incrementing a patch version number, the patch should get incremented by 1 (ex: 0.15.7 -> 0.15.8)
            else:
                assert curr_minor - prev_minor == 0 and curr_patch - prev_patch == 1

        prev_date = timestamp
        prev_version = release_version

    # For release safety, there must always be items in the scheduler
    future_release_count: int = sum(1 for date in release_schedule if date > today)
    assert future_release_count > 0
