import datetime as dt
import json
from typing import Dict, Optional, cast

import dateutil.parser
import pytest
from packaging import version

from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def release_file() -> str:
    path: str = file_relative_path(__file__, "../.github/release_schedule.json")
    return path


@pytest.fixture
def release_schedule(release_file: str) -> Dict[dt.datetime, version.Version]:
    with open(release_file) as f:
        release_schedule: Dict[str, str] = json.loads(f.read())

    parsed_schedule: Dict[dt.datetime, version.Version] = {}
    for date, release_version in release_schedule.items():
        parsed_date = dateutil.parser.parse(date)
        parsed_version = cast(version.Version, version.parse(release_version))
        parsed_schedule[parsed_date] = parsed_version

    return parsed_schedule


def test_release_schedule_adheres_to_schema(
    release_schedule: Dict[dt.datetime, version.Version]
) -> None:
    today: dt.datetime = dt.datetime.today()
    prev_date: Optional[dt.datetime] = None
    prev_version: Optional[version.Version] = None

    for date, release_version in release_schedule.items():

        if prev_date and prev_version:

            # Each date should be greater than the prior one
            assert date > prev_date

            # Each release occurs on a Thursday
            assert date.weekday() == 3

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

        prev_date = date
        prev_version = release_version

    # For release safety, there must always be items in the scheduler
    future_release_count: int = sum(1 for date in release_schedule if date > today)
    assert future_release_count > 0
