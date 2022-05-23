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
    prev_date: Optional[dt.datetime] = None
    prev_version: Optional[version.Version] = None

    for date, release_version in release_schedule.items():
        if prev_date and prev_version:
            assert date > prev_date and release_version > prev_version
        prev_date = date
        prev_version = release_version


def test_release_schedule_is_updated_for_future_releases(
    release_schedule: Dict[dt.datetime, version.Version], release_file: str
) -> None:
    today: dt.datetime = dt.datetime.today()
    future_release_count: int = sum(1 for date in release_schedule if date > today)
    assert (
        future_release_count > 0
    ), f"No upcoming releases! Please update `{release_file}`."
