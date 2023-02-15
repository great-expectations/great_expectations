import datetime

import pytest

from great_expectations.core.run_identifier import RunIdentifier


@pytest.mark.unit
def test_run_identifier_parses_datetime_run_name():
    time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
    run_id = RunIdentifier(run_name=time)
    assert run_id.run_name == run_id.run_time.strftime("%Y%m%dT%H%M%S.%fZ")
