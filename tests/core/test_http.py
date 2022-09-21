from typing import Dict

import pytest

from great_expectations.core.http import session_factory


@pytest.mark.unit
def test_session_factory_contains_appropriate_headers(
    ge_cloud_access_token: str, request_headers: Dict[str, str]
) -> None:
    session = session_factory(access_token=ge_cloud_access_token)
    for key, val in request_headers.items():
        assert session.headers.get(key) == val
