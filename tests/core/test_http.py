from typing import Dict

import pytest

from great_expectations.core.http import DEFAULT_TIMEOUT, create_session


@pytest.mark.unit
def test_session_factory_contains_appropriate_headers(
    ge_cloud_access_token: str, request_headers: Dict[str, str]
) -> None:
    session = create_session(access_token=ge_cloud_access_token)
    for key, val in request_headers.items():
        assert session.headers.get(key) == val


@pytest.mark.unit
def test_session_factory_contains_appropriate_adapters() -> None:
    access_token = "05f23701-d8e3-4c0a-8e38-2ad7bf16cb58"
    retry_count = 10
    backoff_factor = 0.1
    session = create_session(
        access_token=access_token,
        retry_count=retry_count,
        backoff_factor=backoff_factor,
    )

    assert len(session.adapters) == 2
    for adapter in session.adapters.values():
        assert adapter.timeout == DEFAULT_TIMEOUT
        retries = adapter.max_retries
        assert retries.total == retry_count
        assert retries.backoff_factor == backoff_factor
