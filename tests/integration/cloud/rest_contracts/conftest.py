from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING, Any, Callable, Final, Optional

import pytest
from pact import Consumer, Pact, Provider

from great_expectations.core.http import create_session

if TYPE_CHECKING:
    from requests import Session

CONSUMER: Final[str] = "great_expectations"
PROVIDER: Final[str] = "mercury"

PACT_BROKER_BASE_URL: Final[str] = "https://greatexpectations.pactflow.io"
PACT_BROKER_TOKEN: Final[str] = os.environ.get("PACT_BROKER_READ_ONLY_TOKEN")
PACT_MOCK_HOST: Final[str] = "localhost"
PACT_MOCK_PORT: Final[int] = 9292
PACT_DIR: Final[str] = str(pathlib.Path(__file__).parent.resolve())
PACT_MOCK_MERCURY_URL: Final[str] = f"http://{PACT_MOCK_HOST}:{PACT_MOCK_PORT}"


@pytest.fixture
def session() -> Session:
    return create_session(access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"))


@pytest.fixture
def pact(request) -> Pact:
    pact: Pact = Consumer(CONSUMER).has_pact_with(
        Provider(PROVIDER),
        broker_base_url=PACT_BROKER_BASE_URL,
        broker_token=PACT_BROKER_TOKEN,
        host_name=PACT_MOCK_HOST,
        port=PACT_MOCK_PORT,
        pact_dir=PACT_DIR,
    )
    pact.start_service()
    yield pact
    pact.stop_service()


@pytest.fixture
def run_pact_test(
    session: Session,
    pact: Pact,
) -> Callable:
    def _run_pact_test(
        path: str,
        method: str,
        upon_receiving: str,
        given: str,
        *,
        request_body: Optional[Any] = None,
        request_headers: Optional[Any] = None,
        response_status: int,
        response_body: Any,
    ):
        request = {
            "method": method,
            "path": path,
        }
        if request_body is not None:
            request["body"] = request_body
        if request_headers is not None:
            request["headers"] = request_headers

        response = {
            "status": response_status,
            "body": response_body,
        }

        (
            pact.given(given)
            .upon_receiving(upon_receiving)
            .with_request(**request)
            .will_respond_with(**response)
        )

        request_url = f"{PACT_MOCK_MERCURY_URL}{path}"

        with pact:
            session.get(request_url)

    return _run_pact_test
