from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING, Final

import pytest
from pact import Consumer, Pact, Provider

from great_expectations.core.http import create_session

if TYPE_CHECKING:
    import requests

CONSUMER: Final[str] = "great_expectations"
PROVIDER: Final[str] = "mercury"

PACT_BROKER_BASE_URL: Final[str] = "https://greatexpectations.pactflow.io"
PACT_BROKER_TOKEN: Final[str] = os.environ.get("PACT_BROKER_READ_ONLY_TOKEN")
PACT_MOCK_HOST: Final[str] = "localhost"
PACT_MOCK_PORT: Final[int] = 9292
PACT_DIR: Final[str] = str(pathlib.Path(__file__).parent.resolve())

GX_CLOUD_ACCESS_TOKEN = os.environ.get("GX_CLOUD_ACCESS_TOKEN")


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
def pact_mock_mercury_url() -> str:
    return f"http://{PACT_MOCK_HOST}:{PACT_MOCK_PORT}"


@pytest.fixture
def session() -> requests.Session:
    return create_session(access_token=GX_CLOUD_ACCESS_TOKEN)


@pytest.fixture
def headers(session: requests.Session) -> dict:
    headers = dict(session.headers)
    headers.pop("Authorization")
    headers.pop("Gx-Version")
    return headers
