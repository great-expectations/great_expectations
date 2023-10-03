import os
import pathlib
from typing import Final
from urllib import parse

import pytest
from pact import Consumer, Pact, Provider

CONSUMER: Final[str] = "great_expectations"
PROVIDER: Final[str] = "mercury"
PACT_UPLOAD_URL: Final[
    str
] = f"https://greatexpectations.pactflow.io/pacts/provider/{PROVIDER}/consumer/{CONSUMER}/version"

PACT_BROKER_BASE_URL: Final[str] = "https://greatexpectations.pactflow.io"
PACT_BROKER_TOKEN: Final[str] = os.environ.get("PACT_BROKER_READ_ONLY_TOKEN")
PACT_MOCK_HOST: Final[str] = "localhost"
PACT_MOCK_PORT: Final[int] = 9292
PACT_DIR = str((pathlib.Path(__file__).parent / pathlib.Path("pacts")).resolve())


@pytest.fixture
def pact(request) -> Pact:
    pact: Pact = Consumer(CONSUMER).has_pact_with(
        Provider(PROVIDER),
        broker_base_url=PACT_BROKER_BASE_URL,
        broker_token=PACT_BROKER_TOKEN,
        host_name=PACT_MOCK_HOST,
        port=PACT_MOCK_PORT,
        pact_dir=PACT_DIR,
        publish_to_broker=False,
    )
    pact.start_service()
    yield pact
    pact.stop_service()


@pytest.fixture
def pact_mock_mercury_url():
    return parse.urlparse(f"http://{PACT_MOCK_HOST}:{PACT_MOCK_PORT}")
