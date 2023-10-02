import json
import logging
import pathlib
from typing import Final

import pytest
import requests
from pact import Consumer, Provider
from requests.auth import HTTPBasicAuth

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CONSUMER: Final[str] = "great_expectations"
PROVIDER: Final[str] = "mercury"
PACT_UPLOAD_URL: Final[
    str
] = f"http://localhost:9292/pacts/provider/{PROVIDER}/consumer/{CONSUMER}/version"
PACT_FILE: Final[str] = f"{CONSUMER}-{PROVIDER}.json"
PACT_BROKER_USERNAME: Final[str] = "pact_broker"
PACT_BROKER_PASSWORD: Final[str] = ""

PACT_MOCK_HOST = "localhost"
PACT_MOCK_PORT = 9292
PACT_DIR = pathlib.Path(__file__).stem


def pytest_addoption(parser):
    parser.addoption(
        "--publish-pact",
        type=str,
        action="store",
        help="Upload generated pact file to pact broker with version",
    )


def push_to_broker(version):
    """TODO: see if we can dynamically learn the pact file name, version, etc."""
    with open(pathlib.Path(PACT_DIR, PACT_FILE), "rb") as pact_file:
        pact_file_json = json.load(pact_file)

    basic_auth = HTTPBasicAuth(PACT_BROKER_USERNAME, PACT_BROKER_PASSWORD)

    log.info("Uploading pact file to pact broker...")

    r = requests.put(
        f"{PACT_UPLOAD_URL}/{version}", auth=basic_auth, json=pact_file_json
    )
    if not r.ok:
        log.error("Error uploading: %s", r.content)
        r.raise_for_status()


@pytest.fixture(scope="session")
def pact(request):
    pact = Consumer(CONSUMER).has_pact_with(
        Provider(PROVIDER),
        host_name=PACT_MOCK_HOST,
        port=PACT_MOCK_PORT,
        pact_dir=PACT_DIR,
    )
    pact.start_service()
    yield pact
    pact.stop_service()

    version = request.config.getoption("--publish-pact")
    if not request.node.testsfailed and version:
        push_to_broker(version)


@pytest.fixture
def cloud_data_context():
    import great_expectations as gx

    return gx.get_context(
        mode="cloud", cloud_base_url=f"https://{PACT_MOCK_HOST}:{PACT_MOCK_PORT}"
    )
