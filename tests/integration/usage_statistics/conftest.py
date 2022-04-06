import pytest
import requests
from requests.adapters import HTTPAdapter, Retry


@pytest.fixture(scope="session")
def requests_session_with_retries() -> requests.Session:
    # https://stackoverflow.com/a/35636367
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session
