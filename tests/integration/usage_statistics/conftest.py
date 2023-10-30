import pytest
import requests
from requests.adapters import HTTPAdapter, Retry


@pytest.fixture(scope="session")
def requests_session_with_retries() -> requests.Session:
    # https://stackoverflow.com/a/35636367
    session = requests.Session()
    # backoff factor above 1.0 means increasing sleep time between retries
    retries = Retry(total=5, backoff_factor=1.25, status_forcelist=[502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session
