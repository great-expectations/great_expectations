from __future__ import annotations

import logging
from typing import Final

import pytest
import requests
from requests import PreparedRequest, Response
from requests.adapters import HTTPAdapter, Retry

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


@pytest.fixture
def requests_session_with_retries() -> requests.Session:
    # https://stackoverflow.com/a/35636367
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    previous_requests: list[PreparedRequest] = []

    def _log_retries(r: Response, *args, **kwargs) -> None:
        if not previous_requests:
            return
        LOGGER.info(f"{r.request.method} was retried - {len(previous_requests)} times")
        previous_requests.append(r.request)

    session.hooks["response"].append(_log_retries)

    return session
