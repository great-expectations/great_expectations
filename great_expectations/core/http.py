import json
import logging
from pprint import pformat as pf

import requests
from requests.adapters import HTTPAdapter, Retry

from great_expectations import __version__

DEFAULT_TIMEOUT = 20

LOGGER = logging.getLogger(__name__)


def _log_request_method_and_response(r: requests.Response, *args, **kwargs):
    LOGGER.info(f"{r.request.method} {r.request.url} - {r}")
    try:
        LOGGER.debug(f"{r}\n{pf(r.json(), depth=3)}")
    except json.JSONDecodeError:
        LOGGER.debug(f"{r}\n{r.content.decode()}")


class _TimeoutHTTPAdapter(HTTPAdapter):
    # https://stackoverflow.com/a/62044100
    # Session-wide timeouts are not supported by requests
    # but are discussed in detail here: https://github.com/psf/requests/issues/3070
    def __init__(self, *args, **kwargs) -> None:
        self.timeout = kwargs.pop("timeout", DEFAULT_TIMEOUT)
        super().__init__(*args, **kwargs)

    def send(self, request: requests.PreparedRequest, **kwargs) -> requests.Response:  # type: ignore[override]
        kwargs["timeout"] = kwargs.get("timeout", self.timeout)
        return super().send(request, **kwargs)


def create_session(
    access_token: str,
    retry_count: int = 5,
    backoff_factor: float = 1.0,
    timeout: int = DEFAULT_TIMEOUT,
) -> requests.Session:
    session = requests.Session()
    session = _update_headers(session=session, access_token=access_token)
    session = _mount_adapter(
        session=session,
        timeout=timeout,
        retry_count=retry_count,
        backoff_factor=backoff_factor,
    )
    # add an event hook to log outgoing http requests
    # https://requests.readthedocs.io/en/latest/user/advanced/#event-hooks
    session.hooks["response"].append(_log_request_method_and_response)
    return session


def _update_headers(session: requests.Session, access_token: str) -> requests.Session:
    headers = {
        "Content-Type": "application/vnd.api+json",
        "Authorization": f"Bearer {access_token}",
        "Gx-Version": __version__,
    }
    session.headers.update(headers)
    return session


def _mount_adapter(
    session: requests.Session, timeout: int, retry_count: int, backoff_factor: float
) -> requests.Session:
    retries = Retry(total=retry_count, backoff_factor=backoff_factor)
    adapter = _TimeoutHTTPAdapter(timeout=timeout, max_retries=retries)
    for protocol in ("http://", "https://"):
        session.mount(protocol, adapter)
    return session
