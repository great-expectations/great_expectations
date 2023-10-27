import json
import logging
from pprint import pformat as pf

import httpx

from great_expectations import __version__

DEFAULT_TIMEOUT = 20

LOGGER = logging.getLogger(__name__)


def _log_request_method_and_response(r: httpx.Response, *args, **kwargs):
    LOGGER.info(f"{r.request.method} {r.request.url} - {r}")
    try:
        LOGGER.debug(f"{r}\n{pf(r.json(), depth=3)}")
    except json.JSONDecodeError:
        LOGGER.debug(f"{r}\n{r.content.decode()}")


def create_session(
    access_token: str,
    retry_count: int = 5,
    timeout: int = DEFAULT_TIMEOUT,
) -> httpx.Client:
    # https://www.python-httpx.org/advanced/#custom-transports
    # TODO: test is this using exponential backoff?
    retry_transport = httpx.HTTPTransport(retries=retry_count)
    session = httpx.Client(
        headers=_update_headers(access_token=access_token),
        timeout=timeout,
        transport=retry_transport,
        event_hooks={"response": [_log_request_method_and_response]},
    )
    return session


def _update_headers(access_token: str) -> httpx.Headers:
    return httpx.Headers(
        {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {access_token}",
            "Gx-Version": __version__,
        }
    )
