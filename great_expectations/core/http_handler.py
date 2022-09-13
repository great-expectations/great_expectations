from typing import Optional

import requests

from great_expectations import __version__


class HTTPHandler:
    """
    Facade class designed to be a lightweight wrapper around HTTP requests.
    For all external HTTP requests made in Great Expectations, this is the entry point.

    HTTPHandler is designed tobe library agnostic - the underlying implementation does not
    matter as long as we fulfill the following contract:
        * get
        * post
        * put
        * patch
        * delete

    20220913 - Chetan - This class should be refactored to completely abstract away any
    specific implementation details (explicit reference to `requests` lib). As it stands,
    the handler returns a `requests.Response` object. Additionally, error checking is done
    downstream in GeCloudStoreBackend that also directly references this lib.
    """

    def __init__(self, access_token: str, timeout: int = 20) -> None:
        self._headers = self._init_headers(access_token)
        self._timeout = timeout

    def _init_headers(self, access_token: str) -> dict:
        headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {access_token}",
            "Gx-Version": __version__,
        }
        return headers

    def get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        return requests.get(
            url=url, params=params, headers=self._headers, timeout=self._timeout
        )

    def post(self, url: str, json: dict) -> requests.Response:
        return requests.post(
            url, json=json, headers=self._headers, timeout=self._timeout
        )

    def put(self, url: str, json: dict) -> requests.Response:
        return requests.put(
            url, json=json, headers=self._headers, timeout=self._timeout
        )

    def patch(self, url: str, json: dict) -> requests.Response:
        return requests.put(
            url, json=json, headers=self._headers, timeout=self._timeout
        )

    def delete(self, url: str, json: dict) -> requests.Response:
        return requests.delete(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
