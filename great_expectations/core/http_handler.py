from typing import Callable, Optional

import requests

from great_expectations import __version__


class HTTPResponse:
    def __init__(self, response: requests.Response) -> None:
        self._response = response

    @property
    def status_code(self) -> int:
        return self._response.status_code

    @property
    def text(self) -> str:
        return self._response.text

    def json(self) -> dict:
        return self._response.json()

    def raise_for_status(self) -> None:
        self._response.raise_for_status()


class HTTPHandler:
    """
    Facade class designed to be a lightweight wrapper around HTTP requests.
    For all external HTTP requests made in Great Expectations, this is the entry point.

    HTTPHandler is designed to be library agnostic - the underlying implementation does not
    matter as long as we fulfill the following contract:
        * get
        * post
        * put
        * patch
        * delete
    """

    TIMEOUT = 20

    def __init__(self, access_token: str, timeout: int = TIMEOUT) -> None:
        self._headers = self._init_headers(access_token)
        self._timeout = timeout

    def _init_headers(self, access_token: str) -> dict:
        headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {access_token}",
            "Gx-Version": __version__,
        }
        return headers

    def _make_request(
        self,
        fn: Callable,
        url: str,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> HTTPResponse:
        response = fn(
            url, params=params, json=json, headers=self._headers, timeout=self._timeout
        )
        return HTTPResponse(response=response)

    def get(self, url: str, params: Optional[dict] = None) -> HTTPResponse:
        return self._make_request(fn=requests.get, url=url, params=params)

    def post(self, url: str, json: dict) -> HTTPResponse:
        return self._make_request(fn=requests.post, url=url, json=json)

    def put(self, url: str, json: dict) -> HTTPResponse:
        return self._make_request(fn=requests.put, url=url, json=json)

    def patch(self, url: str, json: dict) -> HTTPResponse:
        return self._make_request(fn=requests.put, url=url, json=json)

    def delete(self, url: str, json: dict) -> HTTPResponse:
        return self._make_request(fn=requests.delete, url=url, json=json)
