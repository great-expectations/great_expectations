from typing import Optional

import requests

from great_expectations import __version__


class HTTPHandler:
    def __init__(self, headers: Optional[dict] = None, timeout: int = 20) -> None:
        self._headers = headers or {}
        self._timeout = timeout

    @property
    def headers(self) -> dict:
        return self._headers

    @headers.setter
    def headers(self, value: dict) -> None:
        self._headers = value

    def _init_headers(self, access_token: str) -> dict:
        headers = {
            "Content-Type": "application/vnd.api+json",
            "Authorization": f"Bearer {access_token}",
            "Gx-Version": __version__,
        }
        return headers

    def get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        response = requests.get(
            url=url, params=params, headers=self._headers, timeout=self._timeout
        )
        response.raise_for_status()
        return response

    def post(self, url: str, json: dict) -> requests.Response:
        response = requests.post(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        response.raise_for_status()
        return response

    def put(self, url: str, json: dict) -> requests.Response:
        response = requests.put(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        response.raise_for_status()
        return response

    def patch(self, url: str, json: dict) -> requests.Response:
        response = requests.put(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        response.raise_for_status()
        return response

    def delete(self, url: str, json: dict) -> requests.Response:
        response = requests.delete(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        response.raise_for_status()
        return response
