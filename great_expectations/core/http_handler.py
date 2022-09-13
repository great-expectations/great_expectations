from typing import Optional

import requests

from great_expectations import __version__


class HTTPHandler:
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
        response = requests.get(
            url=url, params=params, headers=self._headers, timeout=self._timeout
        )
        return response

    def post(self, url: str, json: dict) -> requests.Response:
        response = requests.post(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        return response

    def put(self, url: str, json: dict) -> requests.Response:
        response = requests.put(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        return response

    def patch(self, url: str, json: dict) -> requests.Response:
        response = requests.put(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        return response

    def delete(self, url: str, json: dict) -> requests.Response:
        response = requests.delete(
            url, json=json, headers=self._headers, timeout=self._timeout
        )
        return response
