import unittest.mock
from typing import Any, Callable, Dict, Optional, Union

import pytest
from requests.exceptions import HTTPError, Timeout

from great_expectations.data_context.store.ge_cloud_store_backend import AnyPayload

JSONData = Union[AnyPayload, Dict[str, Any]]
RequestError = Union[HTTPError, Timeout]


class MockResponse:
    def __init__(
        self,
        json_data: JSONData,
        status_code: int,
        exc_to_raise: Optional[RequestError] = None,
    ) -> None:
        self._json_data = json_data
        # status code should be publicly accesable
        self.status_code = status_code
        self._exc_to_raise = exc_to_raise

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self._exc_to_raise:
            raise self._exc_to_raise
        if self.status_code >= 400:
            raise HTTPError(response=self)
        return None


@pytest.fixture
def mock_response_factory() -> Callable[
    [JSONData, int, Optional[RequestError]], MockResponse
]:
    def _make_mock_response(
        json_data: JSONData,
        status_code: int,
        exc_to_raise: Optional[RequestError] = None,
    ) -> MockResponse:
        return MockResponse(
            json_data=json_data, status_code=status_code, exc_to_raise=exc_to_raise
        )

    return _make_mock_response


@pytest.fixture
def mock_ge_cloud_unavailable(mock_response_factory: Callable):
    def mocked_response(*args, **kwargs):

        return MockResponse(
            {"code": 503, "detail": "API is unavailable"},
            503,
        )

    with unittest.mock.patch(
        "requests.request", autospec=True, side_effect=mocked_response
    ) as mock_request:
        yield mock_request
