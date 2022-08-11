from typing import Callable, Optional

import pytest
from requests.exceptions import HTTPError


class MockResponse:
    def __init__(
        self,
        json_data: dict,
        status_code: int,
        _exc_to_raise: Optional[HTTPError] = None,
    ) -> None:
        self._json_data = json_data
        self._status_code = status_code
        self._exc_to_raise: Optional[HTTPError] = _exc_to_raise

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self._exc_to_raise:
            raise self._exc_to_raise
        return None


@pytest.fixture
def mock_response_factory() -> Callable[[dict, int], MockResponse]:
    def _make_mock_response(json_data: dict, status_code: int) -> MockResponse:
        return MockResponse(json_data=json_data, status_code=status_code)

    return _make_mock_response
