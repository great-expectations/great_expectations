from typing import Callable

import pytest


class MockResponse:
    def __init__(self, json_data: dict, status_code: int) -> None:
        self._json_data = json_data
        self._status_code = status_code

    def json(self):
        return self._json_data


@pytest.fixture
def mock_response_factory() -> Callable[[dict, int], MockResponse]:
    def _make_mock_response(json_data: dict, status_code: int) -> MockResponse:
        return MockResponse(json_data=json_data, status_code=status_code)

    return _make_mock_response
