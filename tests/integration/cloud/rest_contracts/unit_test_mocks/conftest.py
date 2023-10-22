from typing import Any
from unittest import mock

import pytest
from pact.matchers import Matcher

import great_expectations as gx
from great_expectations.data_context import CloudDataContext
from tests.integration.cloud.rest_contracts.conftest import JsonType, PactBody
from tests.integration.cloud.rest_contracts.test_data_context_configuration import (
    DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY,
)


def _convert_matcher_to_value(matcher: Matcher) -> Any:
    return matcher.generate()["contents"]


def _reify_pact_body(
    body: PactBody,
) -> JsonType:
    if isinstance(body, list):
        for index, item in enumerate(body):
            if isinstance(item, Matcher):
                body[index] = _convert_matcher_to_value(matcher=item)
            body[index] = _reify_pact_body(body=body[index])
        return body
    elif isinstance(body, Matcher):
        return _reify_pact_body(body=_convert_matcher_to_value(matcher=body))
    elif isinstance(body, dict):
        for key, value in body.items():
            if isinstance(value, Matcher):
                body[key] = _convert_matcher_to_value(matcher=value)
            body[key] = _reify_pact_body(body=body[key])
        return body
    else:
        return body


@mock.patch(target="requests.Session.get")
@pytest.fixture
def mock_cloud_data_context() -> CloudDataContext:
    mock_cloud_data_context_response_body: JsonType = _reify_pact_body(
        body=DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY
    )
    with mock.patch(
        target="requests.Response.json",
        return_value=mock_cloud_data_context_response_body,
    ):
        return gx.get_context(mode="cloud")
