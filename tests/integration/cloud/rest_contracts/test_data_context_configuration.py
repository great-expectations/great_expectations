from __future__ import annotations

import pathlib
import uuid
from typing import Any, Callable, Final

import pytest
from pact import Like
from pact.matchers import Matcher

from tests.integration.cloud.rest_contracts.conftest import (
    ContractInteraction,
    MinimumResponseBody,
)


def _convert_matcher_to_value(matcher: Matcher) -> Any:
    return matcher.generate()["contents"]


def reify_pact_response_body(
    response_body: MinimumResponseBody,
) -> dict:
    if isinstance(response_body, list):
        for index, item in enumerate(response_body):
            if isinstance(item, Matcher):
                response_body[index] = _convert_matcher_to_value(matcher=item)
            response_body[index] = reify_pact_response_body(
                response_body=response_body[index]
            )
        return response_body
    elif isinstance(response_body, Matcher):
        return reify_pact_response_body(
            response_body=_convert_matcher_to_value(matcher=response_body)
        )
    elif isinstance(response_body, dict):
        for key, value in response_body.items():
            if isinstance(value, Matcher):
                response_body[key] = _convert_matcher_to_value(matcher=value)
            response_body[key] = reify_pact_response_body(
                response_body=response_body[key]
            )
        return response_body
    else:
        return response_body


DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY: Final[dict] = {
    "anonymous_usage_statistics": Like(
        {
            "data_context_id": str(uuid.uuid4()),
            "enabled": True,
        }
    ),
    "datasources": Like({}),
    "include_rendered_content": {
        "globally": True,
        "expectation_validation_result": True,
        "expectation_suite": True,
    },
}


def test_min_response_body_to_dict() -> None:
    test = reify_pact_response_body(
        response_body=DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY
    )
    assert test == {
        "anonymous_usage_statistics": {"data_context_id": None, "enabled": True},
        "datasources": {},
        "include_rendered_content": {
            "expectation_suite": True,
            "expectation_validation_result": True,
            "globally": True,
        },
    }


@pytest.mark.cloud
@pytest.mark.parametrize(
    "contract_interaction",
    [
        ContractInteraction(
            method="GET",
            upon_receiving="a request for a Data Context",
            given="the Data Context exists",
            response_status=200,
            response_body=DATA_CONTEXT_CONFIGURATION_MIN_RESPONSE_BODY,
        ),
    ],
)
def test_data_context_configuration(
    contract_interaction: ContractInteraction,
    run_pact_test: Callable[[pathlib.Path, ContractInteraction], None],
    existing_organization_id: str,
) -> None:
    # the path to the endpoint relative to the base url
    path = pathlib.Path(
        "/", "organizations", existing_organization_id, "data-context-configuration"
    )
    run_pact_test(path, contract_interaction)
