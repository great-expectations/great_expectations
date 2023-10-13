from __future__ import annotations

import os
from typing import Any, Callable, Final

import pytest
from pact import Like

ORGANIZATION_ID: Final[str] = os.environ.get("GX_CLOUD_ORGANIZATION_ID")


@pytest.mark.cloud
@pytest.mark.parametrize(
    ["method", "upon_receiving", "given", "response_status", "response_body"],
    [
        (
            "GET",
            "a request for a Data Context",
            "the Data Context exists",
            200,
            Like(
                {
                    "anonymous_usage_statistics": {
                        "data_context_id": ORGANIZATION_ID,
                        "enabled": False,
                    },
                    "config_version": 3,
                    "datasources": {},
                    "include_rendered_content": {
                        "globally": True,
                        "expectation_validation_result": True,
                        "expectation_suite": True,
                    },
                    "stores": {},
                }
            ),
        ),
    ],
)
def test_data_context(
    method: str,
    upon_receiving: str,
    given: str,
    response_status: int,
    response_body: Any,
    run_pact_test: Callable,
):
    path = f"/organizations/{ORGANIZATION_ID}/data-context-configuration"
    run_pact_test(
        path=path,
        method=method,
        upon_receiving=upon_receiving,
        given=given,
        response_status=response_status,
        response_body=response_body,
    )
