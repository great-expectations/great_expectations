from __future__ import annotations

from typing import TYPE_CHECKING, Final

import pact
import pytest

from great_expectations.data_context import CloudDataContext
from tests.integration.cloud.rest_contracts.conftest import (
    EXISTING_ORGANIZATION_ID,
)

if TYPE_CHECKING:
    from requests import Session

    from tests.integration.cloud.rest_contracts.conftest import PactBody


NON_EXISTENT_DATASOURCE_ID: Final[str] = "6ed9a340-8469-4ee2-a300-ffbe5d09b49d"

EXISTING_DATASOURCE_ID: Final[str] = "15da041b-328e-44f7-892e-2bfd1a887ef8"


POST_DATASOURCE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.Like(
        {
            "id": pact.Format().uuid,
            "attributes": {
                "datasource_config": {},
            },
        },
    )
}

GET_DATASOURCE_NAME: Final[str] = "david_datasource"

GET_DATASOURCE_MIN_RESPONSE_BODY: Final[PactBody] = {
    "data": pact.Like(
        {
            "id": pact.Format().uuid,
            "type": "datasource",
            "attributes": {
                # pact doesn't test optional attributes like an empty "assets" list
                # this is the minimum response. https://docs.pact.io/faq#why-is-there-no-support-for-specifying-optional-attributes
                "datasource_config": pact.Like(
                    {
                        "id": pact.Format().uuid,
                        "name": GET_DATASOURCE_NAME,
                        "type": "pandas",
                    }
                ),
            },
        },
    )
}


@pytest.mark.cloud
def test_get_expectation_suite(
    pact_test: pact.Pact,
    cloud_data_context: CloudDataContext,
    gx_cloud_session: Session,
) -> None:
    provider_state = "the Data Source exists"
    scenario = "a request to get a Data Source"
    method = "GET"
    path = f"/organizations/{EXISTING_ORGANIZATION_ID}/datasources/{EXISTING_DATASOURCE_ID}"
    status = 200
    response_body = GET_DATASOURCE_MIN_RESPONSE_BODY

    (
        pact_test.given(provider_state=provider_state)
        .upon_receiving(scenario=scenario)
        .with_request(
            method=method,
            path=path,
            headers=dict(gx_cloud_session.headers),
        )
        .will_respond_with(
            status=status,
            body=response_body,
        )
    )

    with pact_test:
        cloud_data_context.get_datasource(datasource_name=GET_DATASOURCE_NAME)
