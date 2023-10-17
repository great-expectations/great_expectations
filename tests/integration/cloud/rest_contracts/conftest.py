from __future__ import annotations

import enum
import os
import pathlib
from typing import TYPE_CHECKING, Callable, Final, Union

import pydantic
import pytest
from pact import Consumer, Pact, Provider
from pact.matchers import Matcher
from pydantic import StrictStr
from typing_extensions import Annotated

from great_expectations.core.http import create_session

if TYPE_CHECKING:
    from requests import Session

CONSUMER: Final[str] = "great_expectations"
PROVIDER: Final[str] = "mercury"

PACT_BROKER_BASE_URL: Final[str] = "https://greatexpectations.pactflow.io"
PACT_BROKER_TOKEN: Final[str] = os.environ.get("PACT_BROKER_READ_ONLY_TOKEN")
PACT_MOCK_HOST: Final[str] = "localhost"
PACT_MOCK_PORT: Final[int] = 9292
PACT_DIR: Final[str] = str(pathlib.Path(__file__).parent.resolve())


class RequestMethods(str, enum.Enum):
    DELETE = "DELETE"
    GET = "GET"
    PATCH = "PATCH"
    POST = "POST"
    PUT = "PUT"


@pytest.fixture
def session() -> Session:
    return create_session(access_token=os.environ.get("GX_CLOUD_ACCESS_TOKEN"))


@pytest.fixture
def pact(request) -> Pact:
    pact: Pact = Consumer(CONSUMER).has_pact_with(
        Provider(PROVIDER),
        broker_base_url=PACT_BROKER_BASE_URL,
        broker_token=PACT_BROKER_TOKEN,
        host_name=PACT_MOCK_HOST,
        port=PACT_MOCK_PORT,
        pact_dir=PACT_DIR,
    )
    pact.start_service()
    yield pact
    pact.stop_service()


class ContractInteraction(pydantic.BaseModel):
    """Represents a Python API (Consumer) request and expected minimal response,
       given a state in the Cloud backend (Provider).

    The given state is something you know to be true about the Cloud backend data requested.

    Args:
        method: A string (e.g. "GET" or "POST") or attribute of the RequestMethods class representing a request method.
        upon_receiving: A string description of the type of request being made.
        given: A string description of the state of the Cloud backend data requested.
        response_status: The status code associated with the response. An integer between 100 and 599.
        response_body: A dictionary or Pact Matcher object representing the response body.
        request_body (Optional): A dictionary or Pact Matcher object representing the request body.
        request_headers (Optional): A dictionary representing the request headers.

    Returns:
        ContractInteraction
    """

    model_config = pydantic.ConfigDict(arbitrary_types_allowed=True)

    method: RequestMethods
    upon_receiving: StrictStr
    given: StrictStr
    response_status: Annotated[int, pydantic.Field(strict=True, ge=100, lt=600)]
    response_body: Union[dict, Matcher]
    request_body: Union[dict, Matcher, None] = None
    request_headers: Union[dict, None] = None


@pytest.fixture
def run_pact_test(
    session: Session,
    pact: Pact,
) -> Callable:
    def _run_pact_test(
        path: pathlib.Path,
        contract_interaction: ContractInteraction,
    ):
        request = {
            "method": contract_interaction.method,
            "path": str(path),
        }
        if contract_interaction.request_body is not None:
            request["body"] = contract_interaction.request_body
        if contract_interaction.request_headers is not None:
            request["headers"] = contract_interaction.request_headers

        response = {
            "status": contract_interaction.response_status,
            "body": contract_interaction.response_body,
        }

        (
            pact.given(provider_state=contract_interaction.given)
            .upon_receiving(scenario=contract_interaction.upon_receiving)
            .with_request(**request)
            .will_respond_with(**response)
        )

        request_url = f"http://{PACT_MOCK_HOST}:{PACT_MOCK_PORT}{path}"

        with pact:
            session.get(str(request_url))

    return _run_pact_test
