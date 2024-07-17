from __future__ import annotations

import enum
import os
import pathlib
import subprocess
import uuid
from typing import TYPE_CHECKING, Any, Callable, Dict, Final, List, Union

import pact
import pytest
from typing_extensions import Annotated, TypeAlias  # noqa: TCH002

from great_expectations.compatibility import pydantic
from great_expectations.core.http import create_session
from great_expectations.data_context import CloudDataContext

if TYPE_CHECKING:
    from requests import Session


CONSUMER_NAME: Final[str] = "great_expectations"
PROVIDER_NAME: Final[str] = "mercury"


PACT_MOCK_HOST: Final[str] = "localhost"
PACT_MOCK_PORT: Final[int] = 9292
PACT_DIR: Final[pathlib.Path] = pathlib.Path(
    pathlib.Path(__file__, ".."), "pacts"
).resolve()
PACT_MOCK_SERVICE_URL: Final[str] = f"http://{PACT_MOCK_HOST}:{PACT_MOCK_PORT}"


JsonData: TypeAlias = Union[None, int, str, bool, List[Any], Dict[str, Any]]

PactBody: TypeAlias = Union[
    Dict[str, Union[JsonData, pact.matchers.Matcher]], pact.matchers.Matcher, None
]


EXISTING_ORGANIZATION_ID: Final[str] = os.environ.get("GX_CLOUD_ORGANIZATION_ID", "")


class RequestMethods(str, enum.Enum):
    DELETE = "DELETE"
    GET = "GET"
    PATCH = "PATCH"
    POST = "POST"
    PUT = "PUT"


@pytest.fixture
def cloud_base_url() -> str:
    try:
        return os.environ["GX_CLOUD_BASE_URL"]
    except KeyError as e:
        raise OSError("GX_CLOUD_BASE_URL is not set in this environment.") from e


@pytest.fixture
def cloud_access_token() -> str:
    try:
        return os.environ["GX_CLOUD_ACCESS_TOKEN"]
    except KeyError as e:
        raise OSError("GX_CLOUD_ACCESS_TOKEN is not set in this environment.") from e


@pytest.fixture(scope="module")
def gx_cloud_session() -> Session:
    try:
        access_token = os.environ["GX_CLOUD_ACCESS_TOKEN"]
    except KeyError as e:
        raise OSError("GX_CLOUD_ACCESS_TOKEN is not set in this environment.") from e
    return create_session(access_token=access_token)


@pytest.fixture
def cloud_data_context(
    cloud_base_url: str,
    cloud_access_token: str,
) -> CloudDataContext:
    """This is a real Cloud Data Context that points to the pact mock service instead of the Mercury API."""
    cloud_data_context = CloudDataContext(
        cloud_base_url=cloud_base_url,
        cloud_organization_id=EXISTING_ORGANIZATION_ID,
        cloud_access_token=cloud_access_token,
    )
    # we can't override the base url to use the mock service due to
    # reliance on env vars, so instead we override with a real project config
    project_config = cloud_data_context.config
    return CloudDataContext(
        cloud_base_url=PACT_MOCK_SERVICE_URL,
        cloud_organization_id=EXISTING_ORGANIZATION_ID,
        cloud_access_token=cloud_access_token,
        project_config=project_config,
    )


def get_git_commit_hash() -> str:
    return subprocess.check_output(["git", "rev-parse", "HEAD"]).decode("ascii").strip()


@pytest.fixture(scope="package")
def pact_test(request) -> pact.Pact:
    """
    pact_test can be used as a context manager and will:
    1. write a new contract to the pact dir
    2. verify the contract against the mock service
    """
    pact_broker_base_url = "https://greatexpectations.pactflow.io"

    broker_token: str
    publish_to_broker: bool
    if os.environ.get("PACT_BROKER_READ_WRITE_TOKEN"):
        broker_token = os.environ.get("PACT_BROKER_READ_WRITE_TOKEN", "")
        publish_to_broker = True
    elif os.environ.get("PACT_BROKER_READ_ONLY_TOKEN"):
        broker_token = os.environ.get("PACT_BROKER_READ_ONLY_TOKEN", "")
        publish_to_broker = False
    else:
        pytest.skip(
            "no pact credentials: set PACT_BROKER_READ_ONLY_TOKEN from greatexpectations.pactflow.io"
        )

    # Adding random id to the commit hash allows us to run the build
    # and publish the contract more than once for a given commit.
    # We need this because we have the ability to trigger re-run of tests
    # in GH, and we run the release build process on the tagged commit.
    version = f"{get_git_commit_hash()}_{str(uuid.uuid4())[:5]}"

    _pact: pact.Pact = pact.Consumer(
        name=CONSUMER_NAME,
        version=version,
        tag_with_git_branch=True,
        auto_detect_version_properties=True,
    ).has_pact_with(
        pact.Provider(name=PROVIDER_NAME),
        broker_base_url=pact_broker_base_url,
        broker_token=broker_token,
        host_name=PACT_MOCK_HOST,
        port=PACT_MOCK_PORT,
        pact_dir=str(PACT_DIR),
        publish_to_broker=publish_to_broker,
    )

    _pact.start_service()
    yield _pact
    _pact.stop_service()


class ContractInteraction(pydantic.BaseModel):
    """Represents a Python API (Consumer) request and expected minimal response,
       given a state in the Cloud backend (Provider).

    The given state is something you know to be true about the Cloud backend data requested.

    Args:
        method: A string (e.g. "GET" or "POST") or attribute of the RequestMethods class representing a request method.
        request_path: A pathlib.Path to the endpoint relative to the base url.
                        e.g.
                            ```
                            path = pathlib.Path(
                                "/", "organizations", organization_id, "data-context-configuration"
                            )
                            ```
        upon_receiving: A string description of the type of request being made.
        given: A string description of the state of the Cloud backend data requested.
        response_status: The status code associated with the response. An integer between 100 and 599.
        response_body: A dictionary or Pact Matcher object representing the response body.
        request_body (Optional): A dictionary or Pact Matcher object representing the request body.
        request_headers (Optional): A dictionary representing the request headers.
        request_parmas (Optional): A dictionary representing the request parameters.

    Returns:
        ContractInteraction
    """

    class Config:
        arbitrary_types_allowed = True

    method: Union[RequestMethods, pydantic.StrictStr]
    request_path: pathlib.Path
    upon_receiving: pydantic.StrictStr
    given: pydantic.StrictStr
    response_status: Annotated[int, pydantic.Field(strict=True, ge=100, lt=600)]
    response_body: PactBody
    request_body: Union[PactBody, None] = None
    request_headers: Union[dict, None] = None
    request_params: Union[dict, None] = None


@pytest.fixture
def run_rest_api_pact_test(
    gx_cloud_session: Session,
    pact_test: pact.Pact,
) -> Callable:
    def _run_pact_test(
        contract_interaction: ContractInteraction,
    ) -> None:
        """Runs a contract test and produces a Pact contract json file in directory:
            - tests/integration/cloud/rest_contracts/pacts

        Args:
            contract_interaction: A ContractInteraction object which represents a Python API (Consumer) request
                                  and expected minimal response, given a state in the Cloud backend (Provider).

        Returns:
            None
        """

        request: dict[str, str | PactBody] = {
            "method": contract_interaction.method,
            "path": str(contract_interaction.request_path),
        }
        if contract_interaction.request_body is not None:
            request["body"] = contract_interaction.request_body
        if contract_interaction.request_params is not None:
            request["query"] = contract_interaction.request_params

        request["headers"] = dict(gx_cloud_session.headers)
        if contract_interaction.request_headers is not None:
            request["headers"].update(contract_interaction.request_headers)  # type: ignore[union-attr]
            gx_cloud_session.headers.update(contract_interaction.request_headers)

        response: dict[str, int | PactBody] = {
            "status": contract_interaction.response_status,
        }
        if contract_interaction.response_body is not None:
            response["body"] = contract_interaction.response_body

        (
            pact_test.given(provider_state=contract_interaction.given)
            .upon_receiving(scenario=contract_interaction.upon_receiving)
            .with_request(**request)
            .will_respond_with(**response)
        )

        request_url = f"http://{PACT_MOCK_HOST}:{PACT_MOCK_PORT}{contract_interaction.request_path}"

        with pact_test:
            # act
            resp = gx_cloud_session.request(
                method=contract_interaction.method,
                url=request_url,
                json=contract_interaction.request_body,
                params=contract_interaction.request_params,
            )

        # assert
        assert resp.status_code == contract_interaction.response_status
        # TODO more unit test assertions would go here e.g. response body checks

    return _run_pact_test
