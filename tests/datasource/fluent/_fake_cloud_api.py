from __future__ import annotations

import contextlib
import json
import logging
import re
import urllib.parse
import uuid
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Dict,
    Final,
    Generator,
    Literal,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    TypedDict,
    Union,
)

import pydantic
import responses

from great_expectations.data_context.store.gx_cloud_store_backend import (
    ErrorDetail,
    ErrorPayload,
)
from great_expectations.datasource.fluent.config import GxConfig

if TYPE_CHECKING:
    from requests import PreparedRequest


LOGGER = logging.getLogger(__name__)

# ##########################
# Constants
# ##########################

MISSING: Final = object()

GX_CLOUD_MOCK_BASE_URL: Final[str] = "https://app.greatexpectations.fake.io"

DUMMY_JWT_TOKEN: Final[
    str
] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
# Can replace hardcoded ids with dynamic ones if using a regex url with responses.add_callback()
# https://github.com/getsentry/responses/tree/master#dynamic-responses
FAKE_ORG_ID: Final[str] = str(uuid.UUID("12345678123456781234567812345678"))
FAKE_DATA_CONTEXT_ID: Final[str] = str(uuid.uuid4())
FAKE_EXPECTATION_SUITE_ID: Final[str] = str(uuid.uuid4())
FAKE_CHECKPOINT_ID: Final[str] = str(uuid.uuid4())
UUID_REGEX: Final[str] = r"[a-f0-9-]{36}"

DEFAULT_HEADERS: Final[dict[str, str]] = {"content-type": "application/json"}

# ##########################
# Models & Types
# ##########################


class _DatasourceSchema(pydantic.BaseModel):
    id: Optional[str] = None
    type: Literal["datasource"]
    attributes: Dict[str, Union[Dict, str]]

    @property
    def name(self) -> str:
        return self.attributes["datasource_config"]["name"]  # type: ignore[index]


class CloudResponseSchema(pydantic.BaseModel):
    data: _DatasourceSchema

    @classmethod
    def from_datasource_json(cls, ds_payload: str | bytes) -> CloudResponseSchema:
        payload_dict = json.loads(ds_payload)
        data = {
            "id": payload_dict.get("id"),
            "type": "datasource",
            "attributes": payload_dict["data"]["attributes"],
        }

        return cls(data=data)  # type: ignore[arg-type] # pydantic type coercion


class CallbackResult(NamedTuple):
    status: int
    headers: dict[str, str]
    body: str


ErrorPayloadSchema = pydantic.create_model_from_typeddict(ErrorPayload)
ErrorPayloadSchema.update_forward_refs(ErrorDetail=ErrorDetail)


class CloudDetails(NamedTuple):
    base_url: str
    org_id: str
    access_token: str


FakeDBTypedDict = TypedDict(
    "FakeDBTypedDict",
    # using alternative syntax for creating type dict because of key names with hyphens
    # https://peps.python.org/pep-0589/#alternative-syntax
    {
        "data-context-configuration": Dict[str, Union[str, dict]],
        "DATASOURCE_NAMES": Set[str],
        "datasources": Dict[str, dict],
        "EXPECTATION_SUITE_NAMES": Set[str],
        "expectation_suites": Dict[str, dict],
        "CHECKPOINT_NAMES": Set[str],
        "checkpoints": Dict[str, dict],
    },
)

# ##########################
# Helpers
# ##########################


@pydantic.validate_arguments
def create_fake_db_seed_data(fds_config: Optional[GxConfig] = None) -> FakeDBTypedDict:
    fds_config = fds_config or GxConfig(fluent_datasources=[])
    datasource_names: set[str] = set()
    datasource_config: dict[str, dict] = {}
    datasources_by_id: dict[str, dict] = {}

    for ds in fds_config._json_dict()["fluent_datasources"]:
        name: str = ds["name"]
        datasource_names.add(name)

        id: str = str(uuid.uuid4())
        ds["id"] = id

        datasource_config[name] = ds
        datasources_by_id[id] = ds

    return {
        "DATASOURCE_NAMES": datasource_names,
        "datasources": datasources_by_id,
        "EXPECTATION_SUITE_NAMES": set(),
        "expectation_suites": {},
        "CHECKPOINT_NAMES": set(),
        "checkpoints": {},
        "data-context-configuration": {
            "anonymous_usage_statistics": {
                "data_context_id": FAKE_DATA_CONTEXT_ID,
                "enabled": False,
            },
            "datasources": datasource_config,
            "checkpoint_store_name": "default_checkpoint_store",
            "expectations_store_name": "default_expectations_store",
            "evaluation_parameter_store_name": "default_evaluation_parameter_store",
            "validations_store_name": "default_validations_store",
            "stores": {
                "default_evaluation_parameter_store": {
                    "class_name": "EvaluationParameterStore"
                },
                "default_expectations_store": {
                    "class_name": "ExpectationsStore",
                    "store_backend": {
                        "class_name": "GXCloudStoreBackend",
                        "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                        "ge_cloud_credentials": {
                            "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                            "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                        },
                        "ge_cloud_resource_type": "expectation_suite",
                        "suppress_store_backend_id": True,
                    },
                },
                "default_checkpoint_store": {
                    "class_name": "CheckpointStore",
                    "store_backend": {
                        "class_name": "GXCloudStoreBackend",
                        "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                        "ge_cloud_credentials": {
                            "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                            "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                        },
                        "ge_cloud_resource_type": "checkpoint",
                        "suppress_store_backend_id": True,
                    },
                },
                "default_validations_store": {
                    "class_name": "ValidationsStore",
                    "store_backend": {
                        "class_name": "GXCloudStoreBackend",
                        "ge_cloud_base_url": r"${GX_CLOUD_BASE_URL}",
                        "ge_cloud_credentials": {
                            "access_token": r"${GX_CLOUD_ACCESS_TOKEN}",
                            "organization_id": r"${GX_CLOUD_ORGANIZATION_ID}",
                        },
                        "ge_cloud_resource_type": "validation_result",
                        "suppress_store_backend_id": True,
                    },
                },
            },
        },
    }


# ##################################
# Cloud API Mock Callbacks
# ##################################

# WARNING: this dict should always be cleared during test teardown
_CLOUD_API_FAKE_DB: FakeDBTypedDict = {}  # type: ignore[typeddict-item] # will be assigned in `create_fake_db_seed_data`


def get_dc_configuration_cb(
    request: PreparedRequest,
) -> CallbackResult:
    if not request.url:
        raise NotImplementedError("request.url should not be empty")
    LOGGER.debug(f"{request.method} {request.url}")
    resource_path: str = request.url.split("/")[-1]
    dc_config = _CLOUD_API_FAKE_DB.get(resource_path)
    LOGGER.debug(f"GET response body -->\n{pf(dc_config, depth=2)}")
    if not dc_config:
        raise NotImplementedError(f"{resource_path} should never be empty")
    return CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(dc_config))


def get_datasource_by_id_cb(request: PreparedRequest) -> CallbackResult:
    if not request.url:
        raise NotImplementedError("request.url should not be empty")
    LOGGER.debug(f"{request.method} {request.url}")

    parsed_url = urllib.parse.urlparse(request.url)
    datasource_id = parsed_url.path.split("/")[-1]

    datasource: dict | None = _CLOUD_API_FAKE_DB["datasources"].get(datasource_id)
    if datasource:
        result = CallbackResult(
            200, headers=DEFAULT_HEADERS, body=json.dumps(datasource)
        )
    else:
        result = CallbackResult(
            404,
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "Mock 404",
                        "detail": f"Datasource {datasource_id} not found",
                        "source": None,
                    }
                ]
            ).json(),
        )
    return result


def delete_datasources_cb(
    request: PreparedRequest,
) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    datasource_id: str = parsed_url.path.split("/")[-1]  # type: ignore[arg-type,assignment]

    datasources: dict[str, dict] = _CLOUD_API_FAKE_DB["datasources"]
    deleted_ds = datasources.pop(datasource_id, None)
    print(pf(deleted_ds, depth=5))
    if deleted_ds:
        ds_name = deleted_ds["data"]["attributes"]["datasource_config"]["name"]
        _CLOUD_API_FAKE_DB["DATASOURCE_NAMES"].remove(ds_name)
        LOGGER.debug(f"Deleted datasource '{ds_name}'")
        result = CallbackResult(204, headers={}, body="")
    else:
        errors = ErrorPayloadSchema(
            errors=[{"code": "mock 404", "detail": None, "source": None}]
        )
        result = CallbackResult(404, headers=DEFAULT_HEADERS, body=errors.json())
    return result


def post_datasources_cb(
    request: PreparedRequest,
) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    ds_names: set[str] = _CLOUD_API_FAKE_DB["DATASOURCE_NAMES"]

    if not request.body:
        return CallbackResult(
            400,
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[{"code": "400", "detail": "Missing Body", "source": None}]
            ).json(),
        )

    try:
        LOGGER.debug(f"POST request body -->\n{pf(json.loads(request.body), depth=4)}")
        payload = CloudResponseSchema.from_datasource_json(request.body)

        datasource_name: str = payload.data.name
        if datasource_name not in ds_names:
            datasource_id = payload.data.id
            if not datasource_id:
                datasource_id = str(uuid.uuid4())
                payload.data.id = datasource_id
            assert (
                datasource_id not in _CLOUD_API_FAKE_DB["datasources"]
            ), f"ID collision for '{datasource_name}'"

            _CLOUD_API_FAKE_DB["datasources"][datasource_id] = payload.dict()
            _CLOUD_API_FAKE_DB["DATASOURCE_NAMES"].add(payload.data.name)

            result = CallbackResult(201, headers=DEFAULT_HEADERS, body=payload.json())
        else:
            errors = ErrorPayloadSchema(
                errors=[
                    {
                        "code": "mock 400/409",
                        "detail": f"Datasource with name '{datasource_name}' already exists.",
                        "source": None,
                    }
                ]
            )
            result = CallbackResult(409, headers=DEFAULT_HEADERS, body=errors.json())

        return result
    except pydantic.ValidationError as val_err:
        LOGGER.exception(val_err)
        return CallbackResult(
            400,
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "mock 400",
                        "detail": str(val_err.errors()),
                        "source": None,
                    }
                ]
            ).json(),
        )
    except Exception as err:
        return CallbackResult(
            500,
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[{"code": "mock 500", "detail": repr(err), "source": None}]
            ).json(),
        )


def put_datasource_cb(request: PreparedRequest) -> CallbackResult:
    LOGGER.debug(f"{request.method} {request.url}")
    if not request.url:
        raise NotImplementedError("request.url should not be empty")

    if not request.body:
        errors = ErrorPayloadSchema(
            errors=[{"code": "mock 400", "detail": "missing body", "source": None}]
        )
        return CallbackResult(400, headers=DEFAULT_HEADERS, body=errors.json())

    LOGGER.debug(f"PUT request body -->\n{pf(json.loads(request.body), depth=4)}")
    payload = CloudResponseSchema.from_datasource_json(request.body)

    parsed_url = urllib.parse.urlparse(request.url)
    datasource_id = parsed_url.path.split("/")[-1]

    old_datasource: dict | None = _CLOUD_API_FAKE_DB["datasources"].get(datasource_id)
    if old_datasource:
        if (
            payload.data.name
            != old_datasource["data"]["attributes"]["datasource_config"]["name"]
        ):
            raise NotImplementedError("Unsure how to handle name change")
        _CLOUD_API_FAKE_DB["datasources"][datasource_id] = payload.dict()
        result = CallbackResult(200, headers=DEFAULT_HEADERS, body=payload.json())
    else:
        result = CallbackResult(404, headers=DEFAULT_HEADERS, body="")
    return result


def get_datasources_cb(
    request: PreparedRequest,
) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)  # type: ignore[type-var]
    queried_names: Sequence[str] = query_params.get("name", [])  # type: ignore[assignment]

    all_datasources: dict[str, dict] = _CLOUD_API_FAKE_DB["datasources"]
    datasources_list: list[dict] = list(all_datasources.values())
    if queried_names:
        datasources_list = [
            d
            for d in datasources_list
            if d["data"]["attributes"]["datasource_config"]["name"] in queried_names
        ]

    resp_body = {"data": datasources_list}
    result = CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(resp_body))
    LOGGER.debug(f"Response {result.status}")
    return result


def get_expectation_suites_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)  # type: ignore[type-var]
    queried_names: Sequence[str] = query_params.get("name", [])  # type: ignore[assignment]

    exp_suites: dict[str, dict] = _CLOUD_API_FAKE_DB["expectation_suites"]
    exp_suite_list: list[dict] = list(exp_suites.values())
    if queried_names:
        exp_suite_list = [
            d
            for d in exp_suite_list
            if d["data"]["attributes"]["suite"]["expectation_suite_name"]
            in queried_names
        ]

    resp_body = {"data": exp_suite_list}

    result = CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(resp_body))
    LOGGER.debug(f"Response {result.status}")
    return result


def get_expectation_suite_by_id_cb(
    request: PreparedRequest,
) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    expectation_id: str = parsed_url.path.split("/")[-1]  # type: ignore[arg-type,assignment]

    expectation_suite: dict | None = _CLOUD_API_FAKE_DB["expectation_suites"].get(
        expectation_id
    )
    if expectation_suite:
        result = CallbackResult(
            200, headers=DEFAULT_HEADERS, body=json.dumps(expectation_suite)
        )
    else:
        result = CallbackResult(404, headers=DEFAULT_HEADERS, body="")
    return result


def post_expectation_suites_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    if not request.body:
        raise NotImplementedError("Handling missing body")

    payload: dict = json.loads(request.body)
    name = payload["data"]["attributes"]["suite"]["expectation_suite_name"]

    exp_suite_names: set[str] = _CLOUD_API_FAKE_DB["EXPECTATION_SUITE_NAMES"]
    exp_suites: dict[str, dict] = _CLOUD_API_FAKE_DB["expectation_suites"]

    if name in exp_suite_names:
        result = CallbackResult(
            409,  # not really a 409 in prod but it's a more informative status code
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "mock 400/409",
                        "detail": f"'{name}' already defined",
                        "source": None,
                    }
                ]
            ).json(),
        )
    else:
        id_ = FAKE_EXPECTATION_SUITE_ID
        payload["data"]["id"] = id_
        exp_suites[id_] = payload
        exp_suite_names.add(name)
        result = CallbackResult(201, headers=DEFAULT_HEADERS, body=json.dumps(payload))

    LOGGER.debug(f"Response {result.status}")
    return result


def get_checkpoints_cb(requests: PreparedRequest) -> CallbackResult:
    url = requests.url
    LOGGER.debug(f"{requests.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)  # type: ignore[type-var]
    queried_names: Sequence[str] = query_params.get("name", [])  # type: ignore[assignment]

    checkpoints: dict[str, dict] = _CLOUD_API_FAKE_DB["checkpoints"]
    checkpoint_list: list[dict] = list(checkpoints.values())
    if queried_names:
        checkpoint_list = [
            d
            for d in checkpoint_list
            if d["data"]["attributes"]["checkpoint_name"] in queried_names
        ]

    resp_body = {"data": checkpoint_list}

    result = CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(resp_body))
    LOGGER.debug(f"Response {result.status}")
    return result


def get_checkpoint_by_id_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    checkpoint_id: str = parsed_url.path.split("/")[-1]  # type: ignore[arg-type,assignment]

    if checkpoint := _CLOUD_API_FAKE_DB["checkpoints"].get(checkpoint_id):
        result = CallbackResult(
            200, headers=DEFAULT_HEADERS, body=json.dumps(checkpoint)
        )
    else:
        result = CallbackResult(
            404,
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "mock 404",
                        "detail": f"Checkpoint '{checkpoint_id}' not found",
                        "source": None,
                    }
                ]
            ).json(),
        )
    return result


def post_checkpoints_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    if not request.body:
        raise NotImplementedError("Handling missing body")

    payload: dict = json.loads(request.body)
    name = payload["data"]["attributes"]["checkpoint_config"]["name"]

    checkpoints: dict[str, dict] = _CLOUD_API_FAKE_DB["checkpoints"]
    checkpoint_names: set[str] = _CLOUD_API_FAKE_DB["CHECKPOINT_NAMES"]

    if name in checkpoint_names:
        result = CallbackResult(
            409,  # not really a 409 in prod but it's a more informative status code
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "mock 400/409",
                        "detail": f"'{name}' already defined",
                        "source": None,
                    }
                ]
            ).json(),
        )
    else:
        id_ = FAKE_CHECKPOINT_ID
        payload["data"]["id"] = id_
        checkpoints[id_] = payload
        checkpoint_names.add(name)
        result = CallbackResult(201, headers=DEFAULT_HEADERS, body=json.dumps(payload))

    LOGGER.debug(f"Response {result.status}")
    return result


def post_validation_results_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    if not request.body:
        raise NotImplementedError("Handling missing body")

    payload: dict = json.loads(request.body)
    validation_id = payload["data"]["attributes"]["result"]["meta"]["validation_id"]
    if validation_id:
        raise NotImplementedError("TODO: Handling the validation_id success case")
    else:
        result = CallbackResult(
            422,  # 400 in prod but this is a more informative status code
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "Mock 400/422",
                        "detail": "Field may not be null.",
                        "source": {
                            "pointer": "/data/attributes/result/meta/validation_id"
                        },
                    }
                ]
            ).json(),
        )

    LOGGER.debug(f"Response {result.status}")
    return result


# #######################
# RequestMocks
# #######################


@contextlib.contextmanager
def gx_cloud_api_fake_ctx(
    cloud_details: CloudDetails,
    fds_config: GxConfig | None = None,
    assert_all_requests_are_fired: bool = False,
) -> Generator[responses.RequestsMock, None, None]:
    """Mock the GX Cloud API for the lifetime of the context manager."""
    org_url_base = f"{cloud_details.base_url}/organizations/{cloud_details.org_id}"
    dc_config_url = f"{org_url_base}/data-context-configuration"

    assert not _CLOUD_API_FAKE_DB, "_CLOUD_API_FAKE_DB should be empty"
    _CLOUD_API_FAKE_DB.update(create_fake_db_seed_data(fds_config))

    LOGGER.info("Mocking the GX Cloud API")

    with responses.RequestsMock(
        assert_all_requests_are_fired=assert_all_requests_are_fired
    ) as resp_mocker:
        resp_mocker.add_callback(responses.GET, dc_config_url, get_dc_configuration_cb)
        resp_mocker.add_callback(
            responses.GET,
            f"{org_url_base}/datasources",
            get_datasources_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            f"{org_url_base}/datasources",
            post_datasources_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            re.compile(f"{org_url_base}/datasources/{UUID_REGEX}"),
            get_datasource_by_id_cb,
        )
        resp_mocker.add_callback(
            responses.DELETE,
            re.compile(f"{org_url_base}/datasources/{UUID_REGEX}"),
            delete_datasources_cb,
        )
        resp_mocker.add_callback(
            responses.PUT,
            re.compile(f"{org_url_base}/datasources/{UUID_REGEX}"),
            put_datasource_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            f"{org_url_base}/expectation-suites",
            get_expectation_suites_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            f"{org_url_base}/expectation-suites/{FAKE_EXPECTATION_SUITE_ID}",
            get_expectation_suite_by_id_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            f"{org_url_base}/expectation-suites",
            post_expectation_suites_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            f"{org_url_base}/checkpoints",
            get_checkpoints_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            f"{org_url_base}/checkpoints",
            post_checkpoints_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            f"{org_url_base}/checkpoints/{FAKE_CHECKPOINT_ID}",
            get_checkpoint_by_id_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            f"{org_url_base}/validation-results",
            post_validation_results_cb,
        )

        yield resp_mocker

    LOGGER.info(f"GX Cloud API Mock ending state ->\n{pf(_CLOUD_API_FAKE_DB, depth=2)}")
    _CLOUD_API_FAKE_DB.clear()
