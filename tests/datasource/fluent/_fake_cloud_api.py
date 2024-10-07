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
    List,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    TypedDict,
    Union,
)

import responses

from great_expectations.compatibility import pydantic
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

GX_CLOUD_MOCK_BASE_URL: Final[str] = "https://app.greatexpectations.fake.io/"

DUMMY_JWT_TOKEN: Final[str] = (
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
)
# Can replace hardcoded ids with dynamic ones if using a regex url with responses.add_callback()
# https://github.com/getsentry/responses/tree/master#dynamic-responses
FAKE_USER_ID: Final[str] = "00000000-0000-0000-0000-000000000000"
FAKE_ORG_ID: Final[str] = str(uuid.UUID("12345678123456781234567812345678"))
FAKE_DATA_CONTEXT_ID: Final[str] = str(uuid.uuid4())
UUID_REGEX: Final[str] = r"[a-f0-9-]{36}"

DEFAULT_HEADERS: Final[dict[str, str]] = {"content-type": "application/json"}

# ##########################
# Models & Types
# ##########################


class _DatasourceSchema(pydantic.BaseModel, extra="allow"):
    id: Optional[str] = None
    type: str
    name: str
    assets: List[dict] = pydantic.Field(default_factory=list)


class CloudResponseSchema(pydantic.BaseModel):
    data: _DatasourceSchema

    @classmethod
    def from_datasource_json(cls, ds_payload: str | bytes) -> CloudResponseSchema:
        data = json.loads(ds_payload)
        return cls(**data)


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
        "me": Dict[str, str],
        "data-context-configuration": Dict[str, Union[str, dict]],
        "DATASOURCE_NAMES": Set[str],
        "datasources": Dict[str, dict],
        "EXPECTATION_SUITE_NAMES": Set[str],
        "expectation_suites": Dict[str, dict],
        "CHECKPOINT_NAMES": Set[str],
        "checkpoints": Dict[str, dict],
        "VALIDATION_DEFINITION_NAMES": Set[str],
        "validation_definitions": Dict[str, dict],
    },
)

# ##########################
# Helpers
# ##########################


@pydantic.validate_arguments
def create_fake_db_seed_data(fds_config: Optional[GxConfig] = None) -> FakeDBTypedDict:
    fds_config = fds_config or GxConfig(fluent_datasources=[])
    datasource_names: set[str] = set()
    datasource_config: dict[str, dict | str] = {}
    datasources_by_id: dict[str, dict] = {}

    for ds in fds_config._json_dict()["fluent_datasources"]:
        name: str = ds["name"]
        datasource_names.add(name)

        id: str = str(uuid.uuid4())
        ds["id"] = id

        ds_response_json = {"data": ds}

        datasource_config[name] = name
        datasources_by_id[id] = ds_response_json

    return {
        "me": {"user_id": FAKE_USER_ID},
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
            "validation_results_store_name": "default_validation_results_store",
            "stores": {
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
                "default_validation_results_store": {
                    "class_name": "ValidationResultsStore",
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
        "VALIDATION_DEFINITION_NAMES": set(),
        "validation_definitions": {},
    }


# ##################################
# Cloud API Mock Callbacks
# ##################################

# WARNING: this dict should always be cleared during test teardown
_CLOUD_API_FAKE_DB: FakeDBTypedDict = {}  # type: ignore[typeddict-item] # will be assigned in `create_fake_db_seed_data`


def get_user_id(request: PreparedRequest) -> CallbackResult:
    if not request.url:
        raise NotImplementedError("request.url should not be empty")
    LOGGER.debug(f"{request.method} {request.url}")
    resource_path: str = request.url.split("/")[-1]
    user_dict = _CLOUD_API_FAKE_DB.get(resource_path)
    return CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(user_dict))


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
        result = CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(datasource))
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
    if deleted_ds:
        ds_name = deleted_ds["data"]["name"]
        _CLOUD_API_FAKE_DB["DATASOURCE_NAMES"].remove(ds_name)
        LOGGER.debug(f"Deleted datasource '{ds_name}'")
        result = CallbackResult(204, headers={}, body="")
    else:
        errors = ErrorPayloadSchema(errors=[{"code": "mock 404", "detail": None, "source": None}])
        result = CallbackResult(404, headers=DEFAULT_HEADERS, body=errors.json())
    return result


def delete_data_assets_cb(
    request: PreparedRequest,
) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    data_asset_id: str = parsed_url.path.split("/")[-1]  # type: ignore[arg-type,assignment]

    datasources: dict[str, dict] = _CLOUD_API_FAKE_DB["datasources"]
    deleted_asset_idx = None
    deleted_asset = None

    # find and remove asset from datasource config
    for datasource in datasources.values():
        for idx, asset in enumerate(datasource["data"].get("assets", {})):
            if asset.get("id") == data_asset_id:
                deleted_asset_idx = idx
                break
        if deleted_asset_idx is not None:
            deleted_asset = datasource["data"]["assets"].pop(deleted_asset_idx)
            break

    if deleted_asset:
        asset_name = deleted_asset["name"]
        LOGGER.debug(f"Deleted asset '{asset_name}'")
        result = CallbackResult(204, headers={}, body="")
    else:
        errors = ErrorPayloadSchema(errors=[{"code": "mock 404", "detail": None, "source": None}])
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
        LOGGER.exception(val_err)  # noqa: TRY401
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


def put_datasource_cb(request: PreparedRequest) -> CallbackResult:  # noqa: C901
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

    # Ensure that assets and batch definitions get IDs
    # Note that this logic should also happen in POST but is not implemented for our fake
    for asset in payload.data.assets:
        if not asset.get("id"):
            asset["id"] = str(uuid.uuid4())
        for batch_definition in asset.get("batch_definitions", []):
            if not batch_definition.get("id"):
                batch_definition["id"] = str(uuid.uuid4())

    old_datasource: dict | None = _CLOUD_API_FAKE_DB["datasources"].get(datasource_id)
    if old_datasource:
        if payload.data.name != old_datasource["data"]["name"]:
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
            d["data"] for d in datasources_list if d["data"]["name"] in queried_names
        ]
    else:
        datasources_list = [d["data"] for d in datasources_list]

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
    exp_suite_list: list[dict] = [d["data"] for d in exp_suites.values()]
    if queried_names:
        exp_suite_list = [d for d in exp_suite_list if d["name"] in queried_names]

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

    expectation_suite: dict | None = _CLOUD_API_FAKE_DB["expectation_suites"].get(expectation_id)
    if expectation_suite:
        result = CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(expectation_suite))
    else:
        result = CallbackResult(404, headers=DEFAULT_HEADERS, body="")
    return result


def post_expectation_suites_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    if not request.body:
        raise NotImplementedError("Handling missing body")

    payload: dict = json.loads(request.body)
    name = payload["data"]["name"]

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
        suite_id = str(uuid.uuid4())
        payload["data"]["id"] = suite_id
        payload["data"]["id"] = suite_id
        for expectation_configuration in payload["data"]["expectations"]:
            expectation_configuration["id"] = str(uuid.uuid4())
        exp_suites[suite_id] = payload
        exp_suite_names.add(name)
        result = CallbackResult(201, headers=DEFAULT_HEADERS, body=json.dumps(payload))

    LOGGER.debug(f"Response {result.status}")
    return result


def put_expectation_suites_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    if not request.body:
        errors = ErrorPayloadSchema(
            errors=[{"code": "mock 400", "detail": "missing body", "source": None}]
        )
        return CallbackResult(400, headers=DEFAULT_HEADERS, body=errors.json())

    payload: dict = json.loads(request.body)
    parsed_url = urllib.parse.urlparse(request.url)
    suite_id: str = parsed_url.path.split("/")[-1]  # type: ignore[arg-type,assignment]

    name = payload["data"]["name"]

    exp_suite_names: set[str] = _CLOUD_API_FAKE_DB["EXPECTATION_SUITE_NAMES"]
    exp_suites: dict[str, dict] = _CLOUD_API_FAKE_DB["expectation_suites"]

    old_suite = exp_suites.get(suite_id)

    if not old_suite:
        result = CallbackResult(
            404,  # not really a 409 in prod but it's a more informative status code
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "404",
                        "detail": "Suite not found",
                        "source": None,
                    }
                ]
            ).json(),
        )
    else:
        payload["data"]["id"] = suite_id
        payload["data"]["id"] = suite_id
        for expectation_configuration in payload["data"]["expectations"]:
            # add IDs to new expectations
            if not expectation_configuration.get("id"):
                expectation_configuration["id"] = str(uuid.uuid4())
        exp_suites[suite_id] = payload
        exp_suite_names.add(name)
        result = CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(payload))

    LOGGER.debug(f"Response {result.status}")
    return result


def delete_expectation_suites_cb(request: PreparedRequest) -> CallbackResult:
    parsed_url = urllib.parse.urlparse(request.url)
    suite_id: str = parsed_url.path.split("/")[-1]  # type: ignore[arg-type,assignment]
    exp_suites: dict[str, dict] = _CLOUD_API_FAKE_DB["expectation_suites"]
    old_suite = exp_suites.pop(suite_id, None)
    if not old_suite:
        result = CallbackResult(
            404,
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "404",
                        "detail": "Suite not found",
                        "source": None,
                    }
                ]
            ).json(),
        )
    else:
        result = CallbackResult(204, headers={}, body="")
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
        checkpoint_list = [d for d in checkpoint_list if d["name"] in queried_names]

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
        result = CallbackResult(200, headers=DEFAULT_HEADERS, body=json.dumps(checkpoint))
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
    name = payload["data"]["name"]

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
        id_ = str(uuid.uuid4())
        payload["data"]["id"] = id_
        checkpoints[id_] = payload["data"]
        checkpoint_names.add(name)
        result = CallbackResult(201, headers=DEFAULT_HEADERS, body=json.dumps(payload))

    LOGGER.debug(f"Response {result.status}")
    return result


def delete_checkpoint_by_id_cb(
    request: PreparedRequest,
) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    path = str(parsed_url.path)
    checkpoint_id = path.split("/")[-1]
    checkpoints: dict[str, dict] = _CLOUD_API_FAKE_DB["checkpoints"]

    deleted_cp = checkpoints.pop(checkpoint_id, None)
    if not deleted_cp:
        errors = ErrorPayloadSchema(errors=[{"code": "mock 404", "detail": None, "source": None}])
        return CallbackResult(404, headers=DEFAULT_HEADERS, body=errors.json())

    print(pf(deleted_cp, depth=5))
    cp_name = deleted_cp["name"]
    _CLOUD_API_FAKE_DB["CHECKPOINT_NAMES"].remove(cp_name)
    LOGGER.debug(f"Deleted checkpoint '{cp_name}'")
    return CallbackResult(204, headers={}, body="")


def delete_checkpoint_by_name_cb(
    request: PreparedRequest,
) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)  # type: ignore[type-var]
    queried_names: list[str] = query_params.get("name", [])  # type: ignore[assignment]
    if not queried_names:
        raise ValueError("Must provide checkpoint name for deletion.")

    cp_name = queried_names[0]
    checkpoints: dict[str, dict] = _CLOUD_API_FAKE_DB["checkpoints"]

    checkpoint_id: str | None = None
    for checkpoint in checkpoints.values():
        if checkpoint["name"] == cp_name:
            checkpoint_id = checkpoint["id"]
            break

    if not checkpoint_id:
        errors = ErrorPayloadSchema(errors=[{"code": "mock 404", "detail": None, "source": None}])
        return CallbackResult(404, headers=DEFAULT_HEADERS, body=errors.json())

    deleted_cp = checkpoints.pop(checkpoint_id)
    print(pf(deleted_cp, depth=5))

    _CLOUD_API_FAKE_DB["CHECKPOINT_NAMES"].remove(cp_name)
    LOGGER.debug(f"Deleted checkpoint '{cp_name}'")
    return CallbackResult(204, headers={}, body="")


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
                        "source": {"pointer": "/data/attributes/result/meta/validation_id"},
                    }
                ]
            ).json(),
        )

    LOGGER.debug(f"Response {result.status}")
    return result


def get_validation_definition_by_id_cb(request: PreparedRequest) -> CallbackResult:
    if not request.url:
        raise NotImplementedError("request.url should not be empty")
    LOGGER.debug(f"{request.method} {request.url}")

    parsed_url = urllib.parse.urlparse(request.url)
    validation_id = parsed_url.path.split("/")[-1]

    validation_definition: dict | None = _CLOUD_API_FAKE_DB["validation_definitions"].get(
        validation_id
    )
    if validation_definition:
        validation_definition = {"data": validation_definition}
        result = CallbackResult(
            200, headers=DEFAULT_HEADERS, body=json.dumps(validation_definition)
        )
    else:
        result = CallbackResult(
            404,
            headers=DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "Mock 404",
                        "detail": f"ValidationDefinition {validation_id} not found",
                        "source": None,
                    }
                ]
            ).json(),
        )
    return result


def post_validation_definitions_cb(request: PreparedRequest) -> CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    if not request.body:
        raise NotImplementedError("Handling missing body")

    payload: dict = json.loads(request.body)
    name = payload["data"]["name"]

    validation_definitions: dict[str, dict] = _CLOUD_API_FAKE_DB["validation_definitions"]
    validation_definition_names: set[str] = _CLOUD_API_FAKE_DB["VALIDATION_DEFINITION_NAMES"]

    if name in validation_definitions:
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
        id_ = str(uuid.uuid4())
        payload["data"]["id"] = id_
        validation_definitions[id_] = payload["data"]
        validation_definition_names.add(name)
        result = CallbackResult(201, headers=DEFAULT_HEADERS, body=json.dumps(payload))

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
    org_url_base_V0 = urllib.parse.urljoin(
        cloud_details.base_url, f"organizations/{cloud_details.org_id}/"
    )
    org_url_base_V1 = urllib.parse.urljoin(
        cloud_details.base_url, f"api/v1/organizations/{cloud_details.org_id}/"
    )
    dc_config_url = urllib.parse.urljoin(org_url_base_V1, "data-context-configuration")
    me_url = urllib.parse.urljoin(org_url_base_V0, "accounts/me")

    assert not _CLOUD_API_FAKE_DB, "_CLOUD_API_FAKE_DB should be empty"
    _CLOUD_API_FAKE_DB.update(create_fake_db_seed_data(fds_config))

    LOGGER.info("Mocking the GX Cloud API")

    with responses.RequestsMock(
        assert_all_requests_are_fired=assert_all_requests_are_fired
    ) as resp_mocker:
        resp_mocker.add_callback(responses.GET, me_url, get_user_id)
        resp_mocker.add_callback(responses.GET, dc_config_url, get_dc_configuration_cb)
        resp_mocker.add_callback(
            responses.GET,
            urllib.parse.urljoin(org_url_base_V1, "datasources"),
            get_datasources_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            urllib.parse.urljoin(org_url_base_V1, "datasources"),
            post_datasources_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"datasources/{UUID_REGEX}")),
            get_datasource_by_id_cb,
        )
        resp_mocker.add_callback(
            responses.DELETE,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"datasources/{UUID_REGEX}")),
            delete_datasources_cb,
        )
        resp_mocker.add_callback(
            responses.PUT,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"datasources/{UUID_REGEX}")),
            put_datasource_cb,
        )
        resp_mocker.add_callback(
            responses.DELETE,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"data-assets/{UUID_REGEX}")),
            delete_data_assets_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            urllib.parse.urljoin(org_url_base_V1, "expectation-suites"),
            get_expectation_suites_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"expectation-suites/{UUID_REGEX}")),
            get_expectation_suite_by_id_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            urllib.parse.urljoin(org_url_base_V1, "expectation-suites"),
            post_expectation_suites_cb,
        )
        resp_mocker.add_callback(
            responses.PUT,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"expectation-suites/{UUID_REGEX}")),
            put_expectation_suites_cb,
        )
        resp_mocker.add_callback(
            responses.DELETE,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"expectation-suites/{UUID_REGEX}")),
            delete_expectation_suites_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            urllib.parse.urljoin(org_url_base_V1, "checkpoints"),
            get_checkpoints_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            urllib.parse.urljoin(org_url_base_V1, "checkpoints"),
            post_checkpoints_cb,
        )
        resp_mocker.add_callback(
            responses.DELETE,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"checkpoints/{UUID_REGEX}")),
            delete_checkpoint_by_id_cb,
        )
        resp_mocker.add_callback(
            responses.DELETE,
            urllib.parse.urljoin(org_url_base_V1, "checkpoints"),
            delete_checkpoint_by_name_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            re.compile(urllib.parse.urljoin(org_url_base_V1, f"checkpoints/{UUID_REGEX}")),
            get_checkpoint_by_id_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            urllib.parse.urljoin(org_url_base_V1, "validation-results"),
            post_validation_results_cb,
        )
        resp_mocker.add_callback(
            responses.POST,
            urllib.parse.urljoin(org_url_base_V1, "validation-definitions"),
            post_validation_definitions_cb,
        )
        resp_mocker.add_callback(
            responses.GET,
            re.compile(
                urllib.parse.urljoin(org_url_base_V1, f"validation-definitions/{UUID_REGEX}")
            ),
            get_validation_definition_by_id_cb,
        )

        yield resp_mocker

    LOGGER.info(f"GX Cloud API Mock ending state ->\n{pf(_CLOUD_API_FAKE_DB, depth=2)}")
    _CLOUD_API_FAKE_DB.clear()
