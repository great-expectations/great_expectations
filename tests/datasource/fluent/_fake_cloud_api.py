from __future__ import annotations

import json
import logging
import urllib.parse
import uuid
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Dict,
    Final,
    Literal,
    NamedTuple,
    Optional,
    Union,
)

import pydantic

from great_expectations.data_context.store.gx_cloud_store_backend import (
    ErrorDetail,
    ErrorPayload,
)

if TYPE_CHECKING:
    from requests import PreparedRequest


LOGGER = logging.getLogger(__name__)

MISSING: Final = object()

GX_CLOUD_MOCK_BASE_URL: Final[str] = "https://app.greatexpectations.fake.io"

DUMMY_JWT_TOKEN: Final[
    str
] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
# Can replace hardcoded ids with dynamic ones if using a regex url with responses.add_callback()
# https://github.com/getsentry/responses/tree/master#dynamic-responses
FAKE_ORG_ID: Final[str] = str(uuid.UUID("12345678123456781234567812345678"))
FAKE_DATA_CONTEXT_ID: Final[str] = str(uuid.uuid4())
FAKE_DATASOURCE_ID: Final[str] = str(uuid.uuid4())
FAKE_EXPECTATION_SUITE_ID: Final[str] = str(uuid.uuid4())


_CLOUD_API_FAKE_DB: dict = {}
_DEFAULT_HEADERS: Final[dict[str, str]] = {"content-type": "application/json"}


class _DatasourceSchema(pydantic.BaseModel):
    id: Optional[str] = None
    type: Literal["datasource"]
    attributes: Dict[str, Union[Dict, str]]

    @property
    def name(self) -> str:
        return self.attributes["datasource_config"]["name"]  # type: ignore[index]


class _CloudResponseSchema(pydantic.BaseModel):
    data: _DatasourceSchema

    @classmethod
    def from_datasource_json(cls, ds_payload: str | bytes) -> _CloudResponseSchema:
        payload_dict = json.loads(ds_payload)
        data = {
            "id": payload_dict.get("id"),
            "type": "datasource",
            "attributes": payload_dict["data"]["attributes"],
        }

        return cls(data=data)  # type: ignore[arg-type] # pydantic type coercion


class _CallbackResult(NamedTuple):
    status: int
    headers: dict[str, str]
    body: str


ErrorPayloadSchema = pydantic.create_model_from_typeddict(ErrorPayload)
ErrorPayloadSchema.update_forward_refs(ErrorDetail=ErrorDetail)


def _get_fake_db_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    assert url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)

    _ = parsed_url.query
    # TODO: do something with this
    url = urllib.parse.urljoin(url, parsed_url.path)

    item = _CLOUD_API_FAKE_DB.get(url, MISSING)
    LOGGER.info(f"GET response body -->\n{pf(item, depth=2)}")
    if item is MISSING:
        errors = ErrorPayloadSchema(
            errors=[
                {"code": "mock 404", "detail": f"NotFound at {url}", "source": None}
            ]
        )
        result = _CallbackResult(404, headers={}, body=errors.json())
    else:
        result = _CallbackResult(200, headers=_DEFAULT_HEADERS, body=json.dumps(item))
    return result


def _delete_fake_db_datasources_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    LOGGER.info(f"{request.method} {url}")

    item = _CLOUD_API_FAKE_DB.pop(url, MISSING)
    if item is MISSING:
        errors = ErrorPayloadSchema(
            errors=[{"code": "mock 404", "detail": None, "source": None}]
        )
        result = _CallbackResult(404, headers=_DEFAULT_HEADERS, body=errors.json())
    else:
        result = _CallbackResult(204, headers=_DEFAULT_HEADERS, body="")
    return result


def _post_fake_db_datasources_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    ds_names: set[str] = _CLOUD_API_FAKE_DB["DATASOURCE_NAMES"]
    datasource_path = f"{url}/{FAKE_DATASOURCE_ID}"

    if not request.body:
        return _CallbackResult(
            400,
            headers=_DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[{"code": "400", "detail": "Missing Body", "source": None}]
            ).json(),
        )

    try:
        LOGGER.info(f"POST request body -->\n{pf(json.loads(request.body), depth=4)}")
        payload = _CloudResponseSchema.from_datasource_json(request.body)

        datasource_name: str = payload.data.name
        if datasource_name not in ds_names:
            datasource_id = payload.data.id
            if not datasource_id:
                datasource_id = FAKE_DATASOURCE_ID
                payload.data.id = datasource_id

            _CLOUD_API_FAKE_DB[datasource_path] = payload.dict()
            _CLOUD_API_FAKE_DB["DATASOURCE_NAMES"].add(payload.data.name)

            result = _CallbackResult(201, headers=_DEFAULT_HEADERS, body=payload.json())
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
            result = _CallbackResult(409, headers=_DEFAULT_HEADERS, body=errors.json())

        return result
    except pydantic.ValidationError as val_err:
        LOGGER.exception(val_err)
        return _CallbackResult(
            400,
            headers=_DEFAULT_HEADERS,
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
        return _CallbackResult(
            500,
            headers=_DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[{"code": "mock 500", "detail": repr(err), "source": None}]
            ).json(),
        )


def _put_db_datasources_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    item = _CLOUD_API_FAKE_DB.get(url, MISSING)
    if not request.body:
        errors = ErrorPayload(
            errors=[{"code": "mock 400", "detail": "missing body", "source": None}]
        )
        result = _CallbackResult(400, headers=_DEFAULT_HEADERS, body=json.dumps(errors))
    elif item is not MISSING:
        payload = json.loads(request.body)
        LOGGER.info(f"PUT request body -->\n{pf(payload, depth=6)}")
        _CLOUD_API_FAKE_DB[url] = payload
        result = _CallbackResult(
            200, headers=_DEFAULT_HEADERS, body=json.dumps(payload)
        )
    else:
        errors = ErrorPayload(
            errors=[{"code": "mock 404", "detail": None, "source": None}]
        )
        result = _CallbackResult(404, headers=_DEFAULT_HEADERS, body=json.dumps(errors))

    LOGGER.debug(f"Response {result.status}")
    return result


def _get_db_datasources_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    item = _CLOUD_API_FAKE_DB.get(url, MISSING)
    if not request.body:
        errors = ErrorPayload(
            errors=[{"code": "mock 400", "detail": "missing body", "source": None}]
        )
        result = _CallbackResult(400, headers=_DEFAULT_HEADERS, body=json.dumps(errors))
    elif item is not MISSING:
        payload = json.loads(request.body)
        _CLOUD_API_FAKE_DB[url] = payload
        result = _CallbackResult(
            200, headers=_DEFAULT_HEADERS, body=json.dumps(payload)
        )
    else:
        errors = ErrorPayload(
            errors=[{"code": "mock 404", "detail": None, "source": None}]
        )
        result = _CallbackResult(404, headers=_DEFAULT_HEADERS, body=json.dumps(errors))

    LOGGER.debug(f"Response {result.status}")
    return result


def _get_db_expectation_suites_callback(request: PreparedRequest) -> _CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    query_params = urllib.parse.parse_qs(parsed_url.query)
    # print(f"{query_params=}")
    queried_names: list[str] = query_params.get("name", [])

    exp_suites: dict[str, dict] = _CLOUD_API_FAKE_DB["EXPECTATION_SUITES"]
    exp_suite_list: list[dict] = list(exp_suites.values())
    if queried_names:
        exp_suite_list = [
            d
            for d in exp_suite_list
            if d["data"]["attributes"]["suite"]["expectation_suite_name"]
            in queried_names
        ]

    # print(pf(exp_suite_list, depth=5))
    resp_body = {"data": exp_suite_list}

    result = _CallbackResult(200, headers=_DEFAULT_HEADERS, body=json.dumps(resp_body))
    LOGGER.debug(f"Response {result.status}")
    return result


def _get_db_expectation_suite_by_id_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)
    expectation_id: str = parsed_url.path.split("/")[-1]

    expectation_suite: dict = _CLOUD_API_FAKE_DB["EXPECTATION_SUITES"].get(
        expectation_id
    )
    if expectation_suite:
        result = _CallbackResult(
            200, headers=_DEFAULT_HEADERS, body=json.dumps(expectation_suite)
        )
    else:
        result = _CallbackResult(404, headers=_DEFAULT_HEADERS, body="")
    return result


def _post_db_expectation_suites_callback(request: PreparedRequest) -> _CallbackResult:
    url = request.url
    LOGGER.debug(f"{request.method} {url}")

    if not request.body:
        raise NotImplementedError("Handling missing body")

    payload: dict = json.loads(request.body)
    name = payload["data"]["attributes"]["suite"]["expectation_suite_name"]

    exp_suite_names: set[str] = _CLOUD_API_FAKE_DB["EXPECTATION_SUITE_NAMES"]
    exp_suites: dict[str, dict] = _CLOUD_API_FAKE_DB["EXPECTATION_SUITES"]
    # print(f"{name=}\n{pf(exp_suites, depth=3)}\n")

    if name in exp_suite_names:
        print("conflict")
        result = _CallbackResult(
            409,  # not really a 409 in prod but it's a more informative status code
            headers=_DEFAULT_HEADERS,
            body=ErrorPayloadSchema(
                errors=[
                    {
                        "code": "mock 409",
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
        result = _CallbackResult(
            201, headers=_DEFAULT_HEADERS, body=json.dumps(payload)
        )

    # print(pf(exp_suites, depth=3))
    LOGGER.debug(f"Response {result.status}")
    return result
