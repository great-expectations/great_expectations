from __future__ import annotations

import json
import logging
import pathlib
import urllib.parse
import uuid
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    List,
    NamedTuple,
    Optional,
    Type,
    Union,
)

import pytest
import responses
from pytest import MonkeyPatch
from typing_extensions import Final

import great_expectations as gx
from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.store.gx_cloud_store_backend import ErrorPayload
from great_expectations.datasource.fluent import (
    PandasAzureBlobStorageDatasource,
    PandasGoogleCloudStorageDatasource,
    SparkAzureBlobStorageDatasource,
    SparkGoogleCloudStorageDatasource,
)
from great_expectations.datasource.fluent.interfaces import Datasource
from great_expectations.datasource.fluent.sources import _SourceFactories
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from tests.sqlalchemy_test_doubles import Dialect, MockSaEngine

if TYPE_CHECKING:
    from pytest import FixtureRequest
    from requests import PreparedRequest

    from great_expectations.data_context import CloudDataContext


FLUENT_DATASOURCE_TEST_DIR: Final = pathlib.Path(__file__).parent
PG_CONFIG_YAML_FILE: Final = FLUENT_DATASOURCE_TEST_DIR / FileDataContext.GX_YML


GX_CLOUD_MOCK_BASE_URL: Final[str] = "https://app.greatexpectations.fake.io"

DUMMY_JWT_TOKEN: Final[
    str
] = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
# Can replace hardcoded ids with dynamic ones if using a regex url with responses.add_callback()
# https://github.com/getsentry/responses/tree/master#dynamic-responses
FAKE_ORG_ID: Final[str] = str(uuid.UUID("12345678123456781234567812345678"))
FAKE_DATA_CONTEXT_ID: Final[str] = str(uuid.uuid4())
FAKE_DATASOURCE_ID: Final[str] = str(uuid.uuid4())

MISSING: Final = object()

logger = logging.getLogger(__name__)


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None],
    dialect: str,
    splitter_query_response: Optional[Union[List[Dict[str, Any]], List[Any]]] = None,
):
    """Creates a mock gx sql alchemy engine class

    Args:
        validate_batch_spec: A hook that can be used to validate the generated the batch spec
            passed into get_batch_data_and_markers
        dialect: A string representing the SQL Engine dialect. Examples include: postgresql, sqlite
        splitter_query_response: An optional list of dictionaries. Each dictionary is a row returned
            from the splitter query. The keys are the column names and the value is the column values,
            eg: [{'year': 2021, 'month': 1}, {'year': 2021, 'month': 2}]
    """

    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, create_temp_table: bool = True, *args, **kwargs):
            # We should likely let the user pass in an engine. In a SqlAlchemyExecutionEngine used in
            # non-mocked code the engine property is of the type:
            # from sqlalchemy.engine import Engine as SaEngine
            self.engine = MockSaEngine(dialect=Dialect(dialect))
            self._create_temp_table = create_temp_table

        def get_batch_data_and_markers(  # type: ignore[override]
            self, batch_spec: SqlAlchemyDatasourceBatchSpec
        ) -> tuple[BatchData, BatchMarkers]:
            validate_batch_spec(batch_spec)
            return BatchData(self), BatchMarkers(ge_load_time=None)

        def execute_split_query(self, split_query):
            class Row:
                def __init__(self, attributes):
                    for k, v in attributes.items():
                        setattr(self, k, v)

            # We know that splitter_query_response is non-empty because of validation
            # at the top of the outer function.
            # In some cases, such as in the datetime splitters,
            # a dictionary is returned our from out splitter query with the key as the parameter_name.
            # Otherwise, a list of values is returned.
            if isinstance(splitter_query_response[0], dict):
                return [Row(row_dict) for row_dict in splitter_query_response]
            return splitter_query_response

    return MockSqlAlchemyExecutionEngine


class ExecutionEngineDouble(ExecutionEngine):
    def __init__(self, *args, **kwargs):
        pass

    def get_batch_data_and_markers(self, batch_spec) -> tuple[BatchData, BatchMarkers]:  # type: ignore[override]
        return BatchData(self), BatchMarkers(ge_load_time=None)


@pytest.fixture
def inject_engine_lookup_double(
    monkeypatch: MonkeyPatch,
) -> Generator[Type[ExecutionEngineDouble], None, None]:
    """
    Inject an execution engine test double into the _SourcesFactory.engine_lookup
    so that all Datasources use the execution engine double.
    Dynamically create a new subclass so that runtime type validation does not fail.
    """
    original_engine_override: dict[Type[Datasource], Type[ExecutionEngine]] = {}
    for key in _SourceFactories.type_lookup.keys():
        if issubclass(type(key), Datasource):
            original_engine_override[key] = key.execution_engine_override

    try:
        for source in original_engine_override.keys():
            source.execution_engine_override = ExecutionEngineDouble
        yield ExecutionEngineDouble
    finally:
        for source, engine in original_engine_override.items():
            source.execution_engine_override = engine


_CLOUD_API_FAKE_DB: dict = {}
_DEFAULT_HEADERS: Final[dict[str, str]] = {"content-type": "application/json"}


class _CallbackResult(NamedTuple):
    status: int
    headers: dict[str, str]
    body: str


def _get_fake_db_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    assert url
    logger.info(f"{request.method} {url}")

    parsed_url = urllib.parse.urlparse(url)

    _ = parsed_url.query
    # TODO: do something with this
    url = urllib.parse.urljoin(url, parsed_url.path)

    item = _CLOUD_API_FAKE_DB.get(url, MISSING)
    logger.info(f"body -->\n{pf(item, depth=2)}")
    if item is MISSING:
        result = _CallbackResult(404, headers={}, body=f"NotFound at {url}")
    else:
        result = _CallbackResult(200, headers=_DEFAULT_HEADERS, body=json.dumps(item))

    logger.info(f"Response {result.status}")
    return result


def _delete_fake_db_datasources_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    logger.info(f"{request.method} {url}")

    item = _CLOUD_API_FAKE_DB.pop(url, MISSING)
    if item is MISSING:
        errors = ErrorPayload(
            errors=[{"code": "mock 404", "detail": None, "source": None}]
        )
        result = _CallbackResult(404, headers=_DEFAULT_HEADERS, body=json.dumps(errors))
    else:
        errors = ErrorPayload(errors=[{"code": "mock", "detail": None, "source": None}])
        result = _CallbackResult(204, headers=_DEFAULT_HEADERS, body="")

    logger.info(f"Response {result.status}")
    return result


def _post_fake_db_datasources_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    logger.info(f"{request.method} {url}")

    item = _CLOUD_API_FAKE_DB.get(url, MISSING)
    if request.body and item is MISSING:
        payload = json.loads(request.body)

        datasource_id = payload.get("data", {}).get("id")
        if not datasource_id:
            datasource_id = FAKE_DATASOURCE_ID
            payload["data"]["id"] = datasource_id

        _CLOUD_API_FAKE_DB[f"{url}/{FAKE_DATASOURCE_ID}"] = payload

        result = _CallbackResult(
            201, headers=_DEFAULT_HEADERS, body=json.dumps(payload)
        )
    else:
        errors = ErrorPayload(errors=[{"code": "mock", "detail": None, "source": None}])
        result = _CallbackResult(409, headers=_DEFAULT_HEADERS, body=json.dumps(errors))

    logger.info(f"Response {result.status}")
    return result


def _put_db_datasources_callback(
    request: PreparedRequest,
) -> _CallbackResult:
    url = request.url
    logger.info(f"{request.method} {url}")

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

    logger.info(f"Response {result.status}")
    return result


@pytest.fixture
def cloud_api_fake():
    org_url_base = f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}"
    dc_config_url = f"{org_url_base}/data-context-configuration"
    datasources_url = f"{org_url_base}/datasources"

    assert not _CLOUD_API_FAKE_DB, "_CLOUD_API_FAKE_DB should be empty"
    _CLOUD_API_FAKE_DB.update(
        {
            dc_config_url: {
                "anonymous_usage_statistics": {
                    "data_context_id": FAKE_DATA_CONTEXT_ID,
                    "enabled": False,
                },
                "datasources": {},
            },
            datasources_url: MISSING,
        }
    )

    logger.info("Mocking the GX Cloud API")

    with responses.RequestsMock(assert_all_requests_are_fired=False) as resp_mocker:
        resp_mocker.add_callback(responses.GET, dc_config_url, _get_fake_db_callback)
        resp_mocker.add_callback(
            responses.GET,
            f"{datasources_url}/{FAKE_DATASOURCE_ID}",
            _get_fake_db_callback,
        )
        resp_mocker.add_callback(
            responses.DELETE,
            f"{datasources_url}/{FAKE_DATASOURCE_ID}",
            _delete_fake_db_datasources_callback,
        )
        resp_mocker.add_callback(
            responses.PUT,
            f"{datasources_url}/{FAKE_DATASOURCE_ID}",
            _put_db_datasources_callback,
        )
        resp_mocker.add_callback(
            responses.POST, datasources_url, _post_fake_db_datasources_callback
        )

        yield resp_mocker

    logger.info(f"Ending state ->\n{pf(_CLOUD_API_FAKE_DB, depth=1)}")
    _CLOUD_API_FAKE_DB.clear()


@pytest.fixture
def empty_cloud_context_fluent(cloud_api_fake) -> CloudDataContext:
    context = gx.get_context(
        cloud_access_token=DUMMY_JWT_TOKEN,
        cloud_organization_id=FAKE_ORG_ID,
        cloud_base_url=GX_CLOUD_MOCK_BASE_URL,
        cloud_mode=True,
    )
    return context


@pytest.fixture
def file_dc_config_dir_init(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Initialize an regular/old-style FileDataContext project config directory.
    Removed on teardown.
    """
    gx_yml = tmp_path / FileDataContext.GX_DIR / FileDataContext.GX_YML
    assert gx_yml.exists() is False
    FileDataContext.create(tmp_path)
    assert gx_yml.exists()

    tmp_gx_dir = gx_yml.parent.absolute()
    logger.info(f"tmp_gx_dir -> {tmp_gx_dir}")
    return tmp_gx_dir


@pytest.fixture
def empty_file_context(file_dc_config_dir_init) -> FileDataContext:
    context = gx.get_context(context_root_dir=file_dc_config_dir_init, cloud_mode=False)
    return context


@pytest.fixture(
    params=["empty_cloud_context_fluent", "empty_file_context"], ids=["cloud", "file"]
)
def empty_contexts(request: FixtureRequest) -> FileDataContext | CloudDataContext:
    context_fixture: FileDataContext | CloudDataContext = request.getfixturevalue(
        request.param
    )
    return context_fixture


@pytest.fixture(scope="session")
def fluent_gx_config_yml() -> pathlib.Path:
    assert PG_CONFIG_YAML_FILE.exists()
    return PG_CONFIG_YAML_FILE


@pytest.fixture(scope="session")
def fluent_gx_config_yml_str(fluent_gx_config_yml: pathlib.Path) -> str:
    return fluent_gx_config_yml.read_text()


class _TestClientDummy:
    pass


_CLIENT_DUMMY = _TestClientDummy()


def _get_test_client_dummy(*args, **kwargs) -> _TestClientDummy:
    logger.debug(
        f"_get_test_client_dummy() called with \nargs: {pf(args)}\nkwargs: {pf(kwargs)}"
    )
    return _CLIENT_DUMMY


@pytest.fixture
def gcs_get_client_dummy(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        PandasGoogleCloudStorageDatasource,
        "_get_gcs_client",
        _get_test_client_dummy,
        raising=True,
    )
    monkeypatch.setattr(
        SparkGoogleCloudStorageDatasource,
        "_get_gcs_client",
        _get_test_client_dummy,
        raising=True,
    )


@pytest.fixture
def azure_get_client_dummy(monkeypatch: MonkeyPatch):
    monkeypatch.setattr(
        PandasAzureBlobStorageDatasource,
        "_get_azure_client",
        _get_test_client_dummy,
        raising=True,
    )
    monkeypatch.setattr(
        SparkAzureBlobStorageDatasource,
        "_get_azure_client",
        _get_test_client_dummy,
        raising=True,
    )


@pytest.fixture
def cloud_storage_get_client_doubles(
    gcs_get_client_dummy,
    azure_get_client_dummy,
):
    """
    Patches Datasources that rely on a private _get_*_client() method to return test doubles instead.

    gcs
    azure
    """
    logger.warning(
        "Patching cloud storage _get_*_client() methods to return client test doubles"
    )
