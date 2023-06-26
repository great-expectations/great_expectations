from __future__ import annotations

import functools
import logging
import pathlib
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Final,
    Generator,
    List,
    Optional,
    Type,
    Union,
)

import pytest
from pytest import MonkeyPatch

import great_expectations as gx
from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.data_context import FileDataContext
from great_expectations.datasource.fluent import (
    PandasAzureBlobStorageDatasource,
    PandasGoogleCloudStorageDatasource,
    SparkAzureBlobStorageDatasource,
    SparkGoogleCloudStorageDatasource,
)
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.interfaces import Datasource
from great_expectations.datasource.fluent.sources import _SourceFactories
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from tests.datasource.fluent._fake_cloud_api import (
    _CLOUD_API_FAKE_DB,
    DUMMY_JWT_TOKEN,
    FAKE_ORG_ID,
    GX_CLOUD_MOCK_BASE_URL,
    CloudDetails,
    FakeDBTypedDict,
    create_fake_db_seed_data,
    gx_cloud_api_fake_ctx,
)
from tests.sqlalchemy_test_doubles import Dialect, MockSaEngine

if TYPE_CHECKING:
    import responses
    from pytest import FixtureRequest

    from great_expectations.data_context import CloudDataContext


FLUENT_DATASOURCE_TEST_DIR: Final = pathlib.Path(__file__).parent
PG_CONFIG_YAML_FILE: Final = FLUENT_DATASOURCE_TEST_DIR / FileDataContext.GX_YML


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


@pytest.fixture
def sqlite_database_path() -> pathlib.Path:
    relative_path = pathlib.Path(
        "..",
        "..",
        "test_sets",
        "taxi_yellow_tripdata_samples",
        "sqlite",
        "yellow_tripdata.db",
    )
    return pathlib.Path(__file__).parent.joinpath(relative_path).resolve(strict=True)


@pytest.fixture
def seed_ds_env_vars(
    monkeypatch: pytest.MonkeyPatch, sqlite_database_path: pathlib.Path
) -> tuple[tuple[str, str], ...]:
    """Seed a collection of ENV variables for use in testing config substitution."""
    config_sub_dict = {
        "MY_CONN_STR": f"sqlite:///{sqlite_database_path}",
        "MY_URL": "http://example.com",
        "MY_FILE": __file__,
    }

    for name, value in config_sub_dict.items():
        monkeypatch.setenv(name, value)
        logger.info(f"Setting ENV - {name} = '{value}'")

    # return as tuple of tuples so that the return value is immutable and therefore cacheable
    return tuple((k, v) for k, v in config_sub_dict.items())


@pytest.fixture(scope="session")
def cloud_details() -> CloudDetails:
    return CloudDetails(
        base_url=GX_CLOUD_MOCK_BASE_URL,
        org_id=FAKE_ORG_ID,
        access_token=DUMMY_JWT_TOKEN,
    )


@pytest.fixture
def cloud_api_fake(cloud_details: CloudDetails):
    with gx_cloud_api_fake_ctx(cloud_details=cloud_details) as requests_mock:
        yield requests_mock


@pytest.fixture
def cloud_api_fake_db(cloud_api_fake) -> FakeDBTypedDict:
    from tests.datasource.fluent._fake_cloud_api import _CLOUD_API_FAKE_DB

    return _CLOUD_API_FAKE_DB


@pytest.fixture
def empty_cloud_context_fluent(
    cloud_api_fake, cloud_details: CloudDetails
) -> CloudDataContext:
    context = gx.get_context(
        cloud_access_token=cloud_details.access_token,
        cloud_organization_id=cloud_details.org_id,
        cloud_base_url=cloud_details.base_url,
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
def empty_contexts(
    request: FixtureRequest,
    cloud_storage_get_client_doubles,
) -> FileDataContext | CloudDataContext:
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


@pytest.fixture
def fluent_only_config(
    fluent_gx_config_yml_str: str, seed_ds_env_vars: tuple
) -> GxConfig:
    """Creates a fluent `GxConfig` object and ensures it contains at least one `Datasource`"""
    fluent_config = GxConfig.parse_yaml(fluent_gx_config_yml_str)
    assert fluent_config.datasources
    return fluent_config


@pytest.fixture
def fluent_yaml_config_file(
    file_dc_config_dir_init: pathlib.Path,
    fluent_gx_config_yml_str: str,
) -> pathlib.Path:
    """
    Dump the provided GxConfig to a temporary path. File is removed during test teardown.

    Append fluent config to default config file
    """
    config_file_path = file_dc_config_dir_init / FileDataContext.GX_YML

    assert config_file_path.exists() is True

    with open(config_file_path, mode="a") as f_append:
        yaml_string = "\n# Fluent\n" + fluent_gx_config_yml_str
        f_append.write(yaml_string)

    logger.debug(f"  Config File Text\n-----------\n{config_file_path.read_text()}")
    return config_file_path


@pytest.fixture
@functools.lru_cache(maxsize=1)
def seeded_file_context(
    cloud_storage_get_client_doubles,
    fluent_yaml_config_file: pathlib.Path,
    seed_ds_env_vars: tuple,
) -> FileDataContext:
    context = gx.get_context(
        context_root_dir=fluent_yaml_config_file.parent, cloud_mode=False
    )
    assert isinstance(context, FileDataContext)
    return context


@pytest.fixture
def seed_cloud(
    cloud_storage_get_client_doubles,
    cloud_api_fake: responses.RequestsMock,
    fluent_only_config: GxConfig,
):
    """
    In order to load the seeded cloud config, this fixture must be called before any
    `get_context()` calls.
    """
    org_url_base = f"{GX_CLOUD_MOCK_BASE_URL}/organizations/{FAKE_ORG_ID}"

    fake_db_data = create_fake_db_seed_data(fds_config=fluent_only_config)
    _CLOUD_API_FAKE_DB.update(fake_db_data)  # type: ignore[typeddict-item]

    seeded_datasources = _CLOUD_API_FAKE_DB["data-context-configuration"]["datasources"]
    logger.info(f"Seeded Datasources ->\n{pf(seeded_datasources, depth=2)}")
    assert seeded_datasources

    yield cloud_api_fake

    assert len(cloud_api_fake.calls) >= 1, f"{org_url_base} was never called"


@pytest.fixture
def seeded_cloud_context(
    seed_cloud,  # NOTE: this fixture must be called before the CloudDataContext is created
    empty_cloud_context_fluent,
):
    return empty_cloud_context_fluent


@pytest.fixture(params=["seeded_file_context", "seeded_cloud_context"])
def seeded_contexts(
    request: FixtureRequest,
):
    """Parametrized fixture for seeded File and Cloud DataContexts."""
    context_fixture: FileDataContext | CloudDataContext = request.getfixturevalue(
        request.param
    )
    return context_fixture
