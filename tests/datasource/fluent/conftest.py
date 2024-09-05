from __future__ import annotations

import functools
import logging
import pathlib
import urllib.parse
import warnings
from contextlib import contextmanager
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    Final,
    Generator,
    List,
    Optional,
    Type,
    Union,
)

import pytest
from moto import mock_s3
from pytest import MonkeyPatch
from typing_extensions import TypeAlias, override

import great_expectations as gx
from great_expectations.compatibility import aws
from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.datasource.fluent import (
    PandasAzureBlobStorageDatasource,
    PandasGoogleCloudStorageDatasource,
    SparkAzureBlobStorageDatasource,
    SparkGoogleCloudStorageDatasource,
)
from great_expectations.datasource.fluent.config import GxConfig
from great_expectations.datasource.fluent.interfaces import Datasource
from great_expectations.datasource.fluent.pandas_filesystem_datasource import (
    PandasFilesystemDatasource,
)
from great_expectations.datasource.fluent.postgres_datasource import PostgresDatasource
from great_expectations.datasource.fluent.sources import DataSourceManager
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from tests.datasource.fluent._fake_cloud_api import (
    _CLOUD_API_FAKE_DB,
    FAKE_ORG_ID,
    GX_CLOUD_MOCK_BASE_URL,
    FakeDBTypedDict,
    create_fake_db_seed_data,
)
from tests.sqlalchemy_test_doubles import Dialect, MockSaEngine

if TYPE_CHECKING:
    import responses
    from botocore.client import BaseClient as BotoBaseClient
    from pytest import FixtureRequest

    from great_expectations.data_context import CloudDataContext

CreateSourceFixture: TypeAlias = Callable[..., ContextManager[PostgresDatasource]]
FLUENT_DATASOURCE_TEST_DIR: Final = pathlib.Path(__file__).parent
PG_CONFIG_YAML_FILE: Final = FLUENT_DATASOURCE_TEST_DIR / FileDataContext.GX_YML
_DEFAULT_TEST_YEARS = list(range(2021, 2023))
_DEFAULT_TEST_MONTHS = list(range(1, 13))


CNF_TEST_LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None],
    dialect: str,
    partitioner_query_response: Optional[Union[List[Dict[str, Any]], List[Any]]] = None,
):
    """Creates a mock gx sql alchemy engine class

    Args:
        validate_batch_spec: A hook that can be used to validate the generated the batch spec
            passed into get_batch_data_and_markers
        dialect: A string representing the SQL Engine dialect. Examples include: postgresql, sqlite
        partitioner_query_response: An optional list of dictionaries. Each dictionary is a row returned
            from the partitioner query. The keys are the column names and the value is the column values,
            eg: [{'year': 2021, 'month': 1}, {'year': 2021, 'month': 2}]
    """  # noqa: E501

    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, create_temp_table: bool = True, *args, **kwargs):
            # We should likely let the user pass in an engine. In a SqlAlchemyExecutionEngine used in  # noqa: E501
            # non-mocked code the engine property is of the type:
            # from sqlalchemy.engine import Engine as SaEngine
            self.engine = MockSaEngine(dialect=Dialect(dialect))  # type: ignore[assignment]
            self._create_temp_table = create_temp_table

        @override
        def get_batch_data_and_markers(  # type: ignore[override]
            self, batch_spec: SqlAlchemyDatasourceBatchSpec
        ) -> tuple[BatchData, BatchMarkers]:
            validate_batch_spec(batch_spec)
            return BatchData(self), BatchMarkers(ge_load_time=None)

        def execute_partitioned_query(self, partitioned_query):  # type: ignore[explicit-override] # FIXME
            class Row:
                def __init__(self, attributes):
                    for k, v in attributes.items():
                        setattr(self, k, v)

            # We know that partitioner_query_response is non-empty because of validation
            # at the top of the outer function.
            # In some cases, such as in the datetime partitioners,
            # a dictionary is returned our from out partitioner query with the key as the parameter_name.  # noqa: E501
            # Otherwise, a list of values is returned.
            if isinstance(partitioner_query_response[0], dict):
                return [Row(row_dict) for row_dict in partitioner_query_response]
            return partitioner_query_response

    return MockSqlAlchemyExecutionEngine


class ExecutionEngineDouble(ExecutionEngine):
    def __init__(self, *args, **kwargs):
        pass

    @override
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
    for key in DataSourceManager.type_lookup:
        if issubclass(type(key), Datasource):
            original_engine_override[key] = key.execution_engine_override

    try:
        for source in original_engine_override:
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
        CNF_TEST_LOGGER.info(f"Setting ENV - {name} = '{value}'")

    # return as tuple of tuples so that the return value is immutable and therefore cacheable
    return tuple((k, v) for k, v in config_sub_dict.items())


@pytest.fixture
def cloud_api_fake_db(cloud_api_fake) -> FakeDBTypedDict:
    from tests.datasource.fluent._fake_cloud_api import _CLOUD_API_FAKE_DB

    return _CLOUD_API_FAKE_DB


@pytest.fixture
def file_dc_config_dir_init(tmp_path: pathlib.Path) -> pathlib.Path:
    """
    Initialize an regular/old-style FileDataContext project config directory.
    Removed on teardown.
    """
    gx_yml = tmp_path / FileDataContext.GX_DIR / FileDataContext.GX_YML
    assert gx_yml.exists() is False
    gx.get_context(mode="file", project_root_dir=tmp_path)
    assert gx_yml.exists()

    tmp_gx_dir = gx_yml.parent.absolute()
    CNF_TEST_LOGGER.info(f"tmp_gx_dir -> {tmp_gx_dir}")
    return tmp_gx_dir


@pytest.fixture
def empty_file_context(file_dc_config_dir_init) -> FileDataContext:
    context = gx.get_context(context_root_dir=file_dc_config_dir_init, cloud_mode=False)
    return context


@pytest.fixture(
    params=[
        pytest.param("empty_cloud_context_fluent", marks=pytest.mark.cloud),
        pytest.param("empty_file_context", marks=pytest.mark.filesystem),
    ],
    ids=["cloud", "file"],
)
def empty_contexts(
    request: FixtureRequest,
    cloud_storage_get_client_doubles,
) -> FileDataContext | CloudDataContext:
    context_fixture: FileDataContext | CloudDataContext = request.getfixturevalue(request.param)
    return context_fixture


@pytest.fixture(scope="session")
def fluent_gx_config_yml() -> pathlib.Path:
    assert PG_CONFIG_YAML_FILE.exists()
    return PG_CONFIG_YAML_FILE


@pytest.fixture(scope="session")
def fluent_gx_config_yml_str(fluent_gx_config_yml: pathlib.Path) -> str:
    return fluent_gx_config_yml.read_text()


@pytest.fixture()
def aws_region_name() -> str:
    return "us-east-1"


@pytest.fixture(scope="function")
def aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """Monkeypatch ENV AWS Credentials for moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "testing")


@pytest.fixture
def s3_mock(aws_credentials, aws_region_name: str) -> Generator[BotoBaseClient, None, None]:
    with mock_s3():
        client = aws.boto3.client("s3", region_name=aws_region_name)
        yield client


class _TestClientDummy:
    pass


_CLIENT_DUMMY = _TestClientDummy()


def _get_test_client_dummy(*args, **kwargs) -> _TestClientDummy:
    CNF_TEST_LOGGER.debug(
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
    Patches Datasources that rely on a private _get_*_client() method to return test doubles
    instead.

    gcs
    azure
    """
    CNF_TEST_LOGGER.warning(
        "Patching cloud storage _get_*_client() methods to return client test doubles"
    )


@pytest.fixture
def filter_data_connector_build_warning():
    with warnings.catch_warnings() as w:
        warnings.simplefilter("ignore", RuntimeWarning)
        yield w


@pytest.fixture
def fluent_only_config(fluent_gx_config_yml_str: str, seed_ds_env_vars: tuple) -> GxConfig:
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

    CNF_TEST_LOGGER.debug(f"  Config File Text\n-----------\n{config_file_path.read_text()}")
    return config_file_path


@pytest.fixture
@functools.lru_cache(maxsize=1)
def seeded_file_context(
    filter_data_connector_build_warning,
    fluent_yaml_config_file: pathlib.Path,
    seed_ds_env_vars: tuple,
) -> FileDataContext:
    context = gx.get_context(context_root_dir=fluent_yaml_config_file.parent, cloud_mode=False)
    assert isinstance(context, FileDataContext)
    return context


@pytest.fixture
def seed_cloud(
    filter_data_connector_build_warning,
    cloud_api_fake: responses.RequestsMock,
    fluent_only_config: GxConfig,
):
    """
    In order to load the seeded cloud config, this fixture must be called before any
    `get_context()` calls.
    """
    org_url_base = urllib.parse.urljoin(GX_CLOUD_MOCK_BASE_URL, f"organizations/{FAKE_ORG_ID}")

    fake_db_data = create_fake_db_seed_data(fds_config=fluent_only_config)
    _CLOUD_API_FAKE_DB.update(fake_db_data)

    seeded_datasources = _CLOUD_API_FAKE_DB["data-context-configuration"]["datasources"]
    CNF_TEST_LOGGER.info(f"Seeded Datasources ->\n{pf(seeded_datasources, depth=2)}")
    assert seeded_datasources

    yield cloud_api_fake

    assert len(cloud_api_fake.calls) >= 1, f"{org_url_base} was never called"


@pytest.fixture
def seeded_cloud_context(
    seed_cloud,  # NOTE: this fixture must be called before the CloudDataContext is created
    empty_cloud_context_fluent,
):
    empty_cloud_context_fluent._init_datasources()
    return empty_cloud_context_fluent


@pytest.fixture(
    params=[
        pytest.param("seeded_file_context", marks=[pytest.mark.filesystem]),
        pytest.param("seeded_cloud_context", marks=[pytest.mark.cloud]),
    ]
)
def seeded_contexts(
    request: FixtureRequest,
):
    """Parametrized fixture for seeded File and Cloud DataContexts."""
    context_fixture: FileDataContext | CloudDataContext = request.getfixturevalue(request.param)
    return context_fixture


@pytest.fixture
def pandas_filesystem_datasource(empty_data_context) -> PandasFilesystemDatasource:
    base_directory_rel_path = pathlib.Path("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    base_directory_abs_path = (
        pathlib.Path(__file__).parent.joinpath(base_directory_rel_path).resolve(strict=True)
    )
    pandas_filesystem_datasource = PandasFilesystemDatasource(
        name="pandas_filesystem_datasource",
        base_directory=base_directory_abs_path,
    )
    pandas_filesystem_datasource._data_context = empty_data_context
    return pandas_filesystem_datasource


@contextmanager
def _source(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None],
    dialect: str,
    connection_string: str = "postgresql+psycopg2://postgres:@localhost/test_ci",
    data_context: Optional[AbstractDataContext] = None,
    partitioner_query_response: Optional[List[Dict[str, Any]]] = None,
    create_temp_table: bool = True,
) -> Generator[PostgresDatasource, None, None]:
    partitioner_response = partitioner_query_response or (
        [
            {"year": year, "month": month}
            for year in _DEFAULT_TEST_YEARS
            for month in _DEFAULT_TEST_MONTHS
        ]
    )

    execution_eng_cls = sqlachemy_execution_engine_mock_cls(
        validate_batch_spec=validate_batch_spec,
        dialect=dialect,
        partitioner_query_response=partitioner_response,
    )
    original_override = PostgresDatasource.execution_engine_override  # type: ignore[misc]
    try:
        PostgresDatasource.execution_engine_override = execution_eng_cls  # type: ignore[misc]
        postgres_datasource = PostgresDatasource(
            name="my_datasource",
            connection_string=connection_string,
            create_temp_table=create_temp_table,
        )
        if data_context:
            postgres_datasource._data_context = data_context
        yield postgres_datasource
    finally:
        PostgresDatasource.execution_engine_override = original_override  # type: ignore[misc]


# We may be able to parameterize this fixture so we can instantiate _source in the fixture.
# This would reduce the `with ...` boilerplate in the individual tests.
@pytest.fixture
def create_source() -> ContextManager:
    return _source  # type: ignore[return-value]
