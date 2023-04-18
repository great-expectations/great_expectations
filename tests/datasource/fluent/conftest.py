from __future__ import annotations

import logging
import pathlib
from pprint import pformat as pf
from typing import Any, Callable, Dict, Generator, List, Type

import pytest
from pytest import MonkeyPatch
from typing_extensions import Final

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
from great_expectations.datasource.fluent.interfaces import Datasource
from great_expectations.datasource.fluent.sources import _SourceFactories
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from tests.sqlalchemy_test_doubles import Dialect, MockSaEngine

FLUENT_DATASOURCE_TEST_DIR: Final = pathlib.Path(__file__).parent
PG_CONFIG_YAML_FILE: Final = FLUENT_DATASOURCE_TEST_DIR / FileDataContext.GX_YML

logger = logging.getLogger(__name__)


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None],
    dialect: str,
    splitter_query_response: List[Dict[str, Any]] | List[Any] | None = None,
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
