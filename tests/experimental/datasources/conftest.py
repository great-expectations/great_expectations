import logging
from datetime import datetime
from typing import Callable, Dict, Tuple

import pytest
from pytest import MonkeyPatch

from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.experimental.datasources.metadatasource import MetaDatasource

LOGGER = logging.getLogger(__name__)

from contextlib import contextmanager

from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.experimental.datasources.sources import _SourceFactories

# This is the default min/max time that we are using in our mocks.
# They are made global so our tests can reference them directly.
DEFAULT_MIN_DT = datetime(2021, 1, 1, 0, 0, 0)
DEFAULT_MAX_DT = datetime(2022, 12, 31, 0, 0, 0)


class _MockConnection:
    def execute(self, query):
        """Execute a query over a sqlalchemy engine connection.

        Currently this mock assumes the query is always of the form:
        "select min(col), max(col) from table"
        where col is a datetime column since that's all that's necessary.
        This can be generalized if needed.

        Args:
            query: The SQL query to execute.
        """
        return [(DEFAULT_MIN_DT, DEFAULT_MAX_DT)]


class _MockSaEngine:
    @contextmanager
    def connect(self):
        """A contextmanager that yields a _MockConnection"""
        yield _MockConnection()


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None]
):
    """Creates a mock gx sql alchemy engine class

    Args:
        validate_batch_spec: A hook that can be used to validate the generated the batch spec
            passed into get_batch_data_and_markers
    """

    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            # We should likely let the user pass in an engine. In a SqlAlchemyExecutionEngine used in
            # non-mocked code the engine property is of the type:
            # from sqlalchemy.engine import Engine as SaEngine
            self.engine = _MockSaEngine()

        def get_batch_data_and_markers(  # type: ignore[override]
            self, batch_spec: SqlAlchemyDatasourceBatchSpec
        ) -> Tuple[BatchData, BatchMarkers]:
            validate_batch_spec(batch_spec)
            return BatchData(self), BatchMarkers(ge_load_time=None)

    return MockSqlAlchemyExecutionEngine


class ExecutionEngineDouble:
    def __init__(self, *args, **kwargs):
        pass

    def get_batch_data_and_markers(self, batch_spec) -> Tuple[BatchData, BatchMarkers]:
        return BatchData(self), BatchMarkers(ge_load_time=None)


@pytest.fixture
def inject_engine_lookup_double(monkeypatch: MonkeyPatch) -> ExecutionEngineDouble:  # type: ignore[misc]
    """
    Inject an execution engine test double into the _SourcesFactory.engine_lookup
    so that all Datasources use the execution engine double.
    Dynamically create a new subclass so that runtime type validation does not fail.
    """
    original_engine_override: Dict[MetaDatasource, ExecutionEngine] = {}
    for key in _SourceFactories.type_lookup.keys():
        if issubclass(type(key), MetaDatasource):
            original_engine_override[key] = key.execution_engine_override

    try:
        for source in original_engine_override.keys():
            source.execution_engine_override = ExecutionEngineDouble
        yield ExecutionEngineDouble
    finally:
        for source, engine in original_engine_override.items():
            source.execution_engine_override = engine
