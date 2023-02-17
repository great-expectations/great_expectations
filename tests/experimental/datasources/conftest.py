from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Callable, Dict, Generator, List, Type

import pytest
from pytest import MonkeyPatch

from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.experimental.datasources.interfaces import Datasource
from great_expectations.experimental.datasources.sources import _SourceFactories

logger = logging.getLogger(__name__)


class MockSaInspector:
    def get_columns(self) -> list[dict[str, Any]]:  # type: ignore[empty-body]
        ...

    def get_schema_names(self) -> list[str]:  # type: ignore[empty-body]
        ...

    def has_table(self, table_name: str, schema: str) -> bool:  # type: ignore[empty-body]
        ...


class Dialect:
    def __init__(self, dialect: str):
        self.name = dialect


class _MockConnection:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect


class MockSaEngine:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    @contextmanager
    def connect(self):
        """A contextmanager that yields a _MockConnection"""
        yield _MockConnection(self.dialect)


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None],
    dialect: str,
    splitter_query_response: List[Dict[str, Any]],
):
    """Creates a mock gx sql alchemy engine class

    Args:
        validate_batch_spec: A hook that can be used to validate the generated the batch spec
            passed into get_batch_data_and_markers
        dialect: A string representing the SQL Engine dialect. Examples include: postgresql, sqlite
        splitter_query_response: A list of dictionaries. Each dictionary is a row returned back from
            the splitter query. The keys are the column names and the value is the column values, eg:
            [{'year': 2021, 'month': 1}, {'year': 2021, 'month': 2}]
    """

    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            # We should likely let the user pass in an engine. In a SqlAlchemyExecutionEngine used in
            # non-mocked code the engine property is of the type:
            # from sqlalchemy.engine import Engine as SaEngine
            self.engine = MockSaEngine(dialect=Dialect(dialect))

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

            return [Row(row_dict) for row_dict in splitter_query_response]

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
