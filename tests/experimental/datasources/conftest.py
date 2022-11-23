import logging
from typing import Callable, Dict, Tuple

import pytest
from pytest import MonkeyPatch

from great_expectations.execution_engine import (
    ExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.experimental.datasources.metadatasource import MetaDatasource

LOGGER = logging.getLogger(__name__)

from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.experimental.datasources.sources import _SourceFactories


def sqlachemy_execution_engine_mock_cls(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None]
):
    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            pass

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
