from contextlib import contextmanager
from typing import Callable, Tuple

import great_expectations.zep.postgres_datasource as postgres_datasource
from great_expectations.core.batch import BatchData
from great_expectations.core.batch_spec import (
    BatchMarkers,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.zep.sources import _SourceFactories


@contextmanager
def sqlachemy_execution_engine_mock(
    validate_batch_spec: Callable[[SqlAlchemyDatasourceBatchSpec], None]
):
    ds_type_name: str = postgres_datasource.PostgresDatasource.__fields__[
        "type"
    ].default
    assert ds_type_name

    class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
        def __init__(self, *args, **kwargs):
            pass

        def get_batch_data_and_markers(  # type: ignore[override]
            self, batch_spec: SqlAlchemyDatasourceBatchSpec
        ) -> Tuple[BatchData, BatchMarkers]:
            validate_batch_spec(batch_spec)
            return BatchData(self), BatchMarkers(ge_load_time=None)

    original_engine = postgres_datasource.SqlAlchemyExecutionEngine
    try:
        postgres_datasource.SqlAlchemyExecutionEngine = MockSqlAlchemyExecutionEngine  # type: ignore[misc]
        # swapping engine_lookup entry
        _SourceFactories.engine_lookup.data[
            ds_type_name
        ] = MockSqlAlchemyExecutionEngine
        yield postgres_datasource.SqlAlchemyExecutionEngine
    finally:
        postgres_datasource.SqlAlchemyExecutionEngine = original_engine  # type: ignore[misc]
        _SourceFactories.engine_lookup.data[ds_type_name] = original_engine
