from typing import Generator, Iterable, Optional, Union

import pytest

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData


class MockSaEngine:
    def __init__(self, dialect: sa.engine.interfaces.Dialect):
        self.dialect = dialect

    def connect(self) -> None:
        pass


class MockResult:
    def fetchmany(self, recordcount: int):
        return None


class MockConnection:
    def execute(self, query: str):
        return MockResult()


_batch_selectable = sa.Table("my_table", sa.MetaData(), schema=None)


@pytest.fixture
def batch_selectable() -> sa.Table:
    return _batch_selectable


class MockSqlAlchemyExecutionEngine(SqlAlchemyExecutionEngine):
    def __init__(self, create_temp_table: bool = True, *args, **kwargs):
        self.engine = MockSaEngine(dialect=sa.dialects.sqlite.dialect())  # type: ignore[assignment] # FIXME CoP
        self._create_temp_table = create_temp_table
        self._connection = MockConnection()  # type: ignore[assignment] # FIXME CoP

        self._batch_manager = None  # type: ignore[assignment] # FIXME CoP

    @override
    def get_compute_domain(
        self,
        domain_kwargs: dict,
        domain_type: Union[str, MetricDomainTypes],
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> tuple[sa.Table, dict, dict]:
        return _batch_selectable, {}, {}


class MockBatchManager:
    active_batch_data = SqlAlchemyBatchData(
        execution_engine=MockSqlAlchemyExecutionEngine(),
        table_name="my_table",
    )

    def save_batch_data(self) -> None: ...


@pytest.fixture
def mock_sqlalchemy_execution_engine():
    execution_engine = MockSqlAlchemyExecutionEngine()
    execution_engine._batch_manager = MockBatchManager()
    return execution_engine


@pytest.fixture
def sql_data_connector_test_db_execution_engine() -> Generator[
    SqlAlchemyExecutionEngine, None, None
]:
    """Provide a sqlite ExecutionEngine pointing to the SQL data connector test database.

    The engine and its underlying connections are explicitly closed after use to avoid
    leaking sqlite3.Connection objects (which surface as ResourceWarning in CI).
    """
    db_file = file_relative_path(
        __file__,
        "../../test_sets/test_cases_for_sql_data_connector.db",
    )
    engine: sa.engine.Engine = sa.create_engine(f"sqlite:///{db_file}")
    execution_engine = SqlAlchemyExecutionEngine(engine=engine)

    try:
        yield execution_engine
    finally:
        execution_engine.close()
