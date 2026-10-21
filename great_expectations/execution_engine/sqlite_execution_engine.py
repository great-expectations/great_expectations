from great_expectations.execution_engine.sqlalchemy_execution_engine import (
    SqlAlchemyExecutionEngine,
)

__all__ = ["SqliteExecutionEngine"]


class SqliteExecutionEngine(SqlAlchemyExecutionEngine):
    """SqlAlchemyExecutionEngine for SQLite databases."""

    pass
