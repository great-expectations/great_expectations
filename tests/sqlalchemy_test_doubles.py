from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, List


class MockSaInspector:
    def get_columns(self) -> List[Dict[str, Any]]:  # type: ignore[empty-body]
        ...

    def get_schema_names(self) -> List[str]:  # type: ignore[empty-body]
        ...

    def has_table(self, table_name: str, schema: str) -> bool:  # type: ignore[empty-body]
        ...


class Dialect:
    def __init__(self, dialect: str):
        self.name = dialect


class _MockConnection:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    @contextmanager
    def begin(self):
        yield _MockConnection(self.dialect)

    def execute(self, statement):
        self.statement = statement


class MockSaEngine:
    def __init__(self, dialect: Dialect):
        self.dialect = dialect

    @contextmanager
    def begin(self):
        yield _MockConnection(self.dialect)

    @contextmanager
    def connect(self):
        """A contextmanager that yields a _MockConnection"""
        yield _MockConnection(self.dialect)

    def execute(self, *args, **kwargs):
        """This method is needed because currently we sometimes use a
        connection in place of an engine.

        When this is cleaned up we should remove this method.
        """
        pass
