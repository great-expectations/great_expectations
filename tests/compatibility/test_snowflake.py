from __future__ import annotations

import importlib
import importlib.abc
import importlib.machinery
import importlib.util
import sys
import types
from typing import Optional, Sequence

import pytest

import great_expectations.compatibility
from great_expectations.compatibility import snowflake as snowflake_compatibility
from great_expectations.compatibility.typing_extensions import override

pytestmark = pytest.mark.unit

COMPATIBILITY_MODULE = "great_expectations.compatibility.snowflake"


class _SnowflakeDialectThatFailsToImport(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    """Stands in for a snowflake-sqlalchemy that is installed but raises while importing.

    snowflake-sqlalchemy 1.11.1 does this under SQLAlchemy 2.1: it subclasses a class
    that 2.1 renamed, so importing it raises AttributeError, not ImportError.
    """

    def __init__(self) -> None:
        self.executed: list[str] = []

    @override
    def find_spec(
        self,
        fullname: str,
        path: Optional[Sequence[str]],
        target: Optional[types.ModuleType] = None,
    ) -> Optional[importlib.machinery.ModuleSpec]:
        if fullname == "snowflake.sqlalchemy":
            return importlib.util.spec_from_loader(fullname, self, is_package=True)
        return None

    @override
    def create_module(self, spec: importlib.machinery.ModuleSpec) -> None:
        return None

    @override
    def exec_module(self, module: types.ModuleType) -> None:
        self.executed.append(module.__name__)
        raise AttributeError(
            "module 'sqlalchemy.orm.context' has no attribute 'ORMSelectCompileState'"
        )


def test_a_snowflake_dialect_that_fails_to_import_is_treated_as_not_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dialect = _SnowflakeDialectThatFailsToImport()
    snowflake_package = types.ModuleType("snowflake")
    snowflake_package.__path__ = []
    monkeypatch.setitem(sys.modules, "snowflake", snowflake_package)
    for name in [name for name in sys.modules if name.startswith("snowflake.")]:
        monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(sys, "meta_path", [dialect, *sys.meta_path])
    # A fresh import rebinds the package attribute; restore it for the tests that follow.
    monkeypatch.setattr(great_expectations.compatibility, "snowflake", snowflake_compatibility)
    monkeypatch.delitem(sys.modules, COMPATIBILITY_MODULE)

    reimported = importlib.import_module(COMPATIBILITY_MODULE)

    assert dialect.executed, "the failing dialect import was never attempted"
    assert reimported.URL is reimported.SNOWFLAKE_NOT_IMPORTED
