from __future__ import annotations

import os
import pathlib
import sys
import types

import pytest

from great_expectations.compatibility import postgresql as postgresql_compatibility
from great_expectations.compatibility.postgresql import resolve_postgresql_driver
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

# The driver SQLAlchemy picks for a `postgresql://` URL with no driver: psycopg2 before 2.1,
# psycopg (3) from 2.1 on. Loading the dialect class does not import the driver.
DEFAULT_DRIVER = sa.engine.make_url("postgresql://").get_dialect().driver

default_is_psycopg2 = pytest.mark.skipif(
    DEFAULT_DRIVER != "psycopg2", reason="SQLAlchemy 2.1+ defaults to psycopg"
)
default_is_not_psycopg2 = pytest.mark.skipif(
    DEFAULT_DRIVER == "psycopg2", reason="SQLAlchemy before 2.1 defaults to psycopg2"
)


def _only_importable(*module_names: str):
    return lambda module_name: module_name in module_names


@pytest.mark.unit
@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg2://user@host/db",
        "postgresql+psycopg://user@host/db",
        "sqlite://",
        "mysql+pymysql://user@host/db",
        "not a url",
    ],
)
def test_urls_that_name_a_driver_or_another_dialect_are_unchanged(
    url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(postgresql_compatibility, "_is_importable", _only_importable("psycopg2"))

    assert resolve_postgresql_driver(url) is url


@pytest.mark.unit
def test_a_driverless_url_is_unchanged_when_its_default_driver_is_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        postgresql_compatibility, "_is_importable", _only_importable("psycopg", "psycopg2")
    )
    url = "postgresql://user@host/db"

    assert resolve_postgresql_driver(url) is url


@pytest.mark.unit
@default_is_psycopg2
def test_before_sqlalchemy_2_1_a_driverless_url_is_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postgresql_compatibility, "_is_importable", _only_importable("psycopg2"))
    url = "postgresql://user@host/db"

    assert resolve_postgresql_driver(url) is url


@pytest.mark.unit
@default_is_not_psycopg2
def test_a_driverless_url_falls_back_to_psycopg2_when_only_psycopg2_is_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postgresql_compatibility, "_is_importable", _only_importable("psycopg2"))

    resolved = resolve_postgresql_driver("postgresql://user:p%40ss@host:5433/db?sslmode=require")

    assert isinstance(resolved, sa.engine.URL)
    assert resolved.drivername == "postgresql+psycopg2"
    # Everything but the driver survives, including the password, which a rendered string masks.
    assert (resolved.username, resolved.password, resolved.host, resolved.port) == (
        "user",
        "p@ss",
        "host",
        5433,
    )
    assert (resolved.database, dict(resolved.query)) == ("db", {"sslmode": "require"})


@pytest.mark.unit
def test_a_module_that_is_installed_but_fails_to_import_is_not_importable(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "gx_broken_driver.py").write_text('raise ImportError("no pq wrapper available")\n')
    monkeypatch.syspath_prepend(str(tmp_path))

    assert postgresql_compatibility._is_importable("gx_broken_driver") is False
    assert postgresql_compatibility._is_importable("gx_no_such_driver") is False
    assert postgresql_compatibility._is_importable("json") is True


@pytest.mark.unit
@default_is_not_psycopg2
def test_a_driverless_url_falls_back_to_psycopg2_when_its_default_driver_fails_to_import(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # psycopg (3) installed without its binary extra, on a host with no libpq.
    (tmp_path / "psycopg.py").write_text('raise ImportError("no pq wrapper available")\n')
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, "psycopg", raising=False)
    monkeypatch.setitem(sys.modules, "psycopg2", types.ModuleType("psycopg2"))

    resolved = resolve_postgresql_driver("postgresql://user@host/db")

    assert isinstance(resolved, sa.engine.URL)
    assert resolved.drivername == "postgresql+psycopg2"


@pytest.mark.unit
@default_is_not_psycopg2
def test_a_driverless_url_is_unchanged_when_no_postgresql_driver_is_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(postgresql_compatibility, "_is_importable", _only_importable())
    url = "postgresql://user@host/db"

    assert resolve_postgresql_driver(url) is url


@pytest.mark.postgresql
def test_a_driverless_url_connects_with_whichever_driver_is_installed() -> None:
    host = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    engine = SqlAlchemyExecutionEngine(connection_string=f"postgresql://postgres@{host}/test_ci")

    with engine.engine.connect() as connection:
        assert connection.execute(sa.text("SELECT 1")).scalar() == 1
