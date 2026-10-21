from __future__ import annotations

from typing import Any

import pytest

import great_expectations.compatibility.bigquery  # noqa: F401 # importing it registers the Double rule
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.sqlalchemy import sqlite

sqlalchemy_bigquery = pytest.importorskip("sqlalchemy_bigquery")

# Offline: these compile statements with the BigQuery dialect and never connect. They are
# marked `bigquery` so they run in the lane that installs sqlalchemy-bigquery.
pytestmark = pytest.mark.bigquery

requires_double = pytest.mark.skipif(
    not hasattr(sa, "Double"), reason="SQLAlchemy 1.4 has no generic Double type"
)


def _compile_for_bigquery(statement: Any) -> str:
    # The pyformat paramstyle is what the BigQuery DB-API uses; the dialect only annotates
    # parameters with their BigQuery type (`%(name:TYPE)s`) in that style.
    dialect = sqlalchemy_bigquery.BigQueryDialect(paramstyle="pyformat")
    return " ".join(str(statement.compile(dialect=dialect)).split())


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param(sa.select(sa.literal(0.5)), id="literal"),
        pytest.param(sa.select(sa.column("x")).where(sa.column("x") > 0.5), id="comparison"),
        pytest.param(
            sa.select(sa.column("x")).where(sa.column("x").between(0.1, 0.9)), id="between"
        ),
    ],
)
def test_float_bind_parameters_are_declared_float64(statement: Any) -> None:
    compiled = _compile_for_bigquery(statement)

    assert ":FLOAT64)s" in compiled
    assert "DOUBLE" not in compiled


@requires_double
def test_an_explicit_double_renders_as_float64() -> None:
    compiled = _compile_for_bigquery(sa.select(sa.cast(sa.column("x"), sa.Double())))

    assert compiled == "SELECT CAST(`x` AS FLOAT64) AS `x`"


@requires_double
def test_other_dialects_still_render_double() -> None:
    compiled = str(sa.cast(sa.column("x"), sa.Double()).compile(dialect=sqlite.dialect()))

    assert compiled == "CAST(x AS DOUBLE)"
