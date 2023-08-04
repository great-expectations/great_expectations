from __future__ import annotations

from typing import Final

import pytest
from pytest import param

from great_expectations import get_context
from great_expectations.data_context import EphemeralDataContext
from great_expectations.datasource.fluent import (
    PostgresDatasource,
    SQLDatasource,
)

PG_TABLE: Final[str] = "pg_aggregate"
TRINO_TABLE: Final[str] = "customer"

TABLE_NAME_MAPPING: Final[dict[str, dict[str, str]]] = {
    "postgres": {
        "unquoted_lower": PG_TABLE.lower(),
        "quoted_lower": f"'{PG_TABLE.lower()}'",
        "unquoted_upper": PG_TABLE.upper(),
        "quoted_upper": f"'{PG_TABLE.upper()}'",
    },
    "trino": {
        "unquoted_lower": TRINO_TABLE.lower(),
        "quoted_lower": f"'{TRINO_TABLE.lower()}'",
        "unquoted_upper": TRINO_TABLE.upper(),
        "quoted_upper": f"'{TRINO_TABLE.upper()}'",
    },
}


@pytest.fixture
def context() -> EphemeralDataContext:
    ctx = get_context(cloud_mode=False)
    assert isinstance(ctx, EphemeralDataContext)
    return ctx


@pytest.fixture
def trino_ds(context: EphemeralDataContext) -> SQLDatasource:
    ds = context.sources.add_sql(
        "trino",
        connection_string="trino://user:@localhost:8088/tpch/sf1",
    )
    return ds


@pytest.fixture
def postgres_ds(context: EphemeralDataContext) -> PostgresDatasource:
    ds = context.sources.add_postgres(
        "postgres",
        connection_string="postgresql+psycopg2://postgres:postgres@localhost:5432/test_ci",
    )
    return ds


@pytest.mark.parametrize(
    ["asset_name"],
    [
        param("unquoted_lower"),
        param("quoted_lower"),
        param(
            "unquoted_upper",
            marks=[pytest.mark.xfail(reason="table names should be lowercase")],
        ),
        param(
            "quoted_upper",
            marks=[pytest.mark.xfail(reason="table names should be lowercase")],
        ),
    ],
)
class TestTableIdentifiers:
    @pytest.mark.trino
    def test_trino(self, trino_ds: SQLDatasource, asset_name: str):
        trino_ds.add_table_asset(
            asset_name, table_name=TABLE_NAME_MAPPING["trino"][asset_name]
        )

    @pytest.mark.postgresql
    def test_postgres(self, postgres_ds: PostgresDatasource, asset_name: str):
        postgres_ds.add_table_asset(
            asset_name, table_name=TABLE_NAME_MAPPING["postgres"][asset_name]
        )


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
