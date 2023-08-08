from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import Final, Generator, Literal, Protocol

import pytest
from pytest import param

from great_expectations import get_context
from great_expectations.compatibility.sqlalchemy import (
    TextClause,
    engine,
    inspect,
)
from great_expectations.data_context import EphemeralDataContext
from great_expectations.datasource.fluent import (
    PostgresDatasource,
    SQLDatasource,
)
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
)

LOGGER: Final = logging.getLogger("tests")

PG_TABLE: Final[str] = "test_table"
# trino container ships with default test tables
TRINO_TABLE: Final[str] = "customer"

TABLE_NAME_MAPPING: Final[dict[str, dict[str, str]]] = {
    "postgres": {
        "unquoted_lower": PG_TABLE.lower(),
        "quoted_lower": f"'{PG_TABLE.lower()}'",
        "unquoted_upper": PG_TABLE.upper(),
        "quoted_upper": f"'{PG_TABLE.upper()}'",
        "unquoted_mixed": PG_TABLE.title(),
    },
    "trino": {
        "unquoted_lower": TRINO_TABLE.lower(),
        "quoted_lower": f"'{TRINO_TABLE.lower()}'",
        "unquoted_upper": TRINO_TABLE.upper(),
        "quoted_upper": f"'{TRINO_TABLE.upper()}'",
        "unquoted_mixed": TRINO_TABLE.title(),
    },
}


@pytest.fixture
def context() -> EphemeralDataContext:
    ctx = get_context(cloud_mode=False)
    assert isinstance(ctx, EphemeralDataContext)
    return ctx


class TableFactory(Protocol):
    def __call__(
        self,
        engine: engine.Engine,
        table_names: set[str],
        schema: str | None = None,
    ) -> None:
        ...


@pytest.fixture(scope="function")
def table_factory() -> Generator[TableFactory, None, None]:
    """
    Given a an SQLALchemy engine, table_name and schema,
    create the table if it does not exist and drop it after the test.
    """
    all_created_tables: dict[
        str, list[dict[Literal["table_name", "schema"], str | None]]
    ] = {}
    engines: dict[str, engine.Engine] = {}

    def _table_factory(
        engine: engine.Engine,
        table_names: set[str],
        schema: str | None = None,
    ) -> None:
        LOGGER.info(
            f"Creating `{engine.dialect.name}` table for {table_names} if it does not exist"
        )
        created_tables: list[dict[Literal["table_name", "schema"], str | None]] = []
        with engine.connect() as conn:
            if schema:
                conn.execute(f"CREATE SCHEMA {schema}")
            for name in table_names:
                qualified_table_name = f"{schema}.{name}" if schema else name
                stmt = TextClause(
                    f"CREATE TABLE IF NOT EXISTS {qualified_table_name} (id INTEGER, name VARCHAR(255))"
                )
                LOGGER.info(stmt)
                conn.execute(stmt)
                created_tables.append(dict(table_name=name, schema=schema))
        all_created_tables[engine.dialect.name] = created_tables
        engines[engine.dialect.name] = engine

    yield _table_factory

    print(f"dropping up tables\n{pf(all_created_tables)}")
    for dialect, tables in all_created_tables.items():
        engine = engines[dialect]
        with engine.connect() as conn:
            for table in tables:
                name = table["table_name"]
                schema = table["schema"]
                qualified_table_name = f"{schema}.{name}" if schema else name
                stmt = TextClause(f"DROP TABLE IF EXISTS {qualified_table_name}")
                LOGGER.info(stmt)
                conn.execute(stmt)


@pytest.fixture
def trino_ds(
    context: EphemeralDataContext,
    table_factory: TableFactory,
) -> SQLDatasource:
    ds = context.sources.add_sql(
        "trino",
        connection_string="trino://user:@localhost:8088/tpch/sf1",
    )
    # trino container ships with default test tables so there is no need to create them
    return ds


@pytest.fixture
def postgres_ds(
    context: EphemeralDataContext,
    table_factory: TableFactory,
) -> PostgresDatasource:
    ds = context.sources.add_postgres(
        "postgres",
        connection_string="postgresql+psycopg2://postgres:postgres@localhost:5432/test_ci",
    )

    table_factory(
        engine=ds.get_engine(),
        table_names={
            v
            for (k, v) in TABLE_NAME_MAPPING["postgres"].items()
            if k.startswith("unquoted")
        },
    )
    # assert False
    return ds


@pytest.mark.parametrize(
    "asset_name",
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
        param(
            "unquoted_mixed",
            marks=[pytest.mark.xfail(reason="table names should be lowercase")],
        ),
    ],
)
class TestTableIdentifiers:
    @pytest.mark.trino
    def test_trino(self, trino_ds: SQLDatasource, asset_name: str):
        table_names: list[str] = inspect(trino_ds.get_engine()).get_table_names()
        print(f"trino tables:\n{pf(table_names)}))")

        trino_ds.add_table_asset(
            asset_name, table_name=TABLE_NAME_MAPPING["trino"][asset_name]
        )

    @pytest.mark.postgresql
    def test_postgres(self, postgres_ds: PostgresDatasource, asset_name: str):
        table_names: list[str] = inspect(postgres_ds.get_engine()).get_table_names()
        print(f"postgres tables:\n{pf(table_names)}))")

        postgres_ds.add_table_asset(
            asset_name, table_name=TABLE_NAME_MAPPING["postgres"][asset_name]
        )

    @pytest.mark.parametrize(
        "datasource_type",
        [
            param("trino", marks=[pytest.mark.trino]),
            param("postgres", marks=[pytest.mark.postgresql]),
        ],
    )
    def test_checkpoint_run(
        self,
        request: pytest.FixtureRequest,
        context: EphemeralDataContext,
        asset_name: str,
        datasource_type: str,
    ):
        datasource: SQLDatasource = request.getfixturevalue(f"{datasource_type}_ds")
        asset = datasource.add_table_asset(
            asset_name, table_name=TABLE_NAME_MAPPING[datasource_type][asset_name]
        )

        suite = context.add_expectation_suite(
            expectation_suite_name=f"{datasource.name}-{asset.name}"
        )
        suite.add_expectation(
            expectation_configuration=ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "val",
                    "mostly": 1,
                },
            )
        )

        checkpoint_config = {
            "name": f"{datasource.name}-{asset.name}",
            "validations": [
                {
                    "expectation_suite_name": suite.expectation_suite_name,
                    "batch_request": {
                        "datasource_name": datasource.name,
                        "data_asset_name": asset.name,
                    },
                }
            ],
        }
        checkpoint = context.add_checkpoint(  # type: ignore[call-overload]
            **checkpoint_config,
        )
        result = checkpoint.run()

        print(f"result:\n{pf(result)}")
        assert result.success is True


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
