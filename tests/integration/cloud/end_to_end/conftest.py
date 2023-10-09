from __future__ import annotations

import logging
import os
from pprint import pformat as pf
from typing import Final, Generator, Literal, Protocol

import pytest

import great_expectations as gx
from great_expectations.compatibility.sqlalchemy import TextClause
from great_expectations.data_context import CloudDataContext
from great_expectations.execution_engine import SqlAlchemyExecutionEngine

LOGGER: Final = logging.getLogger("tests")


@pytest.fixture
def context() -> CloudDataContext:
    context = gx.get_context(
        mode="cloud",
        cloud_base_url=os.environ.get("MERCURY_BASE_URL"),
        cloud_organization_id=os.environ.get("MERCURY_ORGANIZATION_ID"),
        cloud_access_token=os.environ.get("MERCURY_ACCESS_TOKEN"),
    )
    assert isinstance(context, CloudDataContext)
    return context


class TableFactory(Protocol):
    def __call__(
        self,
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema_name: str | None = None,
    ) -> None:
        ...


@pytest.fixture(
    scope="class",
)
def table_factory() -> Generator[TableFactory, None, None]:
    """
    Class scoped.
    Given a SQLAlchemy engine, table_name and schema,
    create the table if it does not exist and drop it after the test class.
    """
    all_created_tables: dict[
        str, list[dict[Literal["table_name", "schema"], str | None]]
    ] = {}
    engines: dict[str, engine.Engine] = {}

    def _table_factory(
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema_name: str | None = None,
    ) -> None:
        sa_engine = gx_engine.engine
        LOGGER.info(
            f"Creating `{sa_engine.dialect.name}` table for {table_names} if it does not exist"
        )
        created_tables: list[
            dict[Literal["table_name", "schema_name"], str | None]
        ] = []

        with gx_engine.get_connection() as conn:
            transaction = conn.begin()
            if schema_name:
                conn.execute(TextClause(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
            for name in table_names:
                qualified_table_name = f"{schema_name}.{name}" if schema_name else name
                create_tables: str = (
                    f"CREATE TABLE IF NOT EXISTS {qualified_table_name}"
                    f" (id INTEGER, name VARCHAR(255), value INTEGER)"
                )
                conn.execute(TextClause(create_tables))

                created_tables.append(dict(table_name=name, schema_name=schema_name))
            transaction.commit()
        all_created_tables[sa_engine.dialect.name] = created_tables
        engines[sa_engine.dialect.name] = sa_engine

    yield _table_factory

    # teardown
    print(f"dropping tables\n{pf(all_created_tables)}")
    for dialect, tables in all_created_tables.items():
        engine = engines[dialect]
        with engine.connect() as conn:
            transaction = conn.begin()
            for table in tables:
                name = table["table_name"]
                schema = table["schema_name"]
                qualified_table_name = f"{schema}.{name}" if schema else name
                conn.execute(TextClause(f"DROP TABLE IF EXISTS {qualified_table_name}"))
            if schema:
                conn.execute(TextClause(f"DROP SCHEMA IF EXISTS {schema}"))
            transaction.commit()
