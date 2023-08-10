from __future__ import annotations

import logging
from pprint import pformat as pf
from typing import Final, Generator, Literal, Protocol

import pytest
from packaging.version import Version
from pytest import param

from great_expectations import get_context
from great_expectations.compatibility.sqlalchemy import (
    TextClause,
    engine,
    inspect,
)
from great_expectations.compatibility.sqlalchemy import (
    __version__ as sqlalchemy_version,
)
from great_expectations.data_context import EphemeralDataContext
from great_expectations.datasource.fluent import (
    DatabricksSQLDatasource,
    PostgresDatasource,
    SnowflakeDatasource,
    SQLDatasource,
)
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
)

SQLA_VERSION: Final = Version(sqlalchemy_version or "0.0.0")
LOGGER: Final = logging.getLogger("tests")

PG_TABLE: Final[str] = "test_table"
# trino container ships with default test tables
TRINO_TABLE: Final[str] = "customer"

# NOTE: can we create tables in trino?
# some of the trino tests probably don't make sense if we can't create tables
DO_NOT_CREATE_TABLES: set[str] = {"trino"}

# TODO: simplify this and possible get rid of this mapping once we have settled on
# all the naming conventions we want to support for different SQL dialects
TABLE_NAME_MAPPING: Final[dict[str, dict[str, str]]] = {
    "postgres": {
        "unquoted_lower": PG_TABLE.lower(),
        "quoted_lower": f'"{PG_TABLE.lower()}"',
        "unquoted_upper": PG_TABLE.upper(),
        "quoted_upper": f'"{PG_TABLE.upper()}"',
        "quoted_mixed": f'"{PG_TABLE.title()}"',
        "unquoted_mixed": PG_TABLE.title(),
    },
    "trino": {
        "unquoted_lower": TRINO_TABLE.lower(),
        "quoted_lower": f"'{TRINO_TABLE.lower()}'",
        # "unquoted_upper": TRINO_TABLE.upper(),
        # "quoted_upper": f"'{TRINO_TABLE.upper()}'",
        # "quoted_mixed": f"'TRINO_TABLE.title()'",
        # "unquoted_mixed": TRINO_TABLE.title(),
    },
    "databricks_sql": {
        "unquoted_lower": PG_TABLE.lower(),
        "quoted_lower": f"`{PG_TABLE.lower()}`",
        "unquoted_upper": PG_TABLE.upper(),
        "quoted_upper": f"`{PG_TABLE.upper()}`",
        "quoted_mixed": f"`{PG_TABLE.title()}`",
        "unquoted_mixed": PG_TABLE.title(),
    },
    "snowflake": {
        "unquoted_lower": PG_TABLE.lower(),
        "quoted_lower": f'"{PG_TABLE.lower()}"',
        "unquoted_upper": PG_TABLE.upper(),
        "quoted_upper": f'"{PG_TABLE.upper()}"',
        "quoted_mixed": f"`{PG_TABLE.title()}`",
        "unquoted_mixed": PG_TABLE.title(),
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
def capture_engine_logs(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Capture SQLAlchemy engine logs and display them if the test fails."""
    caplog.set_level(logging.INFO, logger="sqlalchemy.engine")
    return caplog


@pytest.fixture(scope="function")
def table_factory(
    capture_engine_logs: pytest.LogCaptureFixture,
) -> Generator[TableFactory, None, None]:
    """
    Given a SQLALchemy engine, table_name and schema,
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
        if engine.dialect.name in DO_NOT_CREATE_TABLES:
            LOGGER.info(
                f"Skipping table creation for {table_names} for {engine.dialect.name}"
            )
            return
        LOGGER.info(
            f"SQLA:{SQLA_VERSION} - Creating `{engine.dialect.name}` table for {table_names} if it does not exist"
        )
        created_tables: list[dict[Literal["table_name", "schema"], str | None]] = []
        with engine.connect() as conn:
            if schema:
                conn.execute(TextClause(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            for name in table_names:
                qualified_table_name = f"{schema}.{name}" if schema else name
                conn.execute(
                    TextClause(
                        f"CREATE TABLE IF NOT EXISTS {qualified_table_name} (id INTEGER, name VARCHAR(255))"
                    )
                )
                if SQLA_VERSION >= Version("2.0"):
                    conn.commit()
                created_tables.append(dict(table_name=name, schema=schema))
        all_created_tables[engine.dialect.name] = created_tables
        engines[engine.dialect.name] = engine

    yield _table_factory

    # teardown
    print(f"dropping tables\n{pf(all_created_tables)}")
    for dialect, tables in all_created_tables.items():
        engine = engines[dialect]
        with engine.connect() as conn:
            for table in tables:
                name = table["table_name"]
                schema = table["schema"]
                qualified_table_name = f"{schema}.{name}" if schema else name
                conn.execute(TextClause(f"DROP TABLE IF EXISTS {qualified_table_name}"))
            if SQLA_VERSION >= Version("2.0"):
                conn.commit()


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


@pytest.fixture
def databricks_sql_ds(context: EphemeralDataContext) -> DatabricksSQLDatasource:
    ds = context.sources.add_databricks_sql(
        "databricks_sql",
        connection_string=r"databricks+connector://token:${DBS_TOKEN}@${DBS_HOST}:443/cloud_events?http_path=${DBS_HTTP_PATH}&catalog=catalog&schema=schema",
    )
    return ds


@pytest.fixture
def snowflake_ds(context: EphemeralDataContext) -> PostgresDatasource:
    ds = context.sources.add_snowflake(
        "snowflake",
        connection_string="snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_ACCOUNT}/ci/public?warehouse=ci&role=ci",
    )
    return ds


@pytest.mark.parametrize(
    "asset_name",
    [
        param("unquoted_lower"),
        param("quoted_lower"),
        param(
            "unquoted_upper",
            marks=[pytest.mark.xfail(reason="TODO: fix this")],
        ),
        param("quoted_upper"),
        param("quoted_mixed"),
        param(
            "unquoted_mixed",
            marks=[pytest.mark.xfail(reason="TODO: fix this")],
        ),
    ],
)
class TestTableIdentifiers:
    @pytest.mark.trino
    def test_trino(self, trino_ds: SQLDatasource, asset_name: str):
        table_name = TABLE_NAME_MAPPING["trino"].get(asset_name)
        if not table_name:
            pytest.skip(f"no '{asset_name}' table_name for trino")

        table_names: list[str] = inspect(trino_ds.get_engine()).get_table_names()
        print(f"trino tables:\n{pf(table_names)}))")

        trino_ds.add_table_asset(asset_name, table_name=table_name)

    @pytest.mark.postgresql
    def test_postgres(
        self,
        postgres_ds: PostgresDatasource,
        asset_name: str,
        table_factory: TableFactory,
    ):
        table_name: str = TABLE_NAME_MAPPING["postgres"][asset_name]
        # create table
        table_factory(
            engine=postgres_ds.get_engine(), table_names={table_name}, schema="public"
        )

        table_names: list[str] = inspect(postgres_ds.get_engine()).get_table_names()
        print(f"postgres tables:\n{pf(table_names)}))")

        postgres_ds.add_table_asset(asset_name, table_name=table_name)

    @pytest.mark.skip(reason="TODO: implement Databricks SQL integration service")
    @pytest.mark.databricks
    def test_databricks_sql(
        self,
        databricks_sql_ds: DatabricksSQLDatasource,
        asset_name: str,
        # table_factory: TableFactory,
    ):
        table_name = TABLE_NAME_MAPPING["databricks"].get(asset_name)
        if not table_name:
            pytest.skip(f"no '{asset_name}' table_name for databricks")

        table_names: list[str] = inspect(
            databricks_sql_ds.get_engine()
        ).get_table_names()
        print(f"databricks tables:\n{pf(table_names)}))")

        databricks_sql_ds.add_table_asset(asset_name, table_name=table_name)

    @pytest.mark.snowflake
    def test_snowflake(
        self,
        snowflake_ds: SnowflakeDatasource,
        asset_name: str,
        table_factory: TableFactory,
    ):
        table_name: str = TABLE_NAME_MAPPING["snowflake"][asset_name]
        # create table
        # with snowflake_ds.get_engine().connect() as conn:
        #     conn.execute("USE SCHEMA ci.public")
        table_factory(engine=snowflake_ds.get_engine(), table_names={table_name})

        table_names: list[str] = inspect(snowflake_ds.get_engine()).get_table_names()
        print(f"postgres tables:\n{pf(table_names)}))")

        snowflake_ds.add_table_asset(asset_name, table_name=table_name)

    @pytest.mark.parametrize(
        "datasource_type",
        [
            # param("trino", marks=[pytest.mark.trino]),
            # param("postgres", marks=[pytest.mark.postgresql]),
            param("snowflake", marks=[pytest.mark.snowflake]),
        ],
    )
    def test_checkpoint_run(
        self,
        request: pytest.FixtureRequest,
        context: EphemeralDataContext,
        table_factory: TableFactory,
        asset_name: str,
        datasource_type: str,
    ):
        datasource: SQLDatasource = request.getfixturevalue(f"{datasource_type}_ds")

        table_name: str | None = TABLE_NAME_MAPPING[datasource_type].get(asset_name)
        if not table_name:
            pytest.skip(f"no '{asset_name}' table_name for {datasource_type}")

        # create table
        table_factory(engine=datasource.get_engine(), table_names={table_name})

        asset = datasource.add_table_asset(asset_name, table_name=table_name)

        suite = context.add_expectation_suite(
            expectation_suite_name=f"{datasource.name}-{asset.name}"
        )
        suite.add_expectation(
            expectation_configuration=ExpectationConfiguration(
                expectation_type="expect_column_values_to_not_be_null",
                kwargs={
                    "column": "name",
                    "mostly": 1,
                },
            )
        )
        suite = context.add_or_update_expectation_suite(expectation_suite=suite)

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
