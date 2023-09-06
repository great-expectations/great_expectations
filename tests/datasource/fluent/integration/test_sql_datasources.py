from __future__ import annotations

import logging
import os
import pathlib
import sys
import uuid
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Final,
    Generator,
    Literal,
    Protocol,
    Sequence,
    TypedDict,
)

import pytest
from packaging.version import Version
from pytest import param

from great_expectations import get_context
from great_expectations.compatibility.sqlalchemy import (
    TextClause,
    engine,
    inspect,
    quoted_name,
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
    SqliteDatasource,
)
from great_expectations.execution_engine.sqlalchemy_dialect import quote_str
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

PYTHON_VERSION: Final[
    Literal["py38", "py39", "py310", "py311"]
] = f"py{sys.version_info.major}{sys.version_info.minor}"  # type: ignore[assignment] # str for each python version
SQLA_VERSION: Final = Version(sqlalchemy_version or "0.0.0")
LOGGER: Final = logging.getLogger("tests")

TEST_TABLE_NAME: Final[str] = "test_table"
# trino container ships with default test tables
TRINO_TABLE: Final[str] = "customer"

# NOTE: can we create tables in trino?
# some of the trino tests probably don't make sense if we can't create tables
DO_NOT_CREATE_TABLES: set[str] = {"trino"}

DatabaseType: TypeAlias = Literal[
    "trino", "postgres", "databricks_sql", "snowflake", "sqlite"
]
TableNameCase: TypeAlias = Literal[
    "quoted_lower",
    "quoted_mixed",
    "quoted_upper",
    "unquoted_lower",
    "unquoted_mixed",
    "unquoted_upper",
]

# TODO: simplify this and possible get rid of this mapping once we have settled on
# all the naming conventions we want to support for different SQL dialects
# NOTE: commented out are tests we know fail for individual datasources. Ideally all
# test cases should work for all datasrouces
TABLE_NAME_MAPPING: Final[dict[DatabaseType, dict[TableNameCase, str]]] = {
    "postgres": {
        "unquoted_lower": TEST_TABLE_NAME.lower(),
        "quoted_lower": f'"{TEST_TABLE_NAME.lower()}"',
        # "unquoted_upper": TEST_TABLE_NAME.upper(),
        "quoted_upper": f'"{TEST_TABLE_NAME.upper()}"',
        "quoted_mixed": f'"{TEST_TABLE_NAME.title()}"',
        # "unquoted_mixed": TEST_TABLE_NAME.title(),
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
        "unquoted_lower": TEST_TABLE_NAME.lower(),
        "quoted_lower": f"`{TEST_TABLE_NAME.lower()}`",
        "unquoted_upper": TEST_TABLE_NAME.upper(),
        "quoted_upper": f"`{TEST_TABLE_NAME.upper()}`",
        "quoted_mixed": f"`{TEST_TABLE_NAME.title()}`",
        "unquoted_mixed": TEST_TABLE_NAME.title(),
    },
    "snowflake": {
        "unquoted_lower": TEST_TABLE_NAME.lower(),
        "quoted_lower": f'"{TEST_TABLE_NAME.lower()}"',
        "unquoted_upper": TEST_TABLE_NAME.upper(),
        "quoted_upper": f'"{TEST_TABLE_NAME.upper()}"',
        "quoted_mixed": f'"{TEST_TABLE_NAME.title()}"',
        # "unquoted_mixed": TEST_TABLE_NAME.title(),
    },
    "sqlite": {
        "unquoted_lower": TEST_TABLE_NAME.lower(),
        "quoted_lower": f'"{TEST_TABLE_NAME.lower()}"',
        "unquoted_upper": TEST_TABLE_NAME.upper(),
        "quoted_upper": f'"{TEST_TABLE_NAME.upper()}"',
        "quoted_mixed": f'"{TEST_TABLE_NAME.title()}"',
        "unquoted_mixed": TEST_TABLE_NAME.title(),
    },
}


class Row(TypedDict):
    id: int
    name: str
    UPPER: str
    lower: str


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
        data: Sequence[Row] = ...,
    ) -> None:
        ...


def get_random_identifier_name() -> str:
    guid = uuid.uuid4()
    return f"i{guid.hex}"


@pytest.fixture(scope="function")
def capture_engine_logs(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Capture SQLAlchemy engine logs and display them if the test fails."""
    caplog.set_level(logging.INFO, logger="sqlalchemy.engine")
    return caplog


@pytest.fixture
def silence_sqla_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SQLALCHEMY_SILENCE_UBER_WARNING", "1")


@pytest.fixture(scope="function")
def table_factory(
    capture_engine_logs: pytest.LogCaptureFixture,
    silence_sqla_warnings: None,  # TODO: remove this
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
        data: Sequence[Row] = tuple(),
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
            upper: str = quote_str("UPPER", dialect=engine.dialect.name)
            lower: str = quote_str("lower", dialect=engine.dialect.name)
            transaction = conn.begin()
            if schema:
                conn.execute(TextClause(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            for name in table_names:
                qualified_table_name = f"{schema}.{name}" if schema else name
                create_tables = f"CREATE TABLE IF NOT EXISTS {qualified_table_name} (id INTEGER, name VARCHAR(255), {upper} VARCHAR(255), {lower} VARCHAR(255))"
                conn.execute(TextClause(create_tables))
                if data:
                    insert_data = f"INSERT INTO {qualified_table_name} (id, name, {upper}, {lower}) VALUES (:id, :name, :UPPER, :lower)"
                    conn.execute(TextClause(insert_data), data)

                created_tables.append(dict(table_name=name, schema=schema))
            transaction.commit()
        all_created_tables[engine.dialect.name] = created_tables
        engines[engine.dialect.name] = engine

    yield _table_factory

    # teardown
    print(f"dropping tables\n{pf(all_created_tables)}")
    for dialect, tables in all_created_tables.items():
        engine = engines[dialect]
        with engine.connect() as conn:
            transaction = conn.begin()
            for table in tables:
                name = table["table_name"]
                schema = table["schema"]
                qualified_table_name = f"{schema}.{name}" if schema else name
                conn.execute(TextClause(f"DROP TABLE IF EXISTS {qualified_table_name}"))
            if schema:
                conn.execute(TextClause(f"DROP SCHEMA IF EXISTS {schema}"))
            transaction.commit()


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
def databricks_creds_populated() -> bool:
    if (
        os.getenv("DATABRICKS_TOKEN")
        or os.getenv("DATABRICKS_HOST")
        or os.getenv("DATABRICKS_HTTP_PATH")
    ):
        return True
    return False


@pytest.fixture
def databricks_sql_ds(
    context: EphemeralDataContext, databricks_creds_populated: bool
) -> DatabricksSQLDatasource:
    if not databricks_creds_populated:
        pytest.skip("no databricks credentials")
    ds = context.sources.add_databricks_sql(
        "databricks_sql",
        connection_string="databricks://token:"
        "${DATABRICKS_TOKEN}@${DATABRICKS_HOST}:443"
        "/"
        + PYTHON_VERSION
        + "?http_path=${DATABRICKS_HTTP_PATH}&catalog=ci&schema="
        + PYTHON_VERSION,
    )
    return ds


@pytest.fixture
def snowflake_creds_populated() -> bool:
    if os.getenv("SNOWFLAKE_CI_USER_PASSWORD") or os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return True
    return False


@pytest.fixture
def snowflake_ds(
    context: EphemeralDataContext, snowflake_creds_populated: bool
) -> SnowflakeDatasource:
    if not snowflake_creds_populated:
        LOGGER.warning("no snowflake credentials")
    ds = context.sources.add_snowflake(
        "snowflake",
        connection_string="snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@${SNOWFLAKE_CI_ACCOUNT}/ci/public?warehouse=ci&role=ci",
    )
    return ds


@pytest.fixture
def sqlite_ds(
    context: EphemeralDataContext, tmp_path: pathlib.Path
) -> SqliteDatasource:
    ds = context.sources.add_sqlite(
        "sqlite", connection_string=f"sqlite:///{tmp_path}/test.db"
    )
    return ds


@pytest.fixture(
    params=[
        param(
            "trino",
            marks=[
                pytest.mark.trino,
                pytest.mark.skip(reason="cannot create trino tables"),
            ],
        ),
        param("postgres", marks=[pytest.mark.postgresql]),
        param("databricks_sql", marks=[pytest.mark.databricks]),
        param("snowflake", marks=[pytest.mark.snowflake]),
        param("sqlite", marks=[pytest.mark.sqlite]),
    ]
)
def all_sql_datasources(
    request: pytest.FixtureRequest,
) -> Generator[SQLDatasource, None, None]:
    datasource = request.getfixturevalue(f"{request.param}_ds")
    yield datasource


@pytest.mark.parametrize(
    "asset_name",
    [
        param("unquoted_lower"),
        param("quoted_lower"),
        param("unquoted_upper"),
        param("quoted_upper"),
        param("quoted_mixed"),
        param("unquoted_mixed"),
    ],
)
class TestTableIdentifiers:
    @pytest.mark.trino
    def test_trino(self, trino_ds: SQLDatasource, asset_name: TableNameCase):
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
        asset_name: TableNameCase,
        table_factory: TableFactory,
    ):
        table_name = TABLE_NAME_MAPPING["postgres"].get(asset_name)
        if not table_name:
            pytest.skip(f"no '{asset_name}' table_name for postgres")
        # create table
        table_factory(engine=postgres_ds.get_engine(), table_names={table_name})

        table_names: list[str] = inspect(postgres_ds.get_engine()).get_table_names()
        print(f"postgres tables:\n{pf(table_names)}))")

        postgres_ds.add_table_asset(asset_name, table_name=table_name)

    @pytest.mark.databricks
    def test_databricks_sql(
        self,
        databricks_sql_ds: DatabricksSQLDatasource,
        asset_name: TableNameCase,
        table_factory: TableFactory,
    ):
        table_name = TABLE_NAME_MAPPING["databricks_sql"].get(asset_name)
        if not table_name:
            pytest.skip(f"no '{asset_name}' table_name for databricks")
        # create table
        table_factory(
            engine=databricks_sql_ds.get_engine(),
            table_names={table_name},
            schema=PYTHON_VERSION,
        )

        table_names: list[str] = inspect(
            databricks_sql_ds.get_engine()
        ).get_table_names(schema=PYTHON_VERSION)
        print(f"databricks tables:\n{pf(table_names)}))")

        databricks_sql_ds.add_table_asset(
            asset_name, table_name=table_name, schema_name=PYTHON_VERSION
        )

    @pytest.mark.snowflake
    def test_snowflake(
        self,
        snowflake_ds: SnowflakeDatasource,
        asset_name: TableNameCase,
        table_factory: TableFactory,
    ):
        table_name = TABLE_NAME_MAPPING["snowflake"].get(asset_name)
        if not table_name:
            pytest.skip(f"no '{asset_name}' table_name for snowflake")
        if not snowflake_ds:
            pytest.skip("no snowflake datasource")
        # create table
        schema = get_random_identifier_name()
        table_factory(
            engine=snowflake_ds.get_engine(),
            table_names={table_name},
            schema=schema,
        )

        table_names: list[str] = inspect(snowflake_ds.get_engine()).get_table_names(
            schema=schema
        )
        print(f"snowflake tables:\n{pf(table_names)}))")

        snowflake_ds.add_table_asset(
            asset_name, table_name=table_name, schema_name=schema
        )

    @pytest.mark.sqlite
    def test_sqlite(
        self,
        sqlite_ds: SqliteDatasource,
        asset_name: TableNameCase,
        table_factory: TableFactory,
    ):
        table_name = TABLE_NAME_MAPPING["sqlite"][asset_name]
        # create table
        table_factory(
            engine=sqlite_ds.get_engine(),
            table_names={table_name},
        )

        table_names: list[str] = inspect(sqlite_ds.get_engine()).get_table_names()
        print(f"sqlite tables:\n{pf(table_names)}))")

        sqlite_ds.add_table_asset(asset_name, table_name=table_name)

    @pytest.mark.parametrize(
        "datasource_type,schema",
        [
            param("trino", None, marks=[pytest.mark.trino]),
            param("postgres", None, marks=[pytest.mark.postgresql]),
            param(
                "snowflake", get_random_identifier_name(), marks=[pytest.mark.snowflake]
            ),
            param("databricks_sql", PYTHON_VERSION, marks=[pytest.mark.databricks]),
            param("sqlite", None, marks=[pytest.mark.sqlite]),
        ],
    )
    def test_checkpoint_run(
        self,
        request: pytest.FixtureRequest,
        context: EphemeralDataContext,
        table_factory: TableFactory,
        asset_name: TableNameCase,
        datasource_type: DatabaseType,
        schema: str | None,
    ):
        datasource: SQLDatasource = request.getfixturevalue(f"{datasource_type}_ds")

        table_name: str | None = TABLE_NAME_MAPPING[datasource_type].get(asset_name)
        if not table_name:
            pytest.skip(f"no '{asset_name}' table_name for {datasource_type}")

        # create table
        table_factory(
            engine=datasource.get_engine(), table_names={table_name}, schema=schema
        )

        asset = datasource.add_table_asset(
            asset_name, table_name=table_name, schema_name=schema
        )

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


@pytest.mark.parametrize(
    "column_name",
    [
        param("lower", id="str lower"),
        param(
            "LOWER",
            marks=[pytest.mark.xfail(reason="may pass but not expected to")],
            id="str LOWER",
        ),
        param("'lower'", id="str 'lower'"),
        param('"lower"', id='str "lower"'),
        param(
            quoted_name(
                "lower",
                quote=None,
            ),
            id="quoted_name lower quote=None",
        ),
        param(
            quoted_name(
                "lower",
                quote=True,
            ),
            id="quoted_name lower quote=True",
        ),
        param(
            quoted_name(
                "lower",
                quote=False,
            ),
            id="quoted_name lower quote=False",
        ),
        param(
            quoted_name(
                "LOWER",
                quote=None,
            ),
            marks=[pytest.mark.xfail],
            id="quoted_name LOWER quote=None",
        ),
        param(
            "upper",
            marks=[pytest.mark.xfail(reason="may pass but not expected to")],
            id="str upper",
        ),
        param("UPPER", id="str UPPER"),
        param("'UPPER'", id="str 'UPPER'"),
        param('"UPPER"', id='str "UPPER"'),
        param(
            quoted_name(
                "UPPER",
                quote=None,
            ),
            id="quoted_name UPPER quote=None",
        ),
        param(
            quoted_name(
                "UPPER",
                quote=True,
            ),
            id="quoted_name UPPER quote=True",
        ),
        param(
            quoted_name(
                "UPPER",
                quote=False,
            ),
            id="quoted_name UPPER quote=False",
        ),
        param(
            quoted_name(
                "upper",
                quote=None,
            ),
            marks=[pytest.mark.xfail(reason="may pass but not expected to")],
            id="quoted_name upper quote=None",
        ),
    ],
)
class TestColumnIdentifiers:
    @pytest.mark.parametrize(
        "expectation_type",
        [
            "expect_column_values_to_not_be_null",
            "expect_column_to_exist",
        ],
    )
    def test_column_expectation(
        self,
        context: EphemeralDataContext,
        all_sql_datasources: SQLDatasource,
        table_factory: TableFactory,
        column_name: str | quoted_name,
        expectation_type: str,
    ):
        datasource = all_sql_datasources

        table_factory(
            engine=datasource.get_engine(),
            table_names={TEST_TABLE_NAME},
            data=[
                {
                    "id": 1,
                    "name": "first",
                    "UPPER": "my column is uppercase",
                    "lower": "my column is lowercase",
                },
                {
                    "id": 2,
                    "name": "second",
                    "UPPER": "my column is uppercase",
                    "lower": "my column is lowercase",
                },
            ],
        )

        # examine columns
        with datasource.get_engine().connect() as conn:
            result = conn.execute(TextClause(f"SELECT * FROM {TEST_TABLE_NAME}"))
            assert result
            print(f"{TEST_TABLE_NAME} Columns:\n  {result.keys()}\n")

        asset = datasource.add_table_asset("my_asset", table_name=TEST_TABLE_NAME)

        suite = context.add_expectation_suite(
            expectation_suite_name=f"{datasource.name}-{asset.name}"
        )
        suite.add_expectation(
            expectation_configuration=ExpectationConfiguration(
                expectation_type=expectation_type,
                kwargs={
                    "column": column_name,
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

        validation_results: list[
            dict[
                Literal[
                    "exception_info", "expectation_config", "meta", "result", "success"
                ],
                dict,
            ]
        ] = next(iter(result.to_json_dict()["run_results"].values()))[
            "validation_result"
        ][
            "results"
        ]
        print(f"validation_result.results:\n{pf(validation_results, depth=4)}")
        assert validation_results[-1]["exception_info"]["raised_exception"] is False
        assert validation_results[-1]["success"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
