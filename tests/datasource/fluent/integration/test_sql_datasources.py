from __future__ import annotations

import logging
import os
import pathlib
import shutil
import sys
import uuid
import warnings
from pprint import pformat as pf
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Generator,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    TypedDict,
)

import pytest
from packaging.version import Version
from pytest import param

import great_expectations.expectations.core as gxe
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.compatibility.sqlalchemy import (
    DatabaseError as SqlAlchemyDatabaseError,
)
from great_expectations.compatibility.sqlalchemy import (
    OperationalError as SqlAlchemyOperationalError,
)
from great_expectations.compatibility.sqlalchemy import (
    ProgrammingError as SqlAlchemyProgrammingError,
)
from great_expectations.compatibility.sqlalchemy import (
    TextClause,
    engine,
    inspect,
    quoted_name,
)
from great_expectations.compatibility.sqlalchemy import (
    __version__ as sqlalchemy_version,
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context import EphemeralDataContext
from great_expectations.datasource.fluent import (
    DatabricksSQLDatasource,
    PostgresDatasource,
    SnowflakeDatasource,
    SQLDatasource,
    SqliteDatasource,
)
from great_expectations.execution_engine.sqlalchemy_dialect import (
    DIALECT_IDENTIFIER_QUOTE_STRINGS,
    GXSqlDialect,
    quote_str,
)
from great_expectations.expectations.expectation_configuration import ExpectationConfiguration

if TYPE_CHECKING:
    from _pytest.mark.structures import ParameterSet
    from typing_extensions import TypeAlias

    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

TERMINAL_WIDTH: Final = shutil.get_terminal_size().columns
STAR_SEPARATOR: Final = "*" * TERMINAL_WIDTH

PYTHON_VERSION: Final[Literal["py38", "py39", "py310", "py311"]] = (
    f"py{sys.version_info.major}{sys.version_info.minor}"  # type: ignore[assignment] # str for each python version
)
SQLA_VERSION: Final = Version(sqlalchemy_version or "0.0.0")
LOGGER: Final = logging.getLogger("tests")

TEST_TABLE_NAME: Final[str] = "test_table"
# trino container ships with default test tables
TRINO_TABLE: Final[str] = "customer"

# NOTE: can we create tables in trino?
# some of the trino tests probably don't make sense if we can't create tables
DO_NOT_CREATE_TABLES: set[str] = {"trino"}
# sqlite db files should be using fresh tmp_path on every test
DO_NOT_DROP_TABLES: set[str] = {"sqlite"}

DatabaseType: TypeAlias = Literal["databricks_sql", "postgres", "snowflake", "sqlite", "trino"]
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
TABLE_NAME_MAPPING: Final[Mapping[DatabaseType, Mapping[TableNameCase, str]]] = {
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

# column names
UNQUOTED_UPPER_COL: Final[Literal["UNQUOTED_UPPER_COL"]] = "UNQUOTED_UPPER_COL"
UNQUOTED_LOWER_COL: Final[Literal["unquoted_lower_col"]] = "unquoted_lower_col"
QUOTED_UPPER_COL: Final[Literal["QUOTED_UPPER_COL"]] = "QUOTED_UPPER_COL"
QUOTED_LOWER_COL: Final[Literal["quoted_lower_col"]] = "quoted_lower_col"
QUOTED_MIXED_CASE: Final[Literal["quotedMixed"]] = "quotedMixed"
QUOTED_W_DOTS: Final[Literal["quoted.w.dots"]] = "quoted.w.dots"


def get_random_identifier_name() -> str:
    guid = uuid.uuid4()
    return f"i{guid.hex}"


RAND_SCHEMA: Final[str] = f"{PYTHON_VERSION}_{get_random_identifier_name()}"


def _get_exception_details(
    result: CheckpointResult,
    prettyprint: bool = False,
) -> list[
    dict[
        Literal["exception_message", "exception_traceback", "raised_exception"],
        str,
    ]
]:
    """Extract a list of exception_info dicts from a CheckpointResult."""
    first_run_result = next(iter(result.run_results.values()))
    validation_results = first_run_result.results
    if prettyprint:
        print(f"validation_result.results:\n{pf(validation_results, depth=2)}\n")

    exc_details = [
        r["exception_info"]
        for r in validation_results
        if r["exception_info"].get("raised_exception")
    ]
    if exc_details and prettyprint:
        print(f"{len(exc_details)} exception_info(s):\n{STAR_SEPARATOR}")
        for i, exc_info in enumerate(exc_details, start=1):
            print(
                f"  {i}: {exc_info['exception_message']}"
                f"\n\n{exc_info['exception_traceback']}\n{STAR_SEPARATOR}"
            )
    return exc_details


@pytest.fixture
def capture_engine_logs(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """Capture SQLAlchemy engine logs and display them if the test fails."""
    caplog.set_level(logging.INFO, logger="sqlalchemy.engine")
    return caplog


class Row(TypedDict):
    id: int
    name: str
    quoted_upper_col: str
    quoted_lower_col: str
    quoted_mixed_case: str
    quoted_w_dots: str
    unquoted_upper_col: str
    unquoted_lower_col: str


ColNameParams: TypeAlias = Literal[
    # DDL: unquoted_lower_col ------
    "unquoted_lower_col",
    '"unquoted_lower_col"',
    "UNQUOTED_LOWER_COL",
    '"UNQUOTED_LOWER_COL"',
    # DDL: UNQUOTED_UPPER_COL ------
    "unquoted_upper_col",
    '"unquoted_upper_col"',
    "UNQUOTED_UPPER_COL",
    '"UNQUOTED_UPPER_COL"',
    # DDL: "quoted_lower_col" -----
    "quoted_lower_col",
    '"quoted_lower_col"',
    "QUOTED_LOWER_COL",
    '"QUOTED_LOWER_COL"',
    # DDL: "QUOTED_UPPER_COL" ----
    "quoted_upper_col",
    '"quoted_upper_col"',
    "QUOTED_UPPER_COL",
    '"QUOTED_UPPER_COL"',
    # DDL: "quotedMixed" -----
    "quotedmixed",
    "quotedMixed",
    '"quotedMixed"',
    "QUOTEDMIXED",
    # DDL: "quoted.w.dots" -------
    "quoted.w.dots",
    '"quoted.w.dots"',
    "QUOTED.W.DOTS",
    '"QUOTED.W.DOTS"',
]

ColNameParamId: TypeAlias = Literal[
    # DDL: unquoted_lower_col ------
    "str unquoted_lower_col",
    'str "unquoted_lower_col"',
    "str UNQUOTED_LOWER_COL",
    'str "UNQUOTED_LOWER_COL"',
    # DDL: UNQUOTED_UPPER_COL ------
    "str unquoted_upper_col",
    'str "unquoted_upper_col"',
    "str UNQUOTED_UPPER_COL",
    'str "UNQUOTED_UPPER_COL"',
    # DDL: "quoted_lower_col" -----
    "str quoted_lower_col",
    'str "quoted_lower_col"',
    "str QUOTED_LOWER_COL",
    'str "QUOTED_LOWER_COL"',
    # DDl: "QUOTED_UPPER_COL" ----
    "str quoted_upper_col",
    'str "quoted_upper_col"',
    "str QUOTED_UPPER_COL",
    'str "QUOTED_UPPER_COL"',
    # DDL: "quotedMixed" -----
    "str quotedmixed",
    "str quotedMixed",
    'str "quotedMixed"',
    "str QUOTEDMIXED",
    # DDL: "quoted.w.dots" -------
    "str quoted.w.dots",
    'str "quoted.w.dots"',
    "str QUOTED.W.DOTS",
    'str "QUOTED.W.DOTS"',
]

COLUMN_DDL: Final[Mapping[ColNameParams, str]] = {
    # DDL: unquoted_lower_col ------
    "unquoted_lower_col": UNQUOTED_LOWER_COL,
    '"unquoted_lower_col"': UNQUOTED_LOWER_COL,
    "UNQUOTED_LOWER_COL": UNQUOTED_LOWER_COL,
    '"UNQUOTED_LOWER_COL"': UNQUOTED_LOWER_COL,
    # DDL: UNQUOTED_UPPER_COL ------
    "unquoted_upper_col": UNQUOTED_UPPER_COL,
    '"unquoted_upper_col"': UNQUOTED_UPPER_COL,
    "UNQUOTED_UPPER_COL": UNQUOTED_UPPER_COL,
    '"UNQUOTED_UPPER_COL"': UNQUOTED_UPPER_COL,
    # DDL: "quoted_lower_col" -----
    "quoted_lower_col": f'"{QUOTED_LOWER_COL}"',
    '"quoted_lower_col"': f'"{QUOTED_LOWER_COL}"',
    "QUOTED_LOWER_COL": f'"{QUOTED_LOWER_COL}"',
    '"QUOTED_LOWER_COL"': f'"{QUOTED_LOWER_COL}"',
    # DDl: "QUOTED_UPPER_COL" ----
    "quoted_upper_col": f'"{QUOTED_UPPER_COL}"',
    '"quoted_upper_col"': f'"{QUOTED_UPPER_COL}"',
    "QUOTED_UPPER_COL": f'"{QUOTED_UPPER_COL}"',
    '"QUOTED_UPPER_COL"': f'"{QUOTED_UPPER_COL}"',
    # DDL: "quotedMixed" -----
    "quotedmixed": f"{QUOTED_MIXED_CASE.lower()}",
    "quotedMixed": f"{QUOTED_MIXED_CASE}",
    '"quotedMixed"': f'"{QUOTED_MIXED_CASE}"',
    "QUOTEDMIXED": f"{QUOTED_MIXED_CASE.upper()}",
    # DDL: "quoted.w.dots" -------
    "quoted.w.dots": f'"{QUOTED_W_DOTS}"',
    '"quoted.w.dots"': f'"{QUOTED_W_DOTS}"',
    "QUOTED.W.DOTS": f'"{QUOTED_W_DOTS}"',
    '"QUOTED.W.DOTS"': f'"{QUOTED_W_DOTS}"',
}


# TODO: remove items from this lookup when working on fixes
# The presence of a database type in the list indicates that this specific value fails when
# used as the `column_name` value for at least one expectation.
# It does not mean that it SHOULD or SHOULD NOT fail, but that it currently does.
FAILS_EXPECTATION: Final[Mapping[ColNameParamId, list[DatabaseType]]] = {
    # DDL: unquoted_lower_col ------
    "str unquoted_lower_col": [],
    'str "unquoted_lower_col"': ["postgres", "snowflake", "sqlite"],
    "str UNQUOTED_LOWER_COL": ["databricks_sql", "postgres", "sqlite"],
    'str "UNQUOTED_LOWER_COL"': ["snowflake", "sqlite"],
    # DDL: UNQUOTED_UPPER_COL ------
    "str unquoted_upper_col": ["databricks_sql", "sqlite"],
    'str "unquoted_upper_col"': ["postgres", "sqlite"],
    "str UNQUOTED_UPPER_COL": ["postgres"],
    'str "UNQUOTED_UPPER_COL"': ["postgres", "snowflake", "sqlite"],
    # DDL: "quoted_lower_col" -----
    'str "quoted_lower_col"': ["postgres", "snowflake", "sqlite"],
    "str quoted_lower_col": ["snowflake"],
    "str QUOTED_LOWER_COL": ["databricks_sql", "postgres", "snowflake", "sqlite"],
    'str "QUOTED_LOWER_COL"': ["sqlite"],
    # DDl: "QUOTED_UPPER_COL" ----
    "str quoted_upper_col": ["databricks_sql", "sqlite", "postgres"],
    'str "quoted_upper_col"': ["sqlite"],
    "str QUOTED_UPPER_COL": [],
    'str "QUOTED_UPPER_COL"': ["postgres", "snowflake", "sqlite"],
    # DDL: "quotedMixed" -----
    "str quotedmixed": ["databricks_sql", "postgres", "sqlite", "snowflake"],
    "str quotedMixed": ["snowflake"],
    'str "quotedMixed"': ["databricks_sql", "postgres", "sqlite", "snowflake"],
    "str QUOTEDMIXED": ["databricks_sql", "postgres", "sqlite", "snowflake"],
    # DDL: "quoted.w.dots" -------
    "str quoted.w.dots": ["databricks_sql"],
    'str "quoted.w.dots"': ["databricks_sql", "postgres", "snowflake", "sqlite"],
    "str QUOTED.W.DOTS": ["databricks_sql", "snowflake", "sqlite", "postgres"],
    'str "QUOTED.W.DOTS"': ["sqlite"],
}


class TableFactory(Protocol):
    def __call__(
        self,
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema: str | None = None,
        data: Sequence[Row] = ...,
    ) -> None: ...


@pytest.fixture(
    scope="class",
)
def table_factory() -> Generator[TableFactory, None, None]:  # noqa: C901
    """
    Class scoped.
    Given a SQLALchemy engine, table_name and schema,
    create the table if it does not exist and drop it after the test class.
    """
    all_created_tables: dict[str, list[dict[Literal["table_name", "schema"], str | None]]] = {}
    engines: dict[str, engine.Engine] = {}

    def _table_factory(
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema: str | None = None,
        data: Sequence[Row] = tuple(),
    ) -> None:
        sa_engine = gx_engine.engine
        if sa_engine.dialect.name in DO_NOT_CREATE_TABLES:
            LOGGER.info(f"Skipping table creation for {table_names} for {sa_engine.dialect.name}")
            return
        LOGGER.info(
            f"SQLA:{SQLA_VERSION} - Creating `{sa_engine.dialect.name}` table for {table_names} if it does not exist"  # noqa: E501
        )
        dialect = GXSqlDialect(sa_engine.dialect.name)
        created_tables: list[dict[Literal["table_name", "schema"], str | None]] = []

        with gx_engine.get_connection() as conn:
            quoted_upper_col: str = quote_str(QUOTED_UPPER_COL, dialect=dialect)
            quoted_lower_col: str = quote_str(QUOTED_LOWER_COL, dialect=dialect)
            quoted_w_dots: str = quote_str(QUOTED_W_DOTS, dialect=dialect)
            quoted_mixed_case: str = quote_str(QUOTED_MIXED_CASE, dialect=dialect)
            transaction = conn.begin()
            if schema:
                conn.execute(TextClause(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            for name in table_names:
                qualified_table_name = f"{schema}.{name}" if schema else name
                # TODO: use dialect specific quotes
                create_tables: str = (
                    f"CREATE TABLE IF NOT EXISTS {qualified_table_name}"
                    " (id INTEGER, name VARCHAR(255),"
                    f" {quoted_upper_col} VARCHAR(255), {quoted_lower_col} VARCHAR(255),"
                    f" {UNQUOTED_UPPER_COL} VARCHAR(255), {UNQUOTED_LOWER_COL} VARCHAR(255),"
                    f" {quoted_mixed_case} VARCHAR(255), {quoted_w_dots} VARCHAR(255))"
                )
                conn.execute(TextClause(create_tables))
                if data:
                    insert_data = (
                        f"INSERT INTO {qualified_table_name}"
                        f" (id, name, {quoted_upper_col}, {quoted_lower_col},"
                        f" {UNQUOTED_UPPER_COL}, {UNQUOTED_LOWER_COL},"
                        f" {quoted_mixed_case}, {quoted_w_dots})"
                        " VALUES (:id, :name, :quoted_upper_col, :quoted_lower_col,"
                        " :unquoted_upper_col, :unquoted_lower_col,"
                        " :quoted_mixed_case, :quoted_w_dots)"
                    )
                    conn.execute(TextClause(insert_data), data)

                created_tables.append(dict(table_name=name, schema=schema))
            transaction.commit()
        all_created_tables[sa_engine.dialect.name] = created_tables
        engines[sa_engine.dialect.name] = sa_engine

    yield _table_factory

    # teardown
    print(f"dropping tables\n{pf(all_created_tables)}")
    for dialect, tables in all_created_tables.items():
        if dialect in DO_NOT_DROP_TABLES:
            print(f"skipping drop for {dialect}")
            continue
        engine = engines[dialect]
        with engine.connect() as conn:
            transaction = conn.begin()
            schema: str | None = None
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
    ds = context.data_sources.add_sql(
        "trino",
        connection_string="trino://user:@localhost:8088/tpch/sf1",
    )
    return ds


@pytest.fixture
def postgres_ds(context: EphemeralDataContext) -> PostgresDatasource:
    ds = context.data_sources.add_postgres(
        "postgres",
        connection_string="postgresql+psycopg2://postgres:postgres@localhost:5432/test_ci",
    )
    return ds


@pytest.fixture
def databricks_creds_populated() -> bool:
    return bool(
        os.getenv("DATABRICKS_TOKEN")
        or os.getenv("DATABRICKS_HOST")
        or os.getenv("DATABRICKS_HTTP_PATH")
    )


@pytest.fixture
def databricks_sql_ds(
    context: EphemeralDataContext, databricks_creds_populated: bool
) -> DatabricksSQLDatasource:
    if not databricks_creds_populated:
        pytest.skip("no databricks credentials")
    ds = context.data_sources.add_databricks_sql(
        "databricks_sql",
        connection_string="databricks://token:"
        "${DATABRICKS_TOKEN}@${DATABRICKS_HOST}:443"
        "?http_path=${DATABRICKS_HTTP_PATH}&catalog=ci&schema=" + RAND_SCHEMA,
    )
    return ds


@pytest.fixture
def snowflake_creds_populated() -> bool:
    return bool(os.getenv("SNOWFLAKE_CI_USER_PASSWORD") or os.getenv("SNOWFLAKE_CI_ACCOUNT"))


@pytest.fixture
def snowflake_ds(
    context: EphemeralDataContext,
    snowflake_creds_populated: bool,
) -> SnowflakeDatasource:
    if not snowflake_creds_populated:
        pytest.skip("no snowflake credentials")
    ds = context.data_sources.add_snowflake(
        "snowflake",
        connection_string="snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@oca29081.us-east-1/ci"
        f"/{RAND_SCHEMA}?warehouse=ci&role=ci",
        # NOTE: uncomment this and set SNOWFLAKE_USER to run tests against your own snowflake account  # noqa: E501
        # connection_string="snowflake://${SNOWFLAKE_USER}@oca29081.us-east-1/DEMO_DB/RESTAURANTS?warehouse=COMPUTE_WH&role=PUBLIC&authenticator=externalbrowser",
    )
    return ds


@pytest.fixture
def sqlite_ds(context: EphemeralDataContext, tmp_path: pathlib.Path) -> SqliteDatasource:
    ds = context.data_sources.add_sqlite(
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
    capture_engine_logs: pytest.LogCaptureFixture,
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
        table_factory(gx_engine=postgres_ds.get_execution_engine(), table_names={table_name})

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
            gx_engine=databricks_sql_ds.get_execution_engine(),
            table_names={table_name},
            schema=RAND_SCHEMA,
        )

        table_names: list[str] = inspect(databricks_sql_ds.get_engine()).get_table_names(
            schema=RAND_SCHEMA
        )
        print(f"databricks tables:\n{pf(table_names)}))")

        databricks_sql_ds.add_table_asset(
            asset_name, table_name=table_name, schema_name=RAND_SCHEMA
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
        schema = RAND_SCHEMA
        table_factory(
            gx_engine=snowflake_ds.get_execution_engine(),
            table_names={table_name},
            schema=schema,
        )

        table_names: list[str] = inspect(snowflake_ds.get_engine()).get_table_names(schema=schema)
        print(f"snowflake tables:\n{pf(table_names)}))")

        snowflake_ds.add_table_asset(asset_name, table_name=table_name)

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
            gx_engine=sqlite_ds.get_execution_engine(),
            table_names={table_name},
        )

        table_names: list[str] = inspect(sqlite_ds.get_engine()).get_table_names()
        print(f"sqlite tables:\n{pf(table_names)}))")

        sqlite_ds.add_table_asset(asset_name, table_name=table_name)

    @pytest.mark.filterwarnings(  # snowflake `add_table_asset` raises warning on passing a schema
        "once::great_expectations.datasource.fluent.GxDatasourceWarning"
    )
    @pytest.mark.parametrize(
        "datasource_type,schema",
        [
            param("trino", None, marks=[pytest.mark.trino]),
            param("postgres", None, marks=[pytest.mark.postgresql]),
            param("snowflake", RAND_SCHEMA, marks=[pytest.mark.snowflake]),
            param(
                "databricks_sql",
                RAND_SCHEMA,
                marks=[pytest.mark.databricks],
            ),
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
            gx_engine=datasource.get_execution_engine(),
            table_names={table_name},
            schema=schema,
        )

        with warnings.catch_warnings():
            # passing a schema to snowflake tables is deprecated
            warnings.simplefilter("once", DeprecationWarning)
            asset = datasource.add_table_asset(
                asset_name, table_name=table_name, schema_name=schema
            )
            batch_definition = asset.add_batch_definition_whole_table("whole table!")

        suite = context.suites.add(ExpectationSuite(name=f"{datasource.name}-{asset.name}"))
        suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="name"))

        validation_definition = context.validation_definitions.add(
            ValidationDefinition(name="validation_definition", suite=suite, data=batch_definition)
        )
        checkpoint = context.checkpoints.add(
            Checkpoint(
                name=f"{datasource.name}-{asset.name}",
                validation_definitions=[validation_definition],
            )
        )
        result = checkpoint.run()

        _get_exception_details(result, prettyprint=True)
        assert result.success is True


def _fails_expectation(param_id: str) -> bool:
    """
    Lookup whether a given column_name fails an expectation for a given dialect.
    This does not mean that it SHOULD fail, but that it currently does.
    """
    column_name: ColNameParamId
    dialect, column_name, *_ = param_id.split("-")  # type: ignore[assignment]
    dialects_need_fixes: list[DatabaseType] = FAILS_EXPECTATION.get(column_name, [])
    return dialect in dialects_need_fixes


def _is_quote_char_dialect_mismatch(
    dialect: GXSqlDialect,
    column_name: str | quoted_name,
) -> bool:
    quote_char = column_name[0] if column_name[0] in ("'", '"', "`") else None
    if quote_char:
        dialect_quote_char = DIALECT_IDENTIFIER_QUOTE_STRINGS[dialect]
        if quote_char != dialect_quote_char:
            return True
    return False


def _raw_query_check_column_exists(
    column_name_param: str,
    qualified_table_name: str,
    gx_execution_engine: SqlAlchemyExecutionEngine,
) -> bool:
    """Use a simple 'SELECT {column_name_param} from {qualified_table_name};' query to check if the column exists.'"""  # noqa: E501
    with gx_execution_engine.get_connection() as connection:
        query = f"""SELECT {column_name_param} FROM {qualified_table_name} LIMIT 1;"""
        print(f"query:\n  {query}")
        # an exception will be raised if the column does not exist
        try:
            col_exist_check = connection.execute(
                TextClause(query),
            )
            print(f"\nResults:\n  {col_exist_check.keys()}")
            col_exist_result = col_exist_check.fetchone()
            print(f"  Values: {col_exist_result}\n{column_name_param} exists!\n")
        except (
            SqlAlchemyProgrammingError,
            SqlAlchemyOperationalError,
            SqlAlchemyDatabaseError,
        ) as sql_err:
            LOGGER.debug("_raw_query_check_column_exists - SQLAlchemy Error", exc_info=sql_err)
            print(f"\n{column_name_param} does not exist! - {sql_err.__class__.__name__}\n")
            return False
        return True


_EXPECTATION_TYPES: Final[tuple[ParameterSet, ...]] = (
    param("expect_column_to_exist", {}, id="expect_column_to_exist"),
    param("expect_column_values_to_not_be_null", {}, id="expect_column_values_to_not_be_null"),
    param(
        "expect_column_values_to_match_regex",
        {"regex": r".*"},
        id="expect_column_values_to_match_regex",
    ),
    param(
        "expect_column_values_to_match_like_pattern",
        {"like_pattern": r"%"},
        id="expect_column_values_to_match_like_pattern",
    ),
)


@pytest.mark.filterwarnings(
    "once::DeprecationWarning"
)  # snowflake `add_table_asset` raises warning on passing a schema
@pytest.mark.parametrize("expectation_type, extra_exp_kwargs", _EXPECTATION_TYPES)
class TestColumnExpectations:
    @pytest.mark.parametrize(
        "column_name",
        [
            # DDL: unquoted_lower_col ----------------------------------
            param("unquoted_lower_col", id="str unquoted_lower_col"),
            param("UNQUOTED_LOWER_COL", id="str UNQUOTED_LOWER_COL"),
            # DDL: UNQUOTED_UPPER_COL ----------------------------------
            param("unquoted_upper_col", id="str unquoted_upper_col"),
            param("UNQUOTED_UPPER_COL", id="str UNQUOTED_UPPER_COL"),
            # DDL: "quoted_lower_col"-----------------------------------
            param("quoted_lower_col", id="str quoted_lower_col"),
            param("QUOTED_LOWER_COL", id="str QUOTED_LOWER_COL"),
            # DDL: "QUOTED_UPPER_COL" ----------------------------------
            param("quoted_upper_col", id="str quoted_upper_col"),
            param("QUOTED_UPPER_COL", id="str QUOTED_UPPER_COL"),
            # DDL: "quotedMixed" -------------------------------------
            param("quotedmixed", id="str quotedmixed"),
            param("quotedMixed", id="str quotedMixed"),
            param("QUOTEDMIXED", id="str QUOTEDMIXED"),
            # DDL: "quoted.w.dots" -------------------------------------
            param("quoted.w.dots", id="str quoted.w.dots"),
            param("QUOTED.W.DOTS", id="str QUOTED.W.DOTS"),
        ],
    )
    def test_unquoted_params(
        self,
        context: EphemeralDataContext,
        all_sql_datasources: SQLDatasource,
        table_factory: TableFactory,
        column_name: str | quoted_name,
        expectation_type: str,
        extra_exp_kwargs: dict[str, Any],
        request: pytest.FixtureRequest,
    ):
        """
        Test column expectations when using unquoted column name parameters
        (actual column may have DDL with quotes).

        Test fails if the expectation fails regardless of dialect.
        """
        param_id = request.node.callspec.id
        datasource = all_sql_datasources
        dialect = datasource.get_engine().dialect.name

        if column_name[0] in ("'", '"', "`"):
            pytest.skip(f"see _desired_state tests for {column_name!r}")
        elif _fails_expectation(param_id):
            # apply marker this way so that xpasses can be seen in the report
            request.applymarker(pytest.mark.xfail)

        print(f"expectations_type:\n  {expectation_type}")

        schema: str | None = (
            RAND_SCHEMA
            if GXSqlDialect(dialect) in (GXSqlDialect.SNOWFLAKE, GXSqlDialect.DATABRICKS)
            else None
        )

        print(f"\ncolumn DDL:\n  {COLUMN_DDL[column_name]}")  # type: ignore[index] # FIXME
        print(f"\n`column_name` parameter __repr__:\n  {column_name!r}")
        print(f"type:\n  {type(column_name)}\n")

        table_factory(
            gx_engine=datasource.get_execution_engine(),
            table_names={TEST_TABLE_NAME},
            schema=schema,
            data=[
                {
                    "id": 1,
                    "name": param_id,
                    "quoted_upper_col": "my column is uppercase",
                    "quoted_lower_col": "my column is lowercase",
                    "unquoted_upper_col": "whatever",
                    "unquoted_lower_col": "whatever",
                    "quoted_mixed_case": "Whatever",
                    "quoted_w_dots": "what.ever",
                },
            ],
        )

        asset = datasource.add_table_asset(
            "my_asset", table_name=TEST_TABLE_NAME, schema_name=schema
        )
        print(f"asset:\n{asset!r}\n")

        suite = context.suites.add(ExpectationSuite(name=f"{datasource.name}-{asset.name}"))
        suite.add_expectation_configuration(
            expectation_configuration=ExpectationConfiguration(
                type=expectation_type, kwargs={"column": column_name, **extra_exp_kwargs}
            )
        )
        suite.save()

        batch_definition = asset.add_batch_definition_whole_table("my_batch_def")
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(name="my_validation_def", suite=suite, data=batch_definition)
        )

        checkpoint = context.checkpoints.add(
            checkpoint=Checkpoint(
                name=f"{datasource.name}-{asset.name}_checkpoint",
                validation_definitions=[validation_definition],
            )
        )
        result = checkpoint.run()

        _ = _get_exception_details(result, prettyprint=True)

        assert result.success is True, "validation failed"

    @pytest.mark.parametrize(
        "column_name",
        [
            # DDL: unquoted_lower_col ----------------------------------
            param('"unquoted_lower_col"', id='str "unquoted_lower_col"'),
            # DDL: UNQUOTED_UPPER_COL ----------------------------------
            param('"UNQUOTED_UPPER_COL"', id='str "UNQUOTED_UPPER_COL"'),
            # DDL: "quoted_lower_col"-----------------------------------
            param('"quoted_lower_col"', id='str "quoted_lower_col"'),
            # DDL: "QUOTED_UPPER_COL" ----------------------------------
            param('"QUOTED_UPPER_COL"', id='str "QUOTED_UPPER_COL"'),
            # DDL: "quotedMixed" -------------------------------------
            param('"quotedMixed"', id='str "quotedMixed"'),
            # DDL: "quoted.w.dots" -------------------------------------
            param('"quoted.w.dots"', id='str "quoted.w.dots"'),
        ],
    )
    def test_quoted_params(
        self,
        context: EphemeralDataContext,
        all_sql_datasources: SQLDatasource,
        table_factory: TableFactory,
        column_name: str | quoted_name,
        expectation_type: str,
        extra_exp_kwargs: dict[str, Any],
        request: pytest.FixtureRequest,
    ):
        """
        Test column expectations when using quoted column name parameters
        (actual column may have DDL without quotes).

        Test fails if the expectation fails regardless of dialect.
        """
        param_id = request.node.callspec.id
        datasource = all_sql_datasources
        dialect = GXSqlDialect(datasource.get_engine().dialect.name)

        if column_name[0] not in ("'", '"', "`"):
            pytest.skip(f"see test_unquoted_params for {column_name!r}")
        elif _is_quote_char_dialect_mismatch(dialect, column_name):
            pytest.skip(f"quote char dialect mismatch: {column_name[0]}")
        elif _fails_expectation(param_id):
            # apply marker this way so that xpasses can be seen in the report
            request.applymarker(pytest.mark.xfail)

        print(f"expectations_type:\n  {expectation_type}")

        schema: str | None = (
            RAND_SCHEMA
            if GXSqlDialect(dialect) in (GXSqlDialect.SNOWFLAKE, GXSqlDialect.DATABRICKS)
            else None
        )

        print(f"\ncolumn DDL:\n  {COLUMN_DDL[column_name]}")  # type: ignore[index] # FIXME
        print(f"\n`column_name` parameter __repr__:\n  {column_name!r}")
        print(f"type:\n  {type(column_name)}\n")

        table_factory(
            gx_engine=datasource.get_execution_engine(),
            table_names={TEST_TABLE_NAME},
            schema=schema,
            data=[
                {
                    "id": 1,
                    "name": param_id,
                    "quoted_upper_col": "my column is uppercase",
                    "quoted_lower_col": "my column is lowercase",
                    "unquoted_upper_col": "whatever",
                    "unquoted_lower_col": "whatever",
                    "quoted_mixed_case": "Whatever",
                    "quoted_w_dots": "what.ever",
                },
            ],
        )

        asset = datasource.add_table_asset(
            "my_asset", table_name=TEST_TABLE_NAME, schema_name=schema
        )
        print(f"asset:\n{asset!r}\n")

        suite = context.suites.add(ExpectationSuite(name=f"{datasource.name}-{asset.name}"))
        suite.add_expectation_configuration(
            expectation_configuration=ExpectationConfiguration(
                type=expectation_type, kwargs={"column": column_name, **extra_exp_kwargs}
            )
        )
        suite.save()

        batch_definition = asset.add_batch_definition_whole_table("my_batch_def")
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(name="my_validation_def", suite=suite, data=batch_definition)
        )

        checkpoint = context.checkpoints.add(
            checkpoint=Checkpoint(
                name=f"{datasource.name}-{asset.name}_checkpoint",
                validation_definitions=[validation_definition],
            )
        )
        result = checkpoint.run()

        assert result.success is True, "validation failed"

    @pytest.mark.parametrize(
        "column_name",
        [
            # DDL: unquoted_lower_col ----------------------------------
            param("unquoted_lower_col", id="str unquoted_lower_col"),
            param('"unquoted_lower_col"', id='str "unquoted_lower_col"'),
            param("UNQUOTED_LOWER_COL", id="str UNQUOTED_LOWER_COL"),
            param('"UNQUOTED_LOWER_COL"', id='str "UNQUOTED_LOWER_COL"'),
            # DDL: UNQUOTED_UPPER_COL ----------------------------------
            param("unquoted_upper_col", id="str unquoted_upper_col"),
            param('"unquoted_upper_col"', id='str "unquoted_upper_col"'),
            param("UNQUOTED_UPPER_COL", id="str UNQUOTED_UPPER_COL"),
            param('"UNQUOTED_UPPER_COL"', id='str "UNQUOTED_UPPER_COL"'),
            # DDL: "quoted_lower_col"-----------------------------------
            param("quoted_lower_col", id="str quoted_lower_col"),
            param('"quoted_lower_col"', id='str "quoted_lower_col"'),
            param("QUOTED_LOWER_COL", id="str QUOTED_LOWER_COL"),
            param('"QUOTED_LOWER_COL"', id='str "QUOTED_LOWER_COL"'),
            # DDL: "QUOTED_UPPER_COL" ----------------------------------
            param("quoted_upper_col", id="str quoted_upper_col"),
            param('"quoted_upper_col"', id='str "quoted_upper_col"'),
            param("QUOTED_UPPER_COL", id="str QUOTED_UPPER_COL"),
            param('"QUOTED_UPPER_COL"', id='str "QUOTED_UPPER_COL"'),
            # DDL: "quotedMixed" ---------------------------------------
            param("quotedmixed", id="str quotedmixed"),
            param("quotedMixed", id="str quotedMixed"),
            param('"quotedMixed"', id='str "quotedMixed"'),
            param("QUOTEDMIXED", id="str QUOTEDMIXED"),
            # DDL: "quoted.w.dots" -------------------------------------
            param("quoted.w.dots", id="str quoted.w.dots"),
            param('"quoted.w.dots"', id='str "quoted.w.dots"'),  # TODO: fix me
            param("QUOTED.W.DOTS", id="str QUOTED.W.DOTS"),
            param('"QUOTED.W.DOTS"', id='str "QUOTED.W.DOTS"'),
        ],
    )
    def test_desired_state(
        self,
        context: EphemeralDataContext,
        all_sql_datasources: SQLDatasource,
        table_factory: TableFactory,
        column_name: str | quoted_name,
        expectation_type: str,
        extra_exp_kwargs: dict[str, Any],
        request: pytest.FixtureRequest,
    ):
        """
        Perform a raw query to check if the column exists before running the expectation.
        This is used to determine how the identifier behaves natively in the database and
        therefore determine if the expectation should pass or fail.

        An expectation is expected to succeed if the column 'exists' and fail if it does not.
        If we want GX to behave the same way as each dialect/database,
        these tests should be our guide.

        However currently, GX does not behave the same way as the databases in all cases.
        """
        param_id = request.node.callspec.id
        datasource = all_sql_datasources
        dialect = GXSqlDialect(datasource.get_engine().dialect.name)

        original_column_name = column_name
        if column_name.startswith('"') and column_name.endswith('"'):
            # databricks uses backticks for quoting
            column_name = quote_str(column_name[1:-1], dialect=dialect)

        print(f"expectations_type:\n  {expectation_type}")

        schema: str | None = (
            RAND_SCHEMA
            if GXSqlDialect(dialect) in (GXSqlDialect.SNOWFLAKE, GXSqlDialect.DATABRICKS)
            else None
        )

        print(f"\ncolumn DDL:\n  {COLUMN_DDL[original_column_name]}")  # type: ignore[index] # FIXME
        print(f"\n`column_name` parameter __repr__:\n  {column_name!r}")
        print(f"type:\n  {type(column_name)}\n")

        table_factory(
            gx_engine=datasource.get_execution_engine(),
            table_names={TEST_TABLE_NAME},
            schema=schema,
            data=[
                {
                    "id": 1,
                    "name": param_id,
                    "quoted_upper_col": "my column is uppercase",
                    "quoted_lower_col": "my column is lowercase",
                    "unquoted_upper_col": "whatever",
                    "unquoted_lower_col": "whatever",
                    "quoted_mixed_case": "Whatever",
                    "quoted_w_dots": "what.ever",
                },
            ],
        )

        qualified_table_name: str = f"{schema}.{TEST_TABLE_NAME}" if schema else TEST_TABLE_NAME
        # check the column exists so that we know what if the expectation should succeed or fail
        column_exists = _raw_query_check_column_exists(
            column_name,
            qualified_table_name,
            datasource.get_execution_engine(),
        )

        asset = datasource.add_table_asset(
            "my_asset", table_name=TEST_TABLE_NAME, schema_name=schema
        )
        print(f"asset:\n{asset!r}\n")

        suite = context.suites.add(ExpectationSuite(name=f"{datasource.name}-{asset.name}"))
        suite.add_expectation_configuration(
            expectation_configuration=ExpectationConfiguration(
                type=expectation_type, kwargs={"column": column_name, **extra_exp_kwargs}
            )
        )
        suite.save()

        batch_definition = asset.add_batch_definition_whole_table("my_batch_def")
        validation_definition = context.validation_definitions.add(
            ValidationDefinition(name="my_validation_def", suite=suite, data=batch_definition)
        )

        checkpoint = context.checkpoints.add(
            checkpoint=Checkpoint(
                name=f"{datasource.name}-{asset.name}_checkpoint",
                validation_definitions=[validation_definition],
            )
        )
        result = checkpoint.run()

        try:
            if column_exists:
                assert result.success is True, "column exists but validation failed"
            else:
                assert result.success is False, "column does not exist but validation succeeded"
        except AssertionError as ae:
            reason = str(ae).splitlines()[0]
            print(reason)
            # xfail if the expectation doesn't behave as the dialect would
            pytest.xfail(reason=reason)


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "-rXf"])
