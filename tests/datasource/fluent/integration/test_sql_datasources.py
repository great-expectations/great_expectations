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
from great_expectations.expectations.expectation import (
    ExpectationConfiguration,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.checkpoint.checkpoint import CheckpointResult
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

TERMINAL_WIDTH: Final = shutil.get_terminal_size().columns
STAR_SEPARATOR: Final = "*" * TERMINAL_WIDTH

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
# sqlite db files should be using fresh tmp_path on every test
DO_NOT_DROP_TABLES: set[str] = {"sqlite"}

DatabaseType: TypeAlias = Literal[
    "databricks_sql", "postgres", "snowflake", "sqlite", "trino"
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

# column names
UNQUOTED_UPPER_COL: Final[Literal["UNQUOTED_UPPER_COL"]] = "UNQUOTED_UPPER_COL"
UNQUOTED_LOWER_COL: Final[Literal["unquoted_lower_col"]] = "unquoted_lower_col"
QUOTED_UPPER_COL: Final[Literal["QUOTED_UPPER_COL"]] = "QUOTED_UPPER_COL"
QUOTED_LOWER_COL: Final[Literal["quoted_lower_col"]] = "quoted_lower_col"


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
    validation_results: list[
        dict[
            Literal[
                "exception_info", "expectation_config", "meta", "result", "success"
            ],
            dict,
        ]
    ] = next(  # type: ignore[index, assignment]
        iter(result.to_json_dict()["run_results"].values())  # type: ignore[call-overload,union-attr]
    )[
        "validation_result"  # type: ignore[index]
    ][
        "results"  # type: ignore[index]
    ]
    if prettyprint:
        print(f"validation_result.results:\n{pf(validation_results, depth=2)}\n")

    exc_details = [
        r["exception_info"]
        for r in validation_results
        if r["exception_info"]["raised_exception"]
    ]
    if exc_details and prettyprint:
        print(f"{len(exc_details)} exception_info(s):\n{STAR_SEPARATOR}")
        for i, exc_info in enumerate(exc_details, start=1):
            print(
                f"  {i}: {exc_info['exception_message']}\n\n{exc_info['exception_traceback']}\n{STAR_SEPARATOR}"
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
    unquoted_upper_col: str
    unquoted_lower_col: str


class TableFactory(Protocol):
    def __call__(
        self,
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema: str | None = None,
        data: Sequence[Row] = ...,
    ) -> None:
        ...


@pytest.fixture(
    scope="class",
)
def table_factory() -> Generator[TableFactory, None, None]:
    """
    Class scoped.
    Given a SQLALchemy engine, table_name and schema,
    create the table if it does not exist and drop it after the test class.
    """
    all_created_tables: dict[
        str, list[dict[Literal["table_name", "schema"], str | None]]
    ] = {}
    engines: dict[str, engine.Engine] = {}

    def _table_factory(
        gx_engine: SqlAlchemyExecutionEngine,
        table_names: set[str],
        schema: str | None = None,
        data: Sequence[Row] = tuple(),
    ) -> None:
        sa_engine = gx_engine.engine
        if sa_engine.dialect.name in DO_NOT_CREATE_TABLES:
            LOGGER.info(
                f"Skipping table creation for {table_names} for {sa_engine.dialect.name}"
            )
            return
        LOGGER.info(
            f"SQLA:{SQLA_VERSION} - Creating `{sa_engine.dialect.name}` table for {table_names} if it does not exist"
        )
        created_tables: list[dict[Literal["table_name", "schema"], str | None]] = []

        with gx_engine.get_connection() as conn:
            quoted_upper_col: str = quote_str(
                QUOTED_UPPER_COL, dialect=sa_engine.dialect.name
            )
            quoted_lower_col: str = quote_str(
                QUOTED_LOWER_COL, dialect=sa_engine.dialect.name
            )
            transaction = conn.begin()
            if schema:
                conn.execute(TextClause(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            for name in table_names:
                qualified_table_name = f"{schema}.{name}" if schema else name
                # TODO: use dialect specific quotes
                create_tables: str = (
                    f"CREATE TABLE IF NOT EXISTS {qualified_table_name}"
                    f" (id INTEGER, name VARCHAR(255), {quoted_upper_col} VARCHAR(255), {quoted_lower_col} VARCHAR(255),"
                    f" {UNQUOTED_UPPER_COL} VARCHAR(255), {UNQUOTED_LOWER_COL} VARCHAR(255))"
                )
                conn.execute(TextClause(create_tables))
                if data:
                    insert_data = (
                        f"INSERT INTO {qualified_table_name} (id, name, {quoted_upper_col}, {quoted_lower_col}, {UNQUOTED_UPPER_COL}, {UNQUOTED_LOWER_COL})"
                        " VALUES (:id, :name, :quoted_upper_col, :quoted_lower_col, :unquoted_upper_col, :unquoted_lower_col)"
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
        + RAND_SCHEMA
        + "?http_path=${DATABRICKS_HTTP_PATH}&catalog=ci&schema="
        + RAND_SCHEMA,
    )
    return ds


@pytest.fixture
def snowflake_creds_populated() -> bool:
    if os.getenv("SNOWFLAKE_CI_USER_PASSWORD") or os.getenv("SNOWFLAKE_CI_ACCOUNT"):
        return True
    return False


@pytest.fixture
def snowflake_ds(
    context: EphemeralDataContext,
    snowflake_creds_populated: bool,
) -> SnowflakeDatasource:
    if not snowflake_creds_populated:
        pytest.skip("no snowflake credentials")
    ds = context.sources.add_snowflake(
        "snowflake",
        connection_string="snowflake://ci:${SNOWFLAKE_CI_USER_PASSWORD}@oca29081.us-east-1/ci"
        f"/{RAND_SCHEMA}?warehouse=ci&role=ci",
        # NOTE: uncomment this and set SNOWFLAKE_USER to run tests against your own snowflake account
        # connection_string="snowflake://${SNOWFLAKE_USER}@oca29081.us-east-1/DEMO_DB/RESTAURANTS?warehouse=COMPUTE_WH&role=PUBLIC&authenticator=externalbrowser",
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
        table_factory(
            gx_engine=postgres_ds.get_execution_engine(), table_names={table_name}
        )

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

        table_names: list[str] = inspect(
            databricks_sql_ds.get_engine()
        ).get_table_names(schema=RAND_SCHEMA)
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

        table_names: list[str] = inspect(snowflake_ds.get_engine()).get_table_names(
            schema=schema
        )
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

        _ = _get_exception_details(result, prettyprint=True)
        assert result.success is True


ColNameParamId: TypeAlias = Literal[
    "str unquoted_lower_col",
    'str "unquoted_lower_col"',
    "str UNQUOTED_LOWER_COL",
    'str "UNQUOTED_LOWER_COL"',
    # ----------------------
    "str unquoted_upper_col",
    'str "unquoted_upper_col"',
    "str UNQUOTED_UPPER_COL",
    'str "UNQUOTED_UPPER_COL"',
    # ----------------------
    "str quoted_lower_col",
    'str "quoted_lower_col"',
    "str QUOTED_LOWER_COL",
    'str "QUOTED_LOWER_COL"',
    # ----------------------
    "str quoted_upper_col",
    'str "quoted_upper_col"',
    "str QUOTED_UPPER_COL",
    'str "QUOTED_UPPER_COL"',
    # ----------------------
]


# TODO: remove items from this lookup when working on fixes
REQUIRE_FIXES: Final[dict[str, list[DatabaseType]]] = {
    'expect_column_to_exist-str "unquoted_lower_col"': ["postgres", "sqlite"],
    "expect_column_to_exist-str UNQUOTED_LOWER_COL": [
        "databricks_sql",
        "postgres",
        "sqlite",
    ],
    'expect_column_to_exist-str "UNQUOTED_LOWER_COL"': ["snowflake", "sqlite"],
    "expect_column_to_exist-str unquoted_upper_col": ["databricks_sql", "sqlite"],
    'expect_column_to_exist-str "unquoted_upper_col"': ["postgres", "sqlite"],
    "expect_column_to_exist-str UNQUOTED_UPPER_COL": ["postgres"],
    'expect_column_to_exist-str "UNQUOTED_UPPER_COL"': ["snowflake", "sqlite"],
    'expect_column_to_exist-str "quoted_lower_col"': [
        "postgres",
        "snowflake",
        "sqlite",
    ],
    "expect_column_to_exist-str quoted_lower_col": ["snowflake"],
    "expect_column_to_exist-str QUOTED_LOWER_COL": [
        "databricks_sql",
        "postgres",
        "snowflake",
        "sqlite",
    ],
    'expect_column_to_exist-str "QUOTED_LOWER_COL"': ["sqlite"],
    "expect_column_to_exist-str quoted_upper_col": [
        "databricks_sql",
        "sqlite",
    ],
    'expect_column_to_exist-str "quoted_upper_col"': ["sqlite"],
    "expect_column_to_exist-str QUOTED_UPPER_COL": ["postgres"],
    'expect_column_to_exist-str "QUOTED_UPPER_COL"': [
        "postgres",
        "snowflake",
        "sqlite",
    ],
    'expect_column_values_to_not_be_null-str "unquoted_lower_col"': [
        "postgres",
        "snowflake",
    ],
    "expect_column_values_to_not_be_null-str UNQUOTED_LOWER_COL": [
        "databricks_sql",
        "postgres",
        "sqlite",
    ],
    'expect_column_values_to_not_be_null-str "UNQUOTED_LOWER_COL"': [
        "postgres",
        "snowflake",
    ],
    "expect_column_values_to_not_be_null-str unquoted_upper_col": [
        "databricks_sql",
        "sqlite",
    ],
    'expect_column_values_to_not_be_null-str "unquoted_upper_col"': [
        "postgres",
        "snowflake",
    ],
    "expect_column_values_to_not_be_null-str UNQUOTED_UPPER_COL": [
        "postgres",
    ],
    'expect_column_values_to_not_be_null-str "UNQUOTED_UPPER_COL"': [
        "postgres",
        "snowflake",
    ],
    "expect_column_values_to_not_be_null-str quoted_lower_col": [
        "snowflake",
    ],
    'expect_column_values_to_not_be_null-str "quoted_lower_col"': [
        "postgres",
        "snowflake",
    ],
    "expect_column_values_to_not_be_null-str QUOTED_LOWER_COL": [
        "databricks_sql",
        "postgres",
        "snowflake",
        "sqlite",
    ],
    'expect_column_values_to_not_be_null-str "QUOTED_LOWER_COL"': [
        "postgres",
        "snowflake",
    ],
    "expect_column_values_to_not_be_null-str quoted_upper_col": [
        "databricks_sql",
        "postgres",
        "sqlite",
    ],
    'expect_column_values_to_not_be_null-str "quoted_upper_col"': [
        "postgres",
        "snowflake",
    ],
    "expect_column_values_to_not_be_null-str QUOTED_UPPER_COL": [
        "postgres",
    ],
    'expect_column_values_to_not_be_null-str "QUOTED_UPPER_COL"': [
        "postgres",
        "snowflake",
    ],
}


def _requires_fix(param_id: str) -> bool:
    column_name: ColNameParamId
    dialect, expectation, column_name = param_id.split("-")  # type: ignore[assignment]
    key = f"{expectation}-{column_name}"
    dialects_need_fixes: list[DatabaseType] = REQUIRE_FIXES.get(key, [])
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
    """Use a simple 'SELECT {column_name_param} from {qualified_table_name};' query to check if the column exists.'"""
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
            if not col_exist_result:
                return False
        except SqlAlchemyProgrammingError as sql_err:
            LOGGER.warning("SQLAlchemy Error", exc_info=sql_err)
            print(f"\n{column_name_param} does not exist!\n")
            return False
        return True


@pytest.mark.filterwarnings(
    "once::DeprecationWarning"
)  # snowflake `add_table_asset` raises warning on passing a schema
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
    ],
)
class TestColumnIdentifiers:
    @pytest.mark.parametrize(
        "expectation_type",
        [
            "expect_column_to_exist",
            "expect_column_values_to_not_be_null",
        ],
    )
    def test_column_expectation(
        self,
        context: EphemeralDataContext,
        all_sql_datasources: SQLDatasource,
        table_factory: TableFactory,
        column_name: str | quoted_name,
        expectation_type: str,
        request: pytest.FixtureRequest,
    ):
        param_id = request.node.callspec.id
        datasource = all_sql_datasources
        dialect = datasource.get_engine().dialect.name

        if _is_quote_char_dialect_mismatch(dialect, column_name):
            pytest.skip(f"quote char dialect mismatch: {column_name[0]}")

        if _requires_fix(param_id):
            # apply marker this way so that xpasses can be seen in the report
            request.applymarker(pytest.mark.xfail)

        schema: str | None = (
            RAND_SCHEMA
            if GXSqlDialect(dialect)
            in (GXSqlDialect.SNOWFLAKE, GXSqlDialect.DATABRICKS)
            else None
        )

        print(f"\ncolumn_name:\n  {column_name!r}")
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
                },
            ],
        )

        qualified_table_name: str = (
            f"{schema}.{TEST_TABLE_NAME}" if schema else TEST_TABLE_NAME
        )
        # check that the column exists so that we know what if the expectation should succeed or fail
        column_exists = _raw_query_check_column_exists(
            column_name,
            qualified_table_name,
            datasource.get_execution_engine(),
        )

        asset = datasource.add_table_asset(
            "my_asset", table_name=TEST_TABLE_NAME, schema_name=schema
        )
        print(f"asset:\n{asset!r}\n")

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

        exc_details = _get_exception_details(result, prettyprint=True)
        assert not exc_details, exc_details[0]["raised_exception"]

        if column_exists:
            assert result.success is True, "column exists but validation failed"
        else:
            assert (
                result.success is False
            ), "column does not exist but validation succeeded"


if __name__ == "__main__":
    pytest.main([__file__, "-vv", "-rXf"])
