"""Unit tests for great_expectations.expectations.type_comparison.

Every dialect tests BOTH compare_column_type (scalar) and
compare_column_type_list (list) independently — no assumption is made
that they share an implementation.
"""

from __future__ import annotations

import importlib
import inspect

import pytest

from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations import type_comparison
from great_expectations.expectations.metrics.util import CaseInsensitiveString
from great_expectations.expectations.type_comparison import (
    CASE_INSENSITIVE_DIALECTS,
    compare_column_type,
    compare_column_type_list,
    native_type_type_map,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# CASE_INSENSITIVE_DIALECTS
# ---------------------------------------------------------------------------


class TestCaseInsensitiveDialects:
    def test_contains_expected_dialects(self):
        expected = {
            GXSqlDialect.DATABRICKS,
            GXSqlDialect.POSTGRESQL,
            GXSqlDialect.SNOWFLAKE,
            GXSqlDialect.SQL_SERVER,
            GXSqlDialect.TRINO,
        }
        assert expected == CASE_INSENSITIVE_DIALECTS

    def test_is_frozenset(self):
        assert isinstance(CASE_INSENSITIVE_DIALECTS, frozenset)

    @pytest.mark.parametrize(
        "dialect_string",
        ["databricks", "postgresql", "snowflake", "mssql", "trino"],
    )
    def test_string_membership_via_gxsqldialect_eq(self, dialect_string):
        """SqlAlchemyExecutionEngine.dialect_name returns lowercase strings.
        GXSqlDialect.__eq__ handles cross-type comparison, so 'in' checks work."""
        assert dialect_string in CASE_INSENSITIVE_DIALECTS


# ---------------------------------------------------------------------------
# native_type_type_map
# ---------------------------------------------------------------------------


class TestNativeTypeTypeMap:
    @pytest.mark.parametrize(
        "type_str, expected",
        [
            ("none", (type(None),)),
            ("None", (type(None),)),
            ("bool", (bool,)),
            ("int", (int,)),
            ("long", (int,)),
            ("float", (float,)),
            ("bytes", (bytes,)),
            ("complex", (complex,)),
            ("str", (str,)),
            ("string_types", (str,)),
            ("list", (list,)),
            ("dict", (dict,)),
        ],
    )
    def test_known_types(self, type_str, expected):
        assert native_type_type_map(type_str) == expected

    def test_unicode_returns_none(self):
        assert native_type_type_map("unicode") is None

    def test_unrecognized_returns_none(self):
        assert native_type_type_map("some_unknown_type") is None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _StubEngine:
    """Lightweight stub for SqlAlchemyExecutionEngine."""

    def __init__(self, dialect_name, dialect_module=None):
        self.dialect_name = dialect_name
        self.dialect_module = dialect_module


class _NonStringType:
    """Stub for a non-string column type."""

    @override
    def __str__(self):
        return "INTEGER"


_NonStringType.__name__ = "INTEGER"


class _NullableType:
    def __init__(self, nested_type):
        self.nested_type = nested_type


_NullableType.__name__ = "Nullable"


def _ci(s: str) -> CaseInsensitiveString:
    """Shorthand for constructing a CaseInsensitiveString."""
    return CaseInsensitiveString(s)


# ===========================================================================
# ClickHouse (isinstance path with Nullable wrapper)
# ===========================================================================


class TestClickHouseNullable:
    engine = _StubEngine(GXSqlDialect.CLICKHOUSE, dialect_module=sa)

    @pytest.fixture(autouse=True)
    def _stub_clickhouse_types(self, monkeypatch):
        class _ClickHouseTypes:
            Nullable = _NullableType

        monkeypatch.setattr(type_comparison, "ch_types", _ClickHouseTypes)

    @pytest.mark.parametrize(
        "expected_type, expected_success", [("BIGINT", True), ("VARCHAR", False)]
    )
    def test_scalar_compares_and_reports_nested_type(self, expected_type, expected_success):
        success, observed = compare_column_type(
            self.engine, _NullableType(sa.types.BIGINT()), expected_type
        )

        assert success is expected_success
        assert observed == "BIGINT"

    @pytest.mark.parametrize(
        "expected_types, expected_success",
        [(["VARCHAR", "BIGINT"], True), (["VARCHAR", "BOOLEAN"], False)],
    )
    def test_list_compares_and_reports_nested_type(self, expected_types, expected_success):
        success, observed = compare_column_type_list(
            self.engine,
            _NullableType(sa.types.BIGINT()),
            expected_types,
        )

        assert success is expected_success
        assert observed == "BIGINT"


# ===========================================================================
# PostgreSQL
# ===========================================================================

_PG_TYPES = [
    "CHAR",
    "TEXT",
    "VARCHAR",
    "INTEGER",
    "SMALLINT",
    "BIGINT",
    "TIMESTAMP WITHOUT TIME ZONE",
    "DATE",
    "TIME",
    "DOUBLE PRECISION",
    "BOOLEAN",
    "NUMERIC",
    "DECIMAL",
    "REAL",
    "BYTEA",
    "JSON",
    "JSONB",
    "UUID",
    "INTERVAL",
]


class TestPostgreSQLScalar:
    engine = _StubEngine(GXSqlDialect.POSTGRESQL)

    @pytest.mark.parametrize("type_name", _PG_TYPES)
    def test_exact_match(self, type_name):
        success, observed = compare_column_type(self.engine, _ci(type_name), type_name)
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _PG_TYPES)
    def test_case_insensitive_match(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), type_name.lower())
        assert success is True

    @pytest.mark.parametrize("type_name", _PG_TYPES)
    def test_mismatch(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), "__NO_SUCH_TYPE__")
        assert success is False


class TestPostgreSQLList:
    engine = _StubEngine(GXSqlDialect.POSTGRESQL)

    @pytest.mark.parametrize("type_name", _PG_TYPES)
    def test_match_in_list(self, type_name):
        success, observed = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", type_name],
        )
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _PG_TYPES)
    def test_case_insensitive_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            [type_name.lower()],
        )
        assert success is True

    @pytest.mark.parametrize("type_name", _PG_TYPES)
    def test_no_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", "__ALSO_WRONG__"],
        )
        assert success is False

    def test_timestamp_accepted_variants(self):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci("TIMESTAMP WITHOUT TIME ZONE"),
            ["TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE"],
        )
        assert success is True


# ===========================================================================
# Snowflake
# ===========================================================================

_SNOWFLAKE_TYPES = [
    "STRING",
    "TEXT",
    "CHARACTER",
    "VARCHAR",
    "VARCHAR(16777216)",
    "BYTEINT",
    "TINYINT",
    "INTEGER",
    "BIGINT",
    "FLOAT",
    "DOUBLE",
    "DECIMAL(38, 0)",
    "DECIMAL(38,0)",
    "FIXED",
    "DEC",
    "NUMBER",
    "DATE",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
    "TIME",
    "VARIANT",
    "VARBINARY",
    "BINARY",
    "GEOGRAPHY",
    "GEOMETRY",
    "ARRAY",
    "OBJECT",
    "BOOLEAN",
]


class TestSnowflakeScalar:
    engine = _StubEngine(GXSqlDialect.SNOWFLAKE)

    @pytest.mark.parametrize("type_name", _SNOWFLAKE_TYPES)
    def test_exact_match(self, type_name):
        success, observed = compare_column_type(self.engine, _ci(type_name), type_name)
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _SNOWFLAKE_TYPES)
    def test_case_insensitive_match(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), type_name.lower())
        assert success is True

    @pytest.mark.parametrize("type_name", _SNOWFLAKE_TYPES)
    def test_mismatch(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), "__NO_SUCH_TYPE__")
        assert success is False


class TestSnowflakeList:
    engine = _StubEngine(GXSqlDialect.SNOWFLAKE)

    @pytest.mark.parametrize("type_name", _SNOWFLAKE_TYPES)
    def test_match_in_list(self, type_name):
        success, observed = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", type_name],
        )
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _SNOWFLAKE_TYPES)
    def test_case_insensitive_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            [type_name.lower()],
        )
        assert success is True

    @pytest.mark.parametrize("type_name", _SNOWFLAKE_TYPES)
    def test_no_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", "__ALSO_WRONG__"],
        )
        assert success is False

    def test_integer_synonyms(self):
        for int_type in ("BYTEINT", "TINYINT", "INTEGER", "BIGINT"):
            success, _obs = compare_column_type_list(
                self.engine,
                _ci("DECIMAL(38, 0)"),
                [int_type, "DECIMAL(38, 0)"],
            )
            assert success is True

    def test_string_variants(self):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci("VARCHAR(16777216)"),
            ["STRING", "VARCHAR", "VARCHAR(16777216)"],
        )
        assert success is True

    def test_number_variants(self):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci("DECIMAL(38, 0)"),
            ["NUMBER", "DECIMAL", "NUMERIC", "DECIMAL(38, 0)"],
        )
        assert success is True


# ===========================================================================
# Databricks
# ===========================================================================

_DATABRICKS_TYPES = [
    "STRING",
    "INT",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "BOOLEAN",
    "FLOAT",
    "DOUBLE",
    "DECIMAL",
    "DECIMAL(10, 0)",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "BINARY",
    "ARRAY",
    "MAP",
    "STRUCT",
]


class TestDatabricksScalar:
    engine = _StubEngine(GXSqlDialect.DATABRICKS)

    @pytest.mark.parametrize("type_name", _DATABRICKS_TYPES)
    def test_exact_match(self, type_name):
        success, observed = compare_column_type(self.engine, _ci(type_name), type_name)
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _DATABRICKS_TYPES)
    def test_case_insensitive_match(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), type_name.lower())
        assert success is True

    @pytest.mark.parametrize("type_name", _DATABRICKS_TYPES)
    def test_mismatch(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), "__NO_SUCH_TYPE__")
        assert success is False


class TestDatabricksList:
    engine = _StubEngine(GXSqlDialect.DATABRICKS)

    @pytest.mark.parametrize("type_name", _DATABRICKS_TYPES)
    def test_match_in_list(self, type_name):
        success, observed = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", type_name],
        )
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _DATABRICKS_TYPES)
    def test_case_insensitive_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            [type_name.lower()],
        )
        assert success is True

    @pytest.mark.parametrize("type_name", _DATABRICKS_TYPES)
    def test_no_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", "__ALSO_WRONG__"],
        )
        assert success is False

    def test_decimal_variants(self):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci("DECIMAL(10, 0)"),
            ["DECIMAL", "DECIMAL(10, 0)"],
        )
        assert success is True


# ===========================================================================
# SQL Server / MS SQL
# ===========================================================================

_SQL_SERVER_TYPES = [
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "FLOAT",
    "REAL",
    "NUMERIC",
    "DECIMAL",
    "MONEY",
    "SMALLMONEY",
    "VARCHAR",
    "NVARCHAR",
    "CHAR",
    "NCHAR",
    "TEXT",
    "NTEXT",
    "BIT",
    "DATE",
    "DATETIME",
    "DATETIME2",
    "DATETIMEOFFSET",
    "SMALLDATETIME",
    "TIME",
    "UNIQUEIDENTIFIER",
    "VARBINARY",
    "IMAGE",
    "XML",
]


class TestSQLServerScalar:
    engine = _StubEngine(GXSqlDialect.SQL_SERVER)

    @pytest.mark.parametrize("type_name", _SQL_SERVER_TYPES)
    def test_exact_match(self, type_name):
        success, observed = compare_column_type(self.engine, _ci(type_name), type_name)
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _SQL_SERVER_TYPES)
    def test_case_insensitive_match(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), type_name.lower())
        assert success is True

    @pytest.mark.parametrize("type_name", _SQL_SERVER_TYPES)
    def test_mismatch(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), "__NO_SUCH_TYPE__")
        assert success is False


class TestSQLServerList:
    engine = _StubEngine(GXSqlDialect.SQL_SERVER)

    @pytest.mark.parametrize("type_name", _SQL_SERVER_TYPES)
    def test_match_in_list(self, type_name):
        success, observed = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", type_name],
        )
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _SQL_SERVER_TYPES)
    def test_case_insensitive_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            [type_name.lower()],
        )
        assert success is True

    @pytest.mark.parametrize("type_name", _SQL_SERVER_TYPES)
    def test_no_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", "__ALSO_WRONG__"],
        )
        assert success is False


# ===========================================================================
# Trino
# ===========================================================================

_TRINO_TYPES = [
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "DOUBLE",
    "REAL",
    "DECIMAL",
    "VARCHAR",
    "CHAR",
    "VARBINARY",
    "BOOLEAN",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "TIMESTAMP WITH TIME ZONE",
    "JSON",
    "UUID",
    "ARRAY",
    "MAP",
    "ROW",
]


class TestTrinoScalar:
    engine = _StubEngine(GXSqlDialect.TRINO)

    @pytest.mark.parametrize("type_name", _TRINO_TYPES)
    def test_exact_match(self, type_name):
        success, observed = compare_column_type(self.engine, _ci(type_name), type_name)
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _TRINO_TYPES)
    def test_case_insensitive_match(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), type_name.lower())
        assert success is True

    @pytest.mark.parametrize("type_name", _TRINO_TYPES)
    def test_mismatch(self, type_name):
        success, _obs = compare_column_type(self.engine, _ci(type_name), "__NO_SUCH_TYPE__")
        assert success is False


class TestTrinoList:
    engine = _StubEngine(GXSqlDialect.TRINO)

    @pytest.mark.parametrize("type_name", _TRINO_TYPES)
    def test_match_in_list(self, type_name):
        success, observed = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", type_name],
        )
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("type_name", _TRINO_TYPES)
    def test_case_insensitive_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            [type_name.lower()],
        )
        assert success is True

    @pytest.mark.parametrize("type_name", _TRINO_TYPES)
    def test_no_match_in_list(self, type_name):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci(type_name),
            ["__WRONG__", "__ALSO_WRONG__"],
        )
        assert success is False


# ===========================================================================
# SQLite (isinstance path)
# ===========================================================================

_SQLITE_TYPE_PAIRS = [
    (sa.types.INTEGER(), "INTEGER"),
    (sa.types.SMALLINT(), "SMALLINT"),
    (sa.types.BIGINT(), "BIGINT"),
    (sa.types.FLOAT(), "FLOAT"),
    (sa.types.NUMERIC(), "NUMERIC"),
    (sa.types.DECIMAL(), "DECIMAL"),
    (sa.types.VARCHAR(), "VARCHAR"),
    (sa.types.CHAR(), "CHAR"),
    (sa.types.TEXT(), "TEXT"),
    (sa.types.BOOLEAN(), "BOOLEAN"),
    (sa.types.DATE(), "DATE"),
    (sa.types.DATETIME(), "DATETIME"),
    (sa.types.TIME(), "TIME"),
    (sa.types.BLOB(), "BLOB"),
]


class TestSQLiteScalar:
    engine = _StubEngine("sqlite", dialect_module=sa)

    @pytest.mark.parametrize("actual_type, expected_name", _SQLITE_TYPE_PAIRS)
    def test_match(self, actual_type, expected_name):
        success, observed = compare_column_type(self.engine, actual_type, expected_name)
        assert success is True
        assert observed == type(actual_type).__name__

    @pytest.mark.parametrize("actual_type, expected_name", _SQLITE_TYPE_PAIRS)
    def test_mismatch(self, actual_type, expected_name):
        success, _obs = compare_column_type(self.engine, actual_type, "__NO_SUCH_TYPE__")
        assert success is False


class TestSQLiteList:
    engine = _StubEngine("sqlite", dialect_module=sa)

    @pytest.mark.parametrize("actual_type, expected_name", _SQLITE_TYPE_PAIRS)
    def test_match_in_list(self, actual_type, expected_name):
        success, observed = compare_column_type_list(
            self.engine,
            actual_type,
            ["__WRONG__", expected_name],
        )
        assert success is True
        assert observed == type(actual_type).__name__

    @pytest.mark.parametrize("actual_type, expected_name", _SQLITE_TYPE_PAIRS)
    def test_no_match_in_list(self, actual_type, expected_name):
        success, _obs = compare_column_type_list(
            self.engine,
            actual_type,
            ["__WRONG__", "__ALSO_WRONG__"],
        )
        assert success is False


class TestNonClassDialectModuleAttribute:
    """A dialect module also exports submodules and functions; naming one is a mismatch."""

    @pytest.mark.parametrize("dialect_name", ["sqlite", "mysql"])
    def test_scalar(self, dialect_name):
        dialect_module = importlib.import_module(f"sqlalchemy.dialects.{dialect_name}")
        assert inspect.ismodule(dialect_module.json)
        engine = _StubEngine(dialect_name, dialect_module=dialect_module)
        success, observed = compare_column_type(engine, dialect_module.JSON(), "json")
        assert success is False
        assert observed == "JSON"

    @pytest.mark.parametrize("dialect_name", ["sqlite", "mysql"])
    def test_list(self, dialect_name):
        dialect_module = importlib.import_module(f"sqlalchemy.dialects.{dialect_name}")
        engine = _StubEngine(dialect_name, dialect_module=dialect_module)
        success, _obs = compare_column_type_list(engine, dialect_module.JSON(), ["json", "JSON"])
        assert success is True


# ===========================================================================
# "Numeric" matches floating-point columns on every SQLAlchemy version
# ===========================================================================

# Floating-point types as reflection returns them. SQLAlchemy 2.1 made Float a sibling of
# Numeric instead of a subclass; before 2.1 each of these is an instance of Numeric.
_FLOATING_POINT_TYPES = [
    sa.types.Float(),
    sa.types.FLOAT(),
    sa.types.REAL(),
    sa.dialects.postgresql.DOUBLE_PRECISION(),
    sa.dialects.mysql.DOUBLE(),
]


class TestNumericMatchesFloatingPoint:
    engine = _StubEngine("sqlite", dialect_module=sa)

    @pytest.mark.parametrize("actual_type", _FLOATING_POINT_TYPES)
    def test_scalar(self, actual_type):
        success, observed = compare_column_type(self.engine, actual_type, "Numeric")
        assert success is True
        assert observed == type(actual_type).__name__

    @pytest.mark.parametrize("actual_type", _FLOATING_POINT_TYPES)
    def test_list(self, actual_type):
        success, _obs = compare_column_type_list(self.engine, actual_type, ["__WRONG__", "Numeric"])
        assert success is True

    @pytest.mark.parametrize(
        "actual_type", [sa.types.INTEGER(), sa.types.VARCHAR(), sa.types.DATE()]
    )
    def test_non_numeric_types_still_do_not_match(self, actual_type):
        success, _obs = compare_column_type(self.engine, actual_type, "Numeric")
        assert success is False

    def test_float_does_not_match_a_fixed_point_column(self):
        success, _obs = compare_column_type(self.engine, sa.types.NUMERIC(), "Float")
        assert success is False


# ===========================================================================
# MySQL (isinstance path)
# ===========================================================================

_MYSQL_TYPE_PAIRS = [
    (sa.types.INTEGER(), "INTEGER"),
    (sa.types.SMALLINT(), "SMALLINT"),
    (sa.types.BIGINT(), "BIGINT"),
    (sa.types.FLOAT(), "FLOAT"),
    (sa.types.DECIMAL(), "DECIMAL"),
    (sa.types.NUMERIC(), "NUMERIC"),
    (sa.types.VARCHAR(), "VARCHAR"),
    (sa.types.CHAR(), "CHAR"),
    (sa.types.TEXT(), "TEXT"),
    (sa.types.BOOLEAN(), "BOOLEAN"),
    (sa.types.DATE(), "DATE"),
    (sa.types.DATETIME(), "DATETIME"),
    (sa.types.TIME(), "TIME"),
    (sa.types.BLOB(), "BLOB"),
]


class TestMySQLScalar:
    engine = _StubEngine("mysql", dialect_module=sa)

    @pytest.mark.parametrize("actual_type, expected_name", _MYSQL_TYPE_PAIRS)
    def test_match(self, actual_type, expected_name):
        success, observed = compare_column_type(self.engine, actual_type, expected_name)
        assert success is True
        assert observed == type(actual_type).__name__

    @pytest.mark.parametrize("actual_type, expected_name", _MYSQL_TYPE_PAIRS)
    def test_mismatch(self, actual_type, expected_name):
        success, _obs = compare_column_type(self.engine, actual_type, "__NO_SUCH_TYPE__")
        assert success is False


class TestMySQLList:
    engine = _StubEngine("mysql", dialect_module=sa)

    @pytest.mark.parametrize("actual_type, expected_name", _MYSQL_TYPE_PAIRS)
    def test_match_in_list(self, actual_type, expected_name):
        success, observed = compare_column_type_list(
            self.engine,
            actual_type,
            ["__WRONG__", expected_name],
        )
        assert success is True
        assert observed == type(actual_type).__name__

    @pytest.mark.parametrize("actual_type, expected_name", _MYSQL_TYPE_PAIRS)
    def test_no_match_in_list(self, actual_type, expected_name):
        success, _obs = compare_column_type_list(
            self.engine,
            actual_type,
            ["__WRONG__", "__ALSO_WRONG__"],
        )
        assert success is False


# ===========================================================================
# CaseInsensitiveString — quoted behavior
# ===========================================================================


class TestCaseInsensitiveStringQuotedScalar:
    engine = _StubEngine(GXSqlDialect.POSTGRESQL)

    def test_quoted_exact_match(self):
        success, _obs = compare_column_type(self.engine, _ci('"MyType"'), '"MyType"')
        assert success is True

    def test_quoted_rejects_case_mismatch(self):
        success, _obs = compare_column_type(self.engine, _ci('"MyType"'), '"mytype"')
        assert success is False

    def test_unquoted_accepts_case_mismatch(self):
        success, _obs = compare_column_type(self.engine, _ci("MyType"), "mytype")
        assert success is True


class TestCaseInsensitiveStringQuotedList:
    engine = _StubEngine(GXSqlDialect.POSTGRESQL)

    def test_quoted_in_type_list(self):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci('"MyType"'),
            ['"mytype"', '"MyType"'],
        )
        assert success is True

    def test_list_uses_casefold_not_eq(self):
        """The type-list path uses .casefold() comparison, so quoted semantics
        are not preserved — unlike the scalar path which uses __eq__."""
        success, _obs = compare_column_type_list(
            self.engine,
            _ci('"MyType"'),
            ['"mytype"'],
        )
        assert success is True

    def test_unquoted_case_insensitive(self):
        success, _obs = compare_column_type_list(
            self.engine,
            _ci("MyType"),
            ["mytype"],
        )
        assert success is True


# ===========================================================================
# Non-string type fallback (both paths independently)
# ===========================================================================


class TestNonStringTypeFallback:
    @pytest.mark.parametrize("dialect", sorted(CASE_INSENSITIVE_DIALECTS, key=str))
    def test_scalar_match(self, dialect):
        engine = _StubEngine(dialect)
        success, _obs = compare_column_type(engine, _NonStringType(), "integer")
        assert success is True

    @pytest.mark.parametrize("dialect", sorted(CASE_INSENSITIVE_DIALECTS, key=str))
    def test_scalar_mismatch(self, dialect):
        engine = _StubEngine(dialect)
        success, _obs = compare_column_type(engine, _NonStringType(), "varchar")
        assert success is False

    @pytest.mark.parametrize("dialect", sorted(CASE_INSENSITIVE_DIALECTS, key=str))
    def test_list_match(self, dialect):
        engine = _StubEngine(dialect)
        success, observed = compare_column_type_list(
            engine,
            _NonStringType(),
            ["VARCHAR", "INTEGER"],
        )
        assert success is True
        assert observed == "INTEGER"

    @pytest.mark.parametrize("dialect", sorted(CASE_INSENSITIVE_DIALECTS, key=str))
    def test_list_mismatch(self, dialect):
        engine = _StubEngine(dialect)
        success, _obs = compare_column_type_list(
            engine,
            _NonStringType(),
            ["VARCHAR", "TEXT"],
        )
        assert success is False


# ===========================================================================
# String dialect_name (as returned by SqlAlchemyExecutionEngine in production)
# ===========================================================================

# Maps dialect string values to representative type names
_STRING_DIALECT_CASES = [
    ("databricks", "STRING"),
    ("postgresql", "INTEGER"),
    ("snowflake", "DECIMAL(38, 0)"),
    ("mssql", "NVARCHAR"),
    ("trino", "VARCHAR"),
]


class TestStringDialectName:
    """Verify that compare functions work when dialect_name is a plain string
    (the form returned by SqlAlchemyExecutionEngine.dialect_name in production),
    not a GXSqlDialect enum member."""

    @pytest.mark.parametrize("dialect_str, type_name", _STRING_DIALECT_CASES)
    def test_scalar(self, dialect_str, type_name):
        engine = _StubEngine(dialect_str)
        success, observed = compare_column_type(engine, _ci(type_name), type_name)
        assert success is True
        assert str(observed) == type_name

    @pytest.mark.parametrize("dialect_str, type_name", _STRING_DIALECT_CASES)
    def test_list(self, dialect_str, type_name):
        engine = _StubEngine(dialect_str)
        success, observed = compare_column_type_list(
            engine,
            _ci(type_name),
            ["__WRONG__", type_name],
        )
        assert success is True
        assert str(observed) == type_name
