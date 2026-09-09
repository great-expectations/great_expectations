from __future__ import annotations

from enum import Enum
from typing import Any, Final, List, Mapping, Union, overload

from great_expectations.compatibility.sqlalchemy import quoted_name
from great_expectations.compatibility.typing_extensions import override


class GXSqlDialect(Enum):
    """Contains sql dialects that have some level of support in Great Expectations.
    Also contains an unsupported attribute if the dialect is not in the list.
    """

    AWSATHENA = "awsathena"
    BIGQUERY = "bigquery"
    DATABRICKS = "databricks"
    DREMIO = "dremio"
    EXASOL = "exasol"
    HIVE = "hive"
    SQL_SERVER = "mssql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    POSTGRESQL = "postgresql"
    REDSHIFT = "redshift"
    SNOWFLAKE = "snowflake"
    SQLITE = "sqlite"
    TERADATASQL = "teradatasql"
    TRINO = "trino"
    VERTICA = "vertica"
    CLICKHOUSE = "clickhouse"
    SINGLESTOREDB = "singlestoredb"
    OTHER = "other"

    @override
    def __eq__(self, other: Union[str, bytes, GXSqlDialect]):  # type: ignore[override] # supertype uses `object`
        if isinstance(other, str):
            return self.value.lower() == other.lower()
        # Comparison against byte string, e.g. `b"hive"` should be treated as unicode
        elif isinstance(other, bytes):
            return self.value.lower() == other.lower().decode("utf-8")
        return self.value.lower() == other.value.lower()

    @override
    def __hash__(self: GXSqlDialect):
        return hash(self.value)

    @classmethod
    @override
    def _missing_(cls, value: Any) -> Any:
        try:
            # Sometimes `value` is a byte string, e.g. `b"hive"`, it should be converted
            return cls(value.decode())
        except (UnicodeDecodeError, AttributeError):
            return super()._missing_(value)

    @classmethod
    def get_all_dialect_names(cls) -> List[str]:
        """Get dialect names for all SQL dialects."""
        return [dialect_name.value for dialect_name in cls if dialect_name != GXSqlDialect.OTHER]

    @classmethod
    def get_all_dialects(cls) -> List[GXSqlDialect]:
        """Get all dialects."""
        return [dialect for dialect in cls if dialect != GXSqlDialect.OTHER]


DIALECT_IDENTIFIER_QUOTE_STRINGS: Final[Mapping[GXSqlDialect, tuple[str, str]]] = {
    GXSqlDialect.DATABRICKS: ("`", "`"),
    GXSqlDialect.SQL_SERVER: ("[", "]"),
    GXSqlDialect.MYSQL: ("`", "`"),
    GXSqlDialect.POSTGRESQL: ('"', '"'),
    GXSqlDialect.SNOWFLAKE: ('"', '"'),
    GXSqlDialect.SQLITE: ('"', '"'),
    GXSqlDialect.TRINO: ('"', '"'),
    GXSqlDialect.SINGLESTOREDB: ("`", "`"),
}


def quote_str(unquoted_identifier: str, dialect: GXSqlDialect) -> str:
    """Quote a string using the specified dialect's quote character."""
    open_q, close_q = DIALECT_IDENTIFIER_QUOTE_STRINGS[dialect]
    if unquoted_identifier.startswith(open_q) or unquoted_identifier.endswith(close_q):
        raise ValueError(  # noqa: TRY003 # FIXME CoP
            f"Identifier {unquoted_identifier} already uses quote characters {open_q}{close_q}"
        )
    return f"{open_q}{unquoted_identifier}{close_q}"


def _strip_quotes(s: str, dialect: GXSqlDialect) -> str:
    open_q, close_q = DIALECT_IDENTIFIER_QUOTE_STRINGS[dialect]
    if s.startswith(open_q) and s.endswith(close_q):
        return s[len(open_q) : -len(close_q)]
    # SQL Server also accepts double quotes when QUOTED_IDENTIFIER is ON
    if dialect == GXSqlDialect.SQL_SERVER and s.startswith('"') and s.endswith('"'):
        return s[1:-1]
    return s


@overload
def wrap_identifier(indentifier: str, dialect: GXSqlDialect = ...) -> quoted_name: ...


@overload
def wrap_identifier(
    indentifier: quoted_name, dialect: GXSqlDialect | None = ...
) -> quoted_name: ...


def wrap_identifier(
    indentifier: str | quoted_name, dialect: GXSqlDialect | None = None
) -> quoted_name:
    if isinstance(indentifier, quoted_name):
        return indentifier
    wo_quotes = _strip_quotes(indentifier, dialect)  # type: ignore[arg-type] # accounted for in overload
    return quoted_name(wo_quotes, quote=True)
