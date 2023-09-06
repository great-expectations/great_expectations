from __future__ import annotations

from enum import Enum
from typing import Any, Final, List, Mapping, Union

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
    HIVE = "hive"
    MSSQL = "mssql"
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
        return [
            dialect_name.value
            for dialect_name in cls
            if dialect_name != GXSqlDialect.OTHER
        ]

    @classmethod
    def get_all_dialects(cls) -> List[GXSqlDialect]:
        """Get all dialects."""
        return [dialect for dialect in cls if dialect != GXSqlDialect.OTHER]


DIALECT_QUOTE_STRINGS: Final[Mapping[GXSqlDialect, str]] = {
    # TODO: add other dialects
    GXSqlDialect.SNOWFLAKE: '"',
    GXSqlDialect.POSTGRESQL: '"',
    GXSqlDialect.SQLITE: '"',
    GXSqlDialect.DATABRICKS: "`",
}


def quote_str(unquoted_identifier: str, dialect: GXSqlDialect) -> str:
    """Quote a string using the specified dialect's quote character."""
    quote_char = DIALECT_QUOTE_STRINGS[dialect]
    return f"{quote_char}{unquoted_identifier}{quote_char}"


def _strip_quotes(s: str, dialect: GXSqlDialect) -> str:
    quote_str = DIALECT_QUOTE_STRINGS[dialect]
    if s.startswith(quote_str) and s.endswith(quote_str):
        return s[1:-1]
    return s


# TODO: only require dialect for string inputs (add overloads)
def wrap_identifier(
    indentifier: str | quoted_name, dialect: GXSqlDialect
) -> quoted_name:
    if isinstance(indentifier, quoted_name):
        return indentifier
    wo_quotes = _strip_quotes(indentifier, dialect)
    return quoted_name(wo_quotes, quote=True)
