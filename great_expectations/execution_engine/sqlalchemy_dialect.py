from __future__ import annotations

from enum import Enum
from typing import Any, List, Union


class GXSqlDialect(Enum):
    """Contains sql dialects that have some level of support in Great Expectations.
    Also contains an unsupported attribute if the dialect is not in the list.
    """

    AWSATHENA = "awsathena"
    BIGQUERY = "bigquery"
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

    def __eq__(self, other: Union[str, bytes, GXSqlDialect]):  # type: ignore[override] # supertype uses `object`
        if isinstance(other, str):
            return self.value.lower() == other.lower()
        # Comparison against byte string, e.g. `b"hive"` should be treated as unicode
        elif isinstance(other, bytes):
            return self.value.lower() == other.lower().decode("utf-8")
        return self.value.lower() == other.value.lower()

    def __hash__(self: GXSqlDialect):
        return hash(self.value)

    @classmethod
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
