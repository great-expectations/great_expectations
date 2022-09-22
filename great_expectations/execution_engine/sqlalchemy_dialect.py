from __future__ import annotations

import enum
from typing import Any, List, Union


class GESqlDialect(enum.Enum):
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
    OTHER = "other"

    def __eq__(self, other: Union[str, GESqlDialect]):
        if isinstance(other, str):
            return self.value.lower() == other.lower()
        return self.value.lower() == other.value.lower()

    def __hash__(self: GESqlDialect):
        return hash(self.value)

    @classmethod
    def _missing_(cls, value: Any) -> None:
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
            if dialect_name != GESqlDialect.OTHER
        ]

    @classmethod
    def get_all_dialects(cls) -> List[GESqlDialect]:
        """Get all dialects."""
        return [dialect for dialect in cls if dialect != GESqlDialect.OTHER]
