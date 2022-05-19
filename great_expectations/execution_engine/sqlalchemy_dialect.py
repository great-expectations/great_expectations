from __future__ import annotations

import enum
from typing import List


class GESqlDialect(enum.Enum):
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    ORACLE = "oracle"
    MSSQL = "mssql"
    SQLITE = "sqlite"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    AWSATHENA = "awsathena"
    DREMIO = "dremio"
    TERADATASQL = "teradatasql"
    TRINO = "trino"

    @classmethod
    def get_all_dialect_names(cls) -> List[str]:
        """Get dialect names for all SQL dialects."""
        return [dialect_name.value for dialect_name in cls]

    @classmethod
    def get_all_dialects(cls) -> List[GESqlDialect]:
        """Get all dialects."""
        return [dialect for dialect in cls]
