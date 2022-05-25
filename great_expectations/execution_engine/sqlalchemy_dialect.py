from __future__ import annotations

import enum
from typing import List


class GESqlDialect(enum.Enum):
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

    @classmethod
    def get_all_dialect_names(cls) -> List[str]:
        """Get dialect names for all SQL dialects."""
        return [dialect_name.value for dialect_name in cls]

    @classmethod
    def get_all_dialects(cls) -> List[GESqlDialect]:
        """Get all dialects."""
        return [dialect for dialect in cls]
