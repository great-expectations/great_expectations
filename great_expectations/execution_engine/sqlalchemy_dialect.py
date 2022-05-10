import enum


class GESqlDialect(enum.Enum):
    POSTGRESQL = "postgresql"
    BIGQUERY = "bigquery"
    DREMIO = "dremio"
    ORACLE = "oracle"
    MSSQL = "mssql"
    MYSQL = "mysql"
    AWSATHENA = "awsathena"
    TERADATASQL = "teradatasql"
    SNOWFLAKE = "snowflake"

    @classmethod
    def get_all_dialect_names(cls):
        """Get dialect names for all SQL dialects."""
        return [dialect_name.value for dialect_name in cls]
