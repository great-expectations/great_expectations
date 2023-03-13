import enum


class BackendDependencies(enum.Enum):
    AWS = "AWS"
    AZURE = "AZURE"
    BIGQUERY = "BIGQUERY"
    GCS = "GCS"
    MYSQL = "MYSQL"
    MSSQL = "MSSQL"
    PANDAS = "PANDAS"
    POSTGRESQL = "POSTGRESQL"
    REDSHIFT = "REDSHIFT"
    SPARK = "SPARK"
    SQLALCHEMY = "SQLALCHEMY"
    SNOWFLAKE = "SNOWFLAKE"
    TRINO = "TRINO"
