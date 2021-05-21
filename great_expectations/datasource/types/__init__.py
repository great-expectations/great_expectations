import enum

from .batch_kwargs import *


# noinspection SpellCheckingInspection
class DatasourceTypes(enum.Enum):
    PANDAS = "pandas"
    SPARK = "spark"
    SQL = "sqlalchemy"


class SupportedDatabaseBackends(enum.Enum):
    BIGQUERY = "BigQuery"
    MYSQL = "MySQL"
    OTHER = "other - Do you have a working SQLAlchemy connection string?"
    POSTGRES = "Postgres"
    REDSHIFT = "Redshift"
    SNOWFLAKE = "Snowflake"
    # TODO MSSQL
