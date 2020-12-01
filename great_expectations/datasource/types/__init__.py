import enum

from .batch_kwargs import *
from .batch_spec import *


# noinspection SpellCheckingInspection
class DatasourceTypes(enum.Enum):
    PANDAS = "pandas"
    SPARK = "spark"
    SQL = "sql"
    # TODO DBT = "dbt"


# noinspection SpellCheckingInspection
class SupportedDatabases(enum.Enum):
    MYSQL = "MySQL"
    POSTGRES = "Postgres"
    REDSHIFT = "Redshift"
    SNOWFLAKE = "Snowflake"
    BIGQUERY = "BigQuery"
    OTHER = "other - Do you have a working SQLAlchemy connection string?"
    # TODO MSSQL
