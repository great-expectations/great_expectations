from pydantic import PostgresDsn
from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import SQLDatasource


class PostgresDatasource(SQLDatasource):
    type: Literal["postgres"] = "postgres"  # type: ignore[assignment]
    connection_string: PostgresDsn
