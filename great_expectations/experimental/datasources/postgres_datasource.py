from typing import TYPE_CHECKING

from pydantic import constr
from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import SQLDatasource

if TYPE_CHECKING:
    PostgresConnectionString = str
else:
    PostgresConnectionString = constr(regex=r"^postgresql")


class PostgresDatasource(SQLDatasource):
    type: Literal["postgres"] = "postgres"  # type: ignore[assignment]
    connection_string: PostgresConnectionString
