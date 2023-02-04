from __future__ import annotations

from typing import TYPE_CHECKING

from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import SQLDatasource

if TYPE_CHECKING:
    from pydantic import PostgresDsn


class PostgresDatasource(SQLDatasource):
    """Adds a postgres datasource to the data context.

    Args:
        name: The name of this postgres datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "postgresql+psycopg2://postgres:@localhost/test_database"
        assets: An optional dictionary whose keys are TableAsset names and whose values
            are TableAsset objects.
    """

    type: Literal["postgres"] = "postgres"  # type: ignore[assignment]
    connection_string: PostgresDsn
