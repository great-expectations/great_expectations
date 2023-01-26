from typing import Optional

from pydantic import PostgresDsn
from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import SQLDatasource


class PostgresDatasource(SQLDatasource):
    """Adds a postgres datasource to the data context.

    Args:
        name: The name of this postgres datasource.
        connection_string: An optional SQLAlchemy connection string used to connect to the postgres database.
            For example: "postgresql+psycopg2://postgres:@localhost/test_database"
            Either a connection_string or an engine must be provided.
            If an engine is provided, connection_string will be ignored.
        engine: An optional SQLAlchemy.engine.Engine used to connect to the database.
            Either a connection_string or an engine must be provided.
            If an engine is provided, connection_string will be ignored.
        assets: An optional dictionary whose keys are TableAsset names and whose values
            are TableAsset objects.
    """

    type: Literal["postgres"] = "postgres"  # type: ignore[assignment]
    connection_string: Optional[PostgresDsn] = None
