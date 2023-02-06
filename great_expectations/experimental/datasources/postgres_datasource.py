from pydantic import PostgresDsn
from typing_extensions import Literal

from great_expectations.experimental.datasources.sql_datasource import SQLDatasource


class PostgresDatasource(SQLDatasource):
    """Adds a postgres datasource to the data context.

    Args:
        name: The name of this postgres datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "postgresql+psycopg2://postgres:@localhost/test_database"
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """

    type: Literal["postgres"] = "postgres"  # type: ignore[assignment]
    connection_string: PostgresDsn
