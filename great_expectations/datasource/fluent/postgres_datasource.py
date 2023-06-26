from typing import Literal, Union

from pydantic import PostgresDsn

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource


@public_api
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
    connection_string: Union[ConfigStr, PostgresDsn]
