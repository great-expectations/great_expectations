from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union

from great_expectations_v1._docs_decorators import public_api
from great_expectations_v1.datasource.fluent.sql_datasource import SQLDatasource

if TYPE_CHECKING:
    from great_expectations_v1.compatibility.pydantic import PostgresDsn
    from great_expectations_v1.datasource.fluent.config_str import ConfigStr


@public_api
class PostgresDatasource(SQLDatasource):
    """Adds a postgres datasource to the data context.

    Args:
        name: The name of this postgres datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "postgresql+psycopg2://postgres:@localhost/test_database"
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """  # noqa: E501

    type: Literal["postgres"] = "postgres"  # type: ignore[assignment]
    connection_string: Union[ConfigStr, PostgresDsn]
