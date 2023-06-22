from typing import Literal, Union

from pydantic import AnyUrl

from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource


class SnowflakeDsn(AnyUrl):
    allowed_schemes = {
        "snowflake",
    }


@public_api
class SnowflakeDatasource(SQLDatasource):
    """Adds a Snowflake datasource to the data context.

    Args:
        name: The name of this Snowflake datasource.
        connection_string: The SQLAlchemy connection string used to connect to the Snowflake database.
            For example: "snowflake://<user_login_name>:<password>@<account_identifier>"
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """

    type: Literal["snowflake"] = "snowflake"  # type: ignore[assignment]
    connection_string: Union[ConfigStr, SnowflakeDsn]
