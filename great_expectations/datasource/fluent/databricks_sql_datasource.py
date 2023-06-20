from typing import Literal


from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource


@public_api
class DatabricksSQLDatasource(SQLDatasource):
    """Adds a DatabricksSQL datasource to the data context.

    Args:
        name: The name of this DatabricksSQL datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "databricks+connector://token:<token>@<host>:<port>/<database>"
        http_path: HTTP path of Databricks SQL endpoint
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """

    type: Literal["databricks_sql"] = "databricks_sql"  # type: ignore[assignment]
    connection_string: ConfigStr
    # connect args
    http_path: str
