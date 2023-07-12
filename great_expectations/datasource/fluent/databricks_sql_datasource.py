from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union
from urllib import parse

from pydantic import AnyUrl

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.sql_datasource import (
    SQLDatasource,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy


class DatabricksDsn(AnyUrl):
    allowed_schemes = {
        "databricks+connector",
    }


@public_api
class DatabricksSQLDatasource(SQLDatasource):
    """Adds a DatabricksSQLDatasource to the data context.

    Args:
        name: The name of this DatabricksSQL datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "databricks+connector://token:<token>@<host>:<port>/<database>"
        http_path: HTTP path of Databricks SQL endpoint
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """

    type: Literal["databricks_sql"] = "databricks_sql"  # type: ignore[assignment]
    connection_string: Union[ConfigStr, DatabricksDsn]

    def _create_engine(self) -> sqlalchemy.Engine:
        model_dict = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
        )
        connection_string = model_dict.pop("connection_string")
        kwargs = model_dict.pop("kwargs", {})
        connect_args = {"http_path": self._get_http_path(connection_string)}
        self._engine = sa.create_engine(
            connection_string, connect_args=connect_args, **kwargs
        )

    def _get_http_path(self, connection_string: DatabricksDsn) -> dict[str, str]:
        query = connection_string.query
        url_components = parse.urlparse(query)
        parse_results = parse.parse_qs(url_components.path)

        path_result = parse_results.get("http_path", [])
        assert (
            path_result and len(path_result) == 1
        )  # TODO: Add prior validation such that this value is guaranteed

        return path_result[0]
