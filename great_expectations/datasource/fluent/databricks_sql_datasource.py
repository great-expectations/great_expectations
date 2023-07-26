from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union
from urllib import parse

import pydantic
from pydantic import AnyUrl

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.sql_datasource import (
    SQLDatasource,
)

if TYPE_CHECKING:
    from pydantic.networks import Parts

    from great_expectations.compatibility import sqlalchemy


def _parse_param_from_query_string(param: str, query: str) -> str | None:
    url_components = parse.urlparse(query)
    path = str(url_components.path)
    parse_results: dict[str, list[str]] = parse.parse_qs(path)
    path_results = parse_results.get(param, [])

    if not path_results:
        return None
    if len(path_results) > 1:
        raise ValueError(f"Only one `{param}` query entry is allowed")
    return path_results[0]


class _UrlQueryError(pydantic.UrlError):
    """
    Custom Pydantic error for missing query in DatabricksDsn.
    """

    code = "url.query"
    msg_template = "URL query is invalid or missing"


class _UrlHttpPathError(pydantic.UrlError):
    """
    Custom Pydantic error for missing http_path in DatabricksDsn query.
    """

    code = "url.query.http_path"
    msg_template = "'http_path' query param is invalid or missing"


class _UrlCatalogError(pydantic.UrlError):
    """
    Custom Pydantic error for missing catalog in DatabricksDsn query.
    """

    code = "url.query.catalog"
    msg_template = "'catalog' query param is invalid or missing"


class _UrlSchemaError(pydantic.UrlError):
    """
    Custom Pydantic error for missing schema in DatabricksDsn query.
    """

    code = "url.query.schema"
    msg_template = "'schema' query param is invalid or missing"


class DatabricksDsn(AnyUrl):
    allowed_schemes = {
        "databricks",
        "databricks+connector",
    }

    @classmethod
    def validate_parts(cls, parts: Parts, validate_port: bool = True) -> Parts:
        """
        Overridden to validate additional fields outside of scheme (which is performed by AnyUrl).
        """
        query = parts["query"]
        if query is None:
            raise _UrlQueryError()

        http_path = _parse_param_from_query_string(param="http_path", query=query)
        if http_path is None:
            raise _UrlHttpPathError()

        catalog = _parse_param_from_query_string(param="catalog", query=query)
        if catalog is None:
            raise _UrlCatalogError()

        schema = _parse_param_from_query_string(param="schema", query=query)
        if schema is None:
            raise _UrlSchemaError()

        return AnyUrl.validate_parts(parts=parts, validate_port=validate_port)


@public_api
class DatabricksSQLDatasource(SQLDatasource):
    """Adds a DatabricksSQLDatasource to the data context.

    Args:
        name: The name of this DatabricksSQL datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "databricks://token:<token>@<host>:<port>/<database>?http_path=<http_path>&catalog=<catalog>&schema=<schema>""
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """

    type: Literal["databricks_sql"] = "databricks_sql"  # type: ignore[assignment]
    connection_string: Union[ConfigStr, DatabricksDsn]

    def test_connection(self, test_assets: bool = True) -> None:
        try:
            super().test_connection(test_assets)
        except TestConnectionError as e:
            nested_exception = None
            if e.__cause__ and e.__cause__.__cause__:
                nested_exception = e.__cause__.__cause__

            # Raise specific error informing how to install dependencies only if relevant
            if isinstance(nested_exception, sa.exc.NoSuchModuleError):
                raise TestConnectionError(
                    "Could not connect to Databricks - please ensure you've installed necessary dependencies with `pip install great_expectations[databricks]`."
                )
            raise e

    def _create_engine(self) -> sqlalchemy.Engine:
        model_dict = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
        )
        connection_string = model_dict.pop("connection_string")
        kwargs = model_dict.pop("kwargs", {})

        http_path = _parse_param_from_query_string(
            param="http_path", query=connection_string.query
        )
        assert (
            http_path
        ), "Presence of http_path query string is guaranteed due to prior validation"

        # Databricks connection is a bit finicky - the http_path portion of the connection string needs to be passed in connect_args
        connect_args = {"http_path": http_path}
        return sa.create_engine(connection_string, connect_args=connect_args, **kwargs)
