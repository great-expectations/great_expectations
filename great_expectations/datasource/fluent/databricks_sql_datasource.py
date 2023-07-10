from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union

from pydantic import AnyUrl

from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.sql_datasource import (
    SQLDatasource,
    SQLDatasourceError,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.datasource.fluent.interfaces import _ExecutionEngineT


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
    # connect args
    http_path: str

    @classmethod
    def _get_exec_engine_excludes(cls) -> set[str]:
        sql_datasource_fields: set[str] = set(SQLDatasource.__fields__.keys())
        databricks_fields: set[str] = set(DatabricksSQLDatasource.__fields__.keys())
        databricks_specific_exclude = databricks_fields.difference(
            sql_datasource_fields
        )
        databricks_specific_exclude.update(
            {"id", "type", "assets"}
        )  # this probably shouldnt be hard coded
        return databricks_specific_exclude

    def _get_connect_args(self) -> dict[str, str | bool]:
        excluded_fields: set[str] = set(SQLDatasource.__fields__.keys())
        # dump as json dict to force serialization of things like AnyUrl
        return self._json_dict(exclude=excluded_fields, exclude_none=True)

    def get_execution_engine(self) -> _ExecutionEngineT:
        current_execution_engine_kwargs = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
        )
        if (
            current_execution_engine_kwargs != self._cached_execution_engine_kwargs
            or not self._execution_engine
        ):
            connect_args = self._get_connect_args()
            if connect_args:
                current_execution_engine_kwargs["connect_args"] = connect_args
            self._execution_engine = self._execution_engine_type()(
                **current_execution_engine_kwargs
            )
            self._cached_execution_engine_kwargs = current_execution_engine_kwargs
        return self._execution_engine

    def get_engine(self) -> sqlalchemy.Engine:
        # todo: this won't rebuild engine if `http_path` is changed
        if self.connection_string != self._cached_connection_string or not self._engine:
            try:
                model_dict = self.dict(
                    exclude=self._get_exec_engine_excludes(),
                    config_provider=self._config_provider,
                )
                connection_string = model_dict.pop("connection_string")
                kwargs = model_dict.pop("kwargs", {})

                connect_args = self._get_connect_args()
                if connect_args:
                    kwargs["connect_args"] = connect_args

                self._engine = sa.create_engine(connection_string, **kwargs)
            except Exception as e:
                # connection_string has passed pydantic validation, but still fails to create a sqlalchemy engine
                # one possible case is a missing plugin (e.g. psycopg2)
                raise SQLDatasourceError(
                    "Unable to create a SQLAlchemy engine from "
                    f"connection_string: {self.connection_string} due to the "
                    f"following exception: {str(e)}"
                ) from e
            self._cached_connection_string = self.connection_string
        return self._engine
