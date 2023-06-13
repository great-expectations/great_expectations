from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

import pydantic
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


class RedshiftDsn(pydantic.PostgresDsn):
    allowed_schemes = {
        "postgres",
        "postgresql",
        "postgresql+asyncpg",
        "postgresql+pg8000",
        "postgresql+psycopg",
        "postgresql+psycopg2",
        "postgresql+psycopg2cffi",
        "postgresql+py-postgresql",
        "postgresql+pygresql",
        "redshift",
        "redshift+redshift_connector",
        "redshift+psycopg2",
    }


@public_api
class Redshift(SQLDatasource):
    """Adds a redshift datasource to the data context.

    Args:
        name: The name of this postgres datasource.
        connection_string: The SQLAlchemy connection string used to connect to the postgres database.
            For example: "redshift+redshift_connector://redshift:@localhost/test_database"
        connect_args: An optional dictionary of connection args for redshift which is passed to
            sqlalchemy `create_engine()`.
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose values
            are TableAsset or QueryAsset objects.
    """

    type: Literal["redshift"] = "redshift"  # type: ignore[assignment] # base class is expecting 'sql'
    connection_string: Union[ConfigStr, RedshiftDsn]
    # connect_args
    iam: bool = True
    credentials_provider: Optional[str] = None
    idp_host: Optional[AnyUrl] = None
    app_id: Optional[str] = None
    app_name: Optional[str] = None
    region: Optional[str] = None
    cluster_identifier: Optional[str] = None
    ssl_insecure: bool = False

    @classmethod
    def _get_exec_engine_excludes(cls) -> set[str]:
        sql_datasource_fields: set[str] = set(SQLDatasource.__fields__.keys())
        redshift_fields: set[str] = set(Redshift.__fields__.keys())
        return redshift_fields.difference(sql_datasource_fields)

    def _get_connect_args(self) -> dict[str, str | bool]:
        excluded_fields: set[str] = set(SQLDatasource.__fields__.keys())
        # dump as json dict to force serialization of things like AnyUrl
        return self._json_dict(exclude=excluded_fields, exclude_none=True)

    def get_engine(self) -> sqlalchemy.Engine:
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
