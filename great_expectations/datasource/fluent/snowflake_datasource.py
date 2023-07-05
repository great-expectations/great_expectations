from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Optional, Union

import pydantic
from pydantic import AnyUrl, errors

from great_expectations.compatibility.snowflake import URL
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.sql_datasource import (
    SQLDatasource,
    SQLDatasourceError,
)

if TYPE_CHECKING:
    from pydantic.networks import Parts

    from great_expectations.compatibility import sqlalchemy


class _UrlPasswordError(pydantic.UrlError):
    """
    Custom Pydantic error for missing password in SnowflakeDsn.
    """

    code = "url.password"
    msg_template = "URL password invalid"


class _UrlDomainError(pydantic.UrlError):
    """
    Custom Pydantic error for missing domain in SnowflakeDsn.
    """

    code = "url.domain"
    msg_template = "URL domain invalid"


class SnowflakeDsn(AnyUrl):
    allowed_schemes = {
        "snowflake",
    }

    @classmethod
    def validate_parts(cls, parts: Parts, validate_port: bool = True) -> Parts:
        """
        Overridden to validate additional fields outside of scheme (which is performed by AnyUrl).
        """
        user = parts["user"]
        if user is None:
            raise errors.UrlUserInfoError()

        password = parts["password"]
        if password is None:
            raise _UrlPasswordError()

        domain = parts["domain"]
        if domain is None:
            raise _UrlDomainError()

        return AnyUrl.validate_parts(parts=parts, validate_port=validate_port)


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
    connection_string: Optional[Union[ConfigStr, SnowflakeDsn]] = None  # type: ignore[assignment] # Deviation from parent class as individual args are supported for connection
    # connect_args
    account: Optional[str] = None
    user: Optional[str] = None
    password: Optional[Union[ConfigStr, str]] = None
    database: Optional[str] = None
    schema_: Optional[str] = pydantic.Field(
        None, alias="schema"
    )  # schema is a reserved attr in BaseModel
    warehouse: Optional[str] = None
    role: Optional[str] = None
    numpy: bool = False

    @pydantic.root_validator
    def _check_xor_input_args(cls, values: dict) -> dict:
        # Method 1 - connection string
        connection_string = values.get("connection_string")
        # Method 2 - individual args (account, user, and password are bare minimum)
        account = values.get("account")
        user = values.get("user")
        password = values.get("password")

        if not bool(connection_string) ^ bool(account and user and password):
            raise ValueError(
                "Must provide either a connection string or a combination of account, user, and password."
            )
        return values

    @classmethod
    def _get_exec_engine_excludes(cls) -> set[str]:
        sql_datasource_fields: set[str] = set(SQLDatasource.__fields__.keys())
        snowflake_fields: set[str] = set(SnowflakeDatasource.__fields__.keys())
        return snowflake_fields.difference(sql_datasource_fields)

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

                kwargs = model_dict.pop("kwargs", {})
                connection_string = model_dict.pop("connection_string")

                if connection_string:
                    self._engine = sa.create_engine(connection_string, **kwargs)
                else:
                    self._engine = self._build_engine_with_connect_args(**kwargs)

            except Exception as e:
                # connection_string has passed pydantic validation, but still fails to create a sqlalchemy engine
                # one possible case is a missing plugin (e.g. psycopg2)
                raise SQLDatasourceError(
                    "Unable to create a SQLAlchemy engine from "
                    f"connection_string: {self.connection_string} due to the "
                    f"following exception: {str(e)}"
                ) from e
            # Since a connection string isn't strictly required for Snowflake, we conditionally cache
            if self.connection_string:
                self._cached_connection_string = self.connection_string
        return self._engine

    def _build_engine_with_connect_args(self, **kwargs) -> sqlalchemy.Engine:
        connect_args = self._get_connect_args()
        connect_args.update(kwargs)
        url = URL(**connect_args)
        return sa.create_engine(url)
