from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Final, Literal, Optional, Type, Union

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import AnyUrl, errors
from great_expectations.compatibility.snowflake import URL
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
    _check_config_substitutions_needed,
)
from great_expectations.datasource.fluent.sql_datasource import (
    FluentBaseModel,
    SQLDatasource,
    SQLDatasourceError,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.compatibility.pydantic.networks import Parts
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)


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
    @override
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


class ConnectionDetails(FluentBaseModel):
    """
    Information needed to connect to a Snowflake database.
    Alternative to a connection string.
    """

    account: str
    user: str
    password: Union[ConfigStr, str]
    database: Optional[str] = None
    schema_: Optional[str] = pydantic.Field(
        None, alias="schema"
    )  # schema is a reserved attr in BaseModel
    warehouse: Optional[str] = None
    role: Optional[str] = None
    numpy: bool = False


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
    # TODO: rename this to `connection` for v1?
    connection_string: Union[ConnectionDetails, ConfigStr, SnowflakeDsn]  # type: ignore[assignment] # Deviation from parent class as individual args are supported for connection

    # TODO: add props for account, user, password, etc?

    @pydantic.root_validator(pre=True)
    def _convert_root_connection_detail_fields(cls, values: dict) -> dict:
        """
        Convert root level connection detail fields to a ConnectionDetails compatible object.
        This preserves backwards compatibility with the previous implementation of SnowflakeDatasource.

        It also allows for users to continue to provide connection details in the
        `context.sources.add_snowflake()` factory functions without nesting it in a
        `connection_string` dict.
        """
        connection_detail_fields: set[str] = {
            "schema",  # field name in ConnectionDetails is schema_ (with underscore)
            *ConnectionDetails.__fields__.keys(),
        }

        connection_details = {}
        for field_name in tuple(values.keys()):
            if field_name in connection_detail_fields:
                connection_details[field_name] = values.pop(field_name)
        if connection_details:
            values["connection_string"] = connection_details
        return values

    @pydantic.root_validator
    def _check_xor_input_args(cls, values: dict) -> dict:
        # keeping this validator isn't strictly necessary, but it provides a better error message
        connection_string: str | ConnectionDetails | None = values.get(
            "connection_string"
        )
        if connection_string:
            # Method 1 - connection string
            if isinstance(connection_string, (str, ConfigStr)):
                return values
            # Method 2 - individual args (account, user, and password are bare minimum)
            elif isinstance(connection_string, ConnectionDetails) and bool(
                connection_string.account
                and connection_string.user
                and connection_string.password
            ):
                return values
        raise ValueError(
            "Must provide either a connection string or a combination of account, user, and password."
        )

    class Config:
        @staticmethod
        def schema_extra(schema: dict, model: type[SnowflakeDatasource]) -> None:
            """
            Customize jsonschema for SnowflakeDatasource.
            https://docs.pydantic.dev/1.10/usage/schema/#schema-customization
            Change connection_string to be a string or a dict, but not both.
            """
            connection_string_prop = schema["properties"]["connection_string"].pop(
                "anyOf"
            )
            schema["properties"]["connection_string"].update(
                {"oneOf": connection_string_prop}
            )

    def _get_connect_args(self) -> dict[str, str | bool]:
        excluded_fields: set[str] = set(SQLDatasource.__fields__.keys())
        # dump as json dict to force serialization of things like AnyUrl
        return self._json_dict(exclude=excluded_fields, exclude_none=True)

    @override
    def get_execution_engine(self) -> SqlAlchemyExecutionEngine:
        """
        Overrides get_execution_engine in Datasource
        Standard behavior is to assume all top-level Datasource config (unless part of `cls._EXTRA_EXCLUDED_EXEC_ENG_ARGS`)
        should be passed to the GX ExecutionEngine constructor.

        For SQLAlchemy this would lead to creating 2 different `sqlalchemy.engine.Engine` objects
        one for the Datasource and one for the ExecutionEngine. This is wasteful and causes multiple connections to
        the database to be created.

        For Snowflake specifically we may represent the connection_string as a dict, which is not supported by SQLAlchemy.
        """
        gx_execution_engine_type: Type[
            SqlAlchemyExecutionEngine
        ] = self.execution_engine_type

        connection_string: str | None = (
            self.connection_string if isinstance(self.connection_string, str) else None
        )

        gx_exec_engine = gx_execution_engine_type(
            self.name,
            connection_string=connection_string,
            engine=self.get_engine(),
            create_temp_table=self.create_temp_table,
            data_context=self._data_context,
        )
        self._execution_engine = gx_exec_engine
        return gx_exec_engine

    @override
    def get_engine(self) -> sqlalchemy.Engine:
        if self.connection_string != self._cached_connection_string or not self._engine:
            try:
                model_dict = self.dict(
                    exclude=self._get_exec_engine_excludes(),
                    config_provider=self._config_provider,
                )
                _check_config_substitutions_needed(
                    self, model_dict, raise_warning_if_provider_not_present=True
                )

                kwargs = model_dict.pop("kwargs", {})
                connection_string: str | dict = model_dict.pop("connection_string")

                if isinstance(connection_string, str):
                    self._engine = sa.create_engine(connection_string, **kwargs)
                else:
                    self._engine = self._build_engine_with_connect_args(
                        **connection_string
                    )

            except Exception as e:
                # connection_string has passed pydantic validation, but still fails to create a sqlalchemy engine
                # one possible case is a missing plugin (e.g. psycopg2)
                raise SQLDatasourceError(
                    "Unable to create a SQLAlchemy engine due to the "
                    f"following exception: {e!s}"
                ) from e
            # Since a connection string isn't strictly required for Snowflake, we conditionally cache
            if isinstance(self.connection_string, str):
                self._cached_connection_string = self.connection_string
        return self._engine

    def _build_engine_with_connect_args(self, **kwargs) -> sqlalchemy.Engine:
        connect_args = self._get_connect_args()
        connect_args.update(kwargs)
        url = URL(**connect_args)
        return sa.create_engine(url)
