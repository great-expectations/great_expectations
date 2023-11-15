from __future__ import annotations

import logging
from typing import TYPE_CHECKING, ClassVar, Final, Literal, Optional, Union

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
    # TODO: rename this to `connection`?
    connection_string: Union[ConnectionDetails, ConfigStr, SnowflakeDsn]  # type: ignore[assignment] # Deviation from parent class as individual args are supported for connection

    _EXTRA_EXCLUDED_EXEC_ENG_ARGS: ClassVar[set] = {
        "role",
        "account",
        "schema_",
        "database",
        "user",
        "password",
        "numpy",
        "warehouse",
    }

    @pydantic.root_validator(pre=True)
    def _convert_root_connection_detail_fields(cls, values: dict) -> dict:
        """
        Convert root level connection detail fields to a ConnectionDetails compatible object.
        """
        connection_detail_fields = {
            "account",
            "user",
            "password",
            "database",
            "schema_",
            "warehouse",
            "role",
            "numpy",
        }
        connection_details = {}
        for field in connection_detail_fields:
            if field in values:
                connection_details[field] = values.pop(field)
        if connection_details:
            values["connection_string"] = connection_details
        return values

    @pydantic.root_validator
    def _check_xor_input_args(cls, values: dict) -> dict:
        # keeping this validator isn't strictly necessary, but it provides a better error message
        connection_string: str | ConnectionDetails = values.get("connection_string")
        if connection_string:
            # Method 1 - connection string
            if isinstance(connection_string, str):
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
                    "Unable to create a SQLAlchemy engine from "
                    f"connection_string: {self.connection_string} due to the "
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
