from __future__ import annotations

from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Final, Literal, Optional, Union
from urllib.parse import quote, quote_plus

from typing_extensions import Annotated

from great_expectations._docs_decorators import public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import Field
from great_expectations.compatibility.pyodbc import pyodbc
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.sql_datasource import (
    FluentBaseModel,
    SQLDatasource,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine


class SqlServerDsn(pydantic.AnyUrl):
    allowed_schemes = {"mssql+pyodbc"}
    host_required = True

    @classmethod
    def from_url(cls, url: str) -> SqlServerDsn:
        """Validate and return a SqlServerDsn from a raw URL string."""

        class _Model(pydantic.BaseModel):
            url: SqlServerDsn

        return _Model(url=url).url  # type: ignore[arg-type] # pydantic coerces str to SqlServerDsn


_MUTUALLY_EXCLUSIVE_MSG: Final[str] = (
    "Provide either a connection_string object or individual keyword arguments, not both."
)

_ENCRYPT_VALUE_MAP: Final[dict[str, str]] = {
    "Mandatory": "yes",
    "Optional": "no",
    "Strict": "strict",
}


def _resolve_config_str(value: Union[ConfigStr, str], config_provider: Any) -> str:
    """Resolve ConfigStr to str; raise ConfigStrError if config_provider is missing."""
    if isinstance(value, ConfigStr):
        if config_provider:
            return value.get_config_value(config_provider)
        raise ConfigStrError()
    return str(value)


class _SQLServerConnectionDetailsBase(FluentBaseModel):
    """Base class with common connection fields."""

    host: str
    port: int = 1433
    database: str
    schema_: str = Field(..., alias="schema")
    driver: str = "ODBC Driver 18 for SQL Server"
    encrypt: Literal["Mandatory", "Optional", "Strict"] = "Mandatory"
    trust_server_certificate: bool = False

    class Config:
        allow_population_by_field_name = (
            True  # this allows us to use the alias "schema" for the "schema_" field
        )

    @abstractmethod
    def build_connection_string(
        self,
        config_provider: Optional[Any] = None,
    ) -> SqlServerDsn:
        """Build and return a validated mssql+pyodbc URL."""


class SQLServerAuthConnectionDetails(_SQLServerConnectionDetailsBase):
    """SQL Server authentication (username/password)."""

    authentication: Literal["SQL Server"] = "SQL Server"
    username: str
    password: Union[ConfigStr, str]

    @override
    def build_connection_string(
        self,
        config_provider: Optional[Any] = None,
    ) -> SqlServerDsn:
        password = _resolve_config_str(self.password, config_provider)
        username = quote(self.username, safe="")
        password_encoded = quote(password, safe="")
        query_params = {
            "driver": quote_plus(self.driver),
            "Encrypt": _ENCRYPT_VALUE_MAP.get(self.encrypt, "yes"),
            "TrustServerCertificate": "yes" if self.trust_server_certificate else "no",
        }
        query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
        url = (
            f"mssql+pyodbc://{username}:{password_encoded}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?{query_string}"
        )
        return SqlServerDsn.from_url(url)


class EntraIDServicePrincipalAuthConnectionDetails(_SQLServerConnectionDetailsBase):
    """Entra ID Service Principal authentication."""

    authentication: Literal["Entra ID Service Principal"] = "Entra ID Service Principal"
    tenant_id: str
    client_id: str
    client_secret: Union[ConfigStr, str]

    @override
    def build_connection_string(
        self,
        config_provider: Optional[Any] = None,
    ) -> SqlServerDsn:
        client_secret = _resolve_config_str(self.client_secret, config_provider)
        client_id = quote(self.client_id, safe="")
        client_secret_encoded = quote(client_secret, safe="")
        query_params = {
            "driver": quote_plus(self.driver),
            "Encrypt": _ENCRYPT_VALUE_MAP.get(self.encrypt, "yes"),
            "TrustServerCertificate": "yes" if self.trust_server_certificate else "no",
            "authentication": "ActiveDirectoryServicePrincipal",
            "TenantId": self.tenant_id,
            "UID": client_id,
            "PWD": client_secret_encoded,
        }
        query_string = "&".join(f"{k}={v}" for k, v in query_params.items())
        url = f"mssql+pyodbc://@{self.host}:{self.port}/{self.database}?{query_string}"
        return SqlServerDsn.from_url(url)


# Discriminated union using the authentication field (Pydantic v1 syntax)
SQLServerConnectionDetails = Annotated[
    Union[
        SQLServerAuthConnectionDetails,
        EntraIDServicePrincipalAuthConnectionDetails,
    ],
    Field(discriminator="authentication"),
]


_CONNECTION_DETAIL_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "schema",  # alias for schema_
        *_SQLServerConnectionDetailsBase.__fields__.keys(),
        *SQLServerAuthConnectionDetails.__fields__.keys(),
        *EntraIDServicePrincipalAuthConnectionDetails.__fields__.keys(),
    }
)


@public_api
class SQLServerDatasource(SQLDatasource):
    """Adds a SQL Server datasource to the data context.

    Args:
        name: The name of this SQL Server datasource.
        host: The environment where the Microsoft SQL Server engine is installed and running,
            for example "sql-server.example.com" for self-hosted Microsoft SQL Server.
        database: The name of the Microsoft SQL Server database
            where the data you want to validate is stored.
        schema: The name of the Microsoft SQL Server schema
            where the data you want to validate is stored.
        port: The port configured for your Microsoft SQL Server instance,
            typically 1433.
        encrypt: The TLS encryption protocol to use.
            Accepts the following.
            - "Optional" - Establish an encrypted connection if your
            Microsoft SQL Server instance is configured to force encryption.
            Otherwise, establish an unencrypted connection.
            - "Mandatory" - Require the connection to be encrypted.
            Validate the server certificate unless "trust_server_certificate" is set to "True".
            Connection will fail if your Microsoft SQL Server instance does not support TLS.
            If "trust_server_certificate" is set to "False", connection will fail if
            the certificate is not valid and publicly trusted.
            - "Strict" - Use TDS 8.0 where encryption begins before the TLS handshake.
            Require the connection to be encrypted and validate the server certificate.
            Connection will fail if your Microsoft SQL Server instance does not support TLS
            or the certificate is not valid and publicly trusted.
        trust_server_certificate: If you set "encrypt" to "Mandatory", you can set
            "trust_server_certificate" to "True" to enable using an encrypted connection
            without a valid publicly trusted server certificate (default is "False"). This
            lets you, for example, use a self-signed certificate with an encrypted connection.
        driver: The name of the ODBC driver your environment uses to connect to
            Microsoft SQL Server. Common values include the following:
            - "ODBC Driver 18 for SQL Server"
            - "ODBC Driver 17 for SQL Server"
            - "FreeTDS"
        authentication: Accepts "SQL Server" or "Entra ID".
            Required credential parameters depend on the authentication method.
        username: For "SQL Server" authentication.
            The username you use to access Microsoft SQL Server.
        password: For "SQL Server" authentication.
            The password you use to access Microsoft SQL Server.
        tenant_id: For "Entra ID" authentication.
            The unique identifier for your organization's instance of Microsoft Entra ID.
        client_id: For "Entra ID" authentication.
            The application ID for your new or existing Entra ID app registration.
        client_secret: For "Entra ID" authentication.
            A new secret key from your Entra ID app registration.
        assets: An optional dictionary whose keys are TableAsset or QueryAsset names and whose
            values are TableAsset or QueryAsset objects.
    """

    type: Literal["sql_server"] = "sql_server"  # type: ignore[assignment]
    connection_string: SQLServerConnectionDetails  # type: ignore[assignment]  # Raw connection strings are not supported

    @pydantic.root_validator(pre=True)
    def _convert_root_connection_detail_fields(cls, values: dict[str, Any]) -> dict[str, Any]:
        """Pack top-level connection detail kwargs into ``connection_string``."""
        connection_string = values.get("connection_string")
        connection_details: dict[str, Any] = {}
        for field_name in list(values.keys()):
            if field_name in _CONNECTION_DETAIL_FIELDS:
                if connection_string is not None:
                    raise ValueError(_MUTUALLY_EXCLUSIVE_MSG)
                connection_details[field_name] = values.pop(field_name)
        if connection_details:
            connection_details.setdefault("authentication", "SQL Server")
            values["connection_string"] = connection_details
        return values

    @property
    @override
    def schema_(self) -> str:
        return self.connection_string.schema_

    @override
    def get_execution_engine(self) -> SqlAlchemyExecutionEngine:
        current_execution_engine_kwargs = self.dict(
            exclude=self._get_exec_engine_excludes(),
            config_provider=self._config_provider,
            exclude_unset=False,
        )
        if (
            current_execution_engine_kwargs != self._cached_execution_engine_kwargs
            or not self._execution_engine
        ):
            # Copy before the pops below, which mutate the dict they are taken from. The
            # cached copy must keep the "kwargs" and "connection_string" keys so that it
            # compares equal to the next freshly computed dict. Caching the same object
            # left the cache permanently missing both, so the comparison never matched
            # and every call built a new execution engine and SQLAlchemy engine.
            cached_execution_engine_kwargs = dict(current_execution_engine_kwargs)
            engine_kwargs = current_execution_engine_kwargs.pop("kwargs", {})
            current_execution_engine_kwargs.pop("connection_string", None)
            engine = self._create_engine()
            self._execution_engine = self._execution_engine_type()(
                engine=engine,
                **current_execution_engine_kwargs,
                **engine_kwargs,
            )
            # Cache only once the engine exists. Caching first would leave a failed
            # rebuild holding kwargs no engine was ever built from, so the next call
            # would compare equal and return the engine from the previous configuration
            # instead of retrying.
            self._cached_execution_engine_kwargs = cached_execution_engine_kwargs
        return self._execution_engine

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        try:
            super().test_connection(test_assets=test_assets)
        except TestConnectionError as e:
            if isinstance(e.cause, (pyodbc.OperationalError, sa.exc.OperationalError)):
                if "Login" in str(e.cause):
                    if isinstance(
                        self.connection_string, EntraIDServicePrincipalAuthConnectionDetails
                    ):
                        raise MSSQLPrincipalAuthError(e.cause) from e
                    else:
                        raise MSSQLPasswordAuthError(e.cause) from e
                else:
                    raise MSSQLNetworkError(e.cause) from e
            elif isinstance(
                e.cause, (pyodbc.InterfaceError, sa.exc.DBAPIError)
            ) and "file not found" in str(e.cause):
                raise MissingODBCDriverError(e.cause) from e
            else:
                raise

    @override
    def _create_engine(self) -> sqlalchemy.Engine:
        url = self._build_connection_string()
        return sa.create_engine(url, **self.kwargs)

    def _build_connection_string(self) -> SqlServerDsn:
        """Convert connection details to a validated ``mssql+pyodbc://`` URL."""
        return self.connection_string.build_connection_string(config_provider=self._config_provider)


class ConfigStrError(ValueError):
    """Raised when a connection test fails due to invalid configuration."""

    def __init__(self) -> None:
        super().__init__(
            "ConfigStr value provided, but no config provider is set on the datasource."
        )


class MSSQLNetworkError(TestConnectionError):
    """Raised when a connection test fails due to a network error."""

    def __init__(self, cause: pyodbc.OperationalError) -> None:
        super().__init__(
            cause=cause,
            message=" ".join(
                [
                    "Unable to connect to the database server.",
                    "Verify the host and port are correct and the server is accessible.",
                ]
            ),
        )


class MSSQLPasswordAuthError(TestConnectionError):
    """Raised when a connection test fails due to an authentication error."""

    def __init__(self, cause: pyodbc.OperationalError) -> None:
        super().__init__(
            cause=cause,
            message="Authentication failed. Verify your username and password.",
        )


class MSSQLPrincipalAuthError(TestConnectionError):
    """Raised when a connection test fails due to an authentication error."""

    def __init__(self, cause: pyodbc.OperationalError) -> None:
        super().__init__(
            cause=cause,
            message="Azure AD authentication failed. Verify your client ID, secret, and tenant ID.",
        )


class MissingODBCDriverError(TestConnectionError):
    """Raised when a connection test fails due to a missing ODBC driver."""

    def __init__(self, cause: pyodbc.OperationalError) -> None:
        super().__init__(
            cause=cause, message="ODBC driver not found. Ensure the specified driver is installed."
        )
