from __future__ import annotations

import functools
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from great_expectations.compatibility.pydantic import ValidationError
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.datasource.fluent.config_str import ConfigStr
from great_expectations.datasource.fluent.interfaces import TestConnectionError
from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset
from great_expectations.datasource.fluent.sql_server_datasource import (
    EntraIDServicePrincipalAuthConnectionDetails,
    MissingODBCDriverError,
    MSSQLNetworkError,
    MSSQLPasswordAuthError,
    MSSQLPrincipalAuthError,
    SQLServerAuthConnectionDetails,
    SQLServerDatasource,
    SqlServerDsn,
)

if TYPE_CHECKING:
    from typing import Callable, Union

    from typing_extensions import TypeAlias

    from great_expectations.data_context import AbstractDataContext

ConnectionDetailsDict: TypeAlias = dict[str, Any]


@pytest.fixture
def connection_details_default() -> ConnectionDetailsDict:
    return {
        "host": "myserver.database.windows.net",
        "port": 1433,
        "database": "mydb",
        "schema": "dbo",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "Mandatory",
        "username": "myuser",
        "password": "mypassword",
    }


@pytest.fixture
def connection_details_encrypt_optional() -> ConnectionDetailsDict:
    return {
        "host": "host",
        "port": 1433,
        "database": "db",
        "schema": "dbo",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "Optional",
        "username": "u",
        "password": "p",
    }


@pytest.fixture
def connection_details_encrypt_strict() -> ConnectionDetailsDict:
    return {
        "host": "host",
        "port": 1433,
        "database": "db",
        "schema": "dbo",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "Strict",
        "username": "u",
        "password": "p",
    }


@pytest.fixture
def connection_details_special_chars() -> ConnectionDetailsDict:
    return {
        "host": "host",
        "port": 1433,
        "database": "db",
        "schema": "dbo",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "Mandatory",
        "username": "user",
        "password": "p@ss:w/rd",
    }


@pytest.fixture
def connection_details_trust_server_certificate() -> ConnectionDetailsDict:
    return {
        "host": "host",
        "port": 1433,
        "database": "db",
        "schema": "dbo",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "Mandatory",
        "trust_server_certificate": True,
        "username": "u",
        "password": "p",
    }


@pytest.fixture
def entra_id_service_principal_connection_details_default() -> ConnectionDetailsDict:
    return {
        "host": "myserver.database.windows.net",
        "port": 1433,
        "database": "mydb",
        "schema": "dbo",
        "driver": "ODBC Driver 18 for SQL Server",
        "encrypt": "Mandatory",
        "authentication": "Entra ID Service Principal",
        "tenant_id": "my-tenant-id-456",
        "client_id": "my-client-id-123",
        "client_secret": "my-secret",
    }


@pytest.mark.unit
class TestSQLServerAuthConnectionDetails:
    def test_create_with_defaults(self) -> None:
        details = SQLServerAuthConnectionDetails(
            host="myserver",
            database="mydb",
            schema="dbo",
            username="myuser",
            password="mypassword",
        )
        assert details.dict(by_alias=True, exclude_unset=False) == {
            "host": "myserver",
            "port": 1433,
            "database": "mydb",
            "schema": "dbo",
            "driver": "ODBC Driver 18 for SQL Server",
            "encrypt": "Mandatory",
            "trust_server_certificate": False,
            "authentication": "SQL Server",
            "username": "myuser",
            "password": "mypassword",
        }

    def test_password_accepts_config_str(self) -> None:
        details = SQLServerAuthConnectionDetails(
            host="myserver",
            database="mydb",
            schema="dbo",
            username="myuser",
            password="${MY_PASSWORD}",
        )
        assert isinstance(details.password, ConfigStr)
        assert str(details.password) == "${MY_PASSWORD}"

    def test_create_with_custom_values(self) -> None:
        details = SQLServerAuthConnectionDetails(
            host="customserver",
            port=3342,
            database="customdb",
            schema="custom_schema",
            driver="ODBC Driver 17 for SQL Server",
            encrypt="Optional",
            username="admin",
            password="secret",
        )
        assert details.dict(by_alias=True, exclude_unset=False) == {
            "host": "customserver",
            "port": 3342,
            "database": "customdb",
            "schema": "custom_schema",
            "driver": "ODBC Driver 17 for SQL Server",
            "encrypt": "Optional",
            "trust_server_certificate": False,
            "authentication": "SQL Server",
            "username": "admin",
            "password": "secret",
        }


@pytest.mark.unit
class TestBuildConnectionString:
    def test_basic_connection_string(
        self, connection_details_default: ConnectionDetailsDict
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        result = ds._build_connection_string()
        assert result == (
            "mssql+pyodbc://myuser:mypassword"
            "@myserver.database.windows.net:1433/mydb"
            "?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no"
        )

    def test_encrypt_optional(
        self, connection_details_encrypt_optional: ConnectionDetailsDict
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_encrypt_optional),
        )
        result = ds._build_connection_string()
        assert "Encrypt=no" in result

    def test_trust_server_certificate(
        self, connection_details_trust_server_certificate: ConnectionDetailsDict
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(
                **connection_details_trust_server_certificate
            ),
        )
        result = ds._build_connection_string()
        assert "TrustServerCertificate=yes" in result

    def test_encrypt_strict(self, connection_details_encrypt_strict: ConnectionDetailsDict) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_encrypt_strict),
        )
        result = ds._build_connection_string()
        assert "Encrypt=strict" in result

    def test_special_chars_in_password_are_encoded(
        self, connection_details_special_chars: ConnectionDetailsDict
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_special_chars),
        )
        result = ds._build_connection_string()
        assert "p%40ss%3Aw%2Frd" in result


@pytest.mark.unit
class TestSQLServerDatasource:
    def test_type_literal(self, connection_details_default: ConnectionDetailsDict) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        assert ds.type == "sql_server"

    def test_schema_property(self, connection_details_default: ConnectionDetailsDict) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        assert ds.schema_ == "dbo"

    def test_sql_server_dsn_rejects_non_pyodbc_scheme(self) -> None:
        with pytest.raises(ValidationError, match="URL scheme not permitted"):
            SqlServerDsn.from_url("mssql+pymssql://user:pass@host:1433/db")

    @pytest.mark.usefixtures("create_engine_fake")
    def test_get_engine_calls_create_engine(
        self,
        connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        engine = ds.get_engine()
        assert engine is not None

    @pytest.mark.usefixtures("create_engine_fake")
    def test_get_engine_caches_engine(
        self,
        connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        engine1 = ds.get_engine()
        engine2 = ds.get_engine()
        assert engine1 is engine2

    @pytest.mark.usefixtures("create_engine_fake")
    def test_get_execution_engine_caches_execution_engine(
        self,
        connection_details_default: ConnectionDetailsDict,
    ) -> None:
        """This override computes its own cache key, so it needs its own coverage.

        Every validation goes through ``get_execution_engine``. If the cache never hits, each
        validation builds a new SQLAlchemy engine and connection pool, and SQL Server holds a
        connection open per engine rather than returning it to the pool after each use.
        """
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )

        first = ds.get_execution_engine()
        second = ds.get_execution_engine()

        assert second is first

    @pytest.mark.usefixtures("create_engine_fake")
    def test_changed_config_rebuilds_the_execution_engine(
        self,
        connection_details_default: ConnectionDetailsDict,
    ) -> None:
        """Caching must not outlive the configuration the engine was built from."""
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )

        first = ds.get_execution_engine()
        ds.create_temp_table = not ds.create_temp_table

        assert ds.get_execution_engine() is not first

    @pytest.mark.usefixtures("create_engine_fake")
    def test_failed_rebuild_does_not_suppress_the_next_one(
        self,
        connection_details_default: ConnectionDetailsDict,
    ) -> None:
        """A rebuild that raises must not leave the new kwargs cached against the old engine."""
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        first = ds.get_execution_engine()

        def _raise_on_construction(*args: Any, **kwargs: Any) -> None:
            raise RuntimeError("could not connect")

        ds.create_temp_table = not ds.create_temp_table
        with patch.object(SQLDatasource, "execution_engine_type", _raise_on_construction):
            with pytest.raises(RuntimeError, match="could not connect"):
                ds.get_execution_engine()

        assert ds.get_execution_engine() is not first

    @pytest.mark.usefixtures("create_engine_fake")
    def test_add_table_asset_inherits_schema_from_datasource(
        self,
        connection_details_default: ConnectionDetailsDict,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(TableAsset, "test_connection", lambda self: None)
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        asset = ds.add_table_asset(name="my_asset", table_name="my_table")
        assert asset._effective_schema_name == ds.schema_


@pytest.mark.unit
class TestEntraIDServicePrincipalAuthConnectionDetails:
    def test_create_with_defaults(self) -> None:
        details = EntraIDServicePrincipalAuthConnectionDetails(
            host="myserver.database.windows.net",
            database="mydb",
            schema="dbo",
            tenant_id="my-tenant-id-456",
            client_id="my-client-id-123",
            client_secret="my-secret",
        )
        assert details.dict(by_alias=True, exclude_unset=False) == {
            "host": "myserver.database.windows.net",
            "port": 1433,
            "database": "mydb",
            "schema": "dbo",
            "driver": "ODBC Driver 18 for SQL Server",
            "encrypt": "Mandatory",
            "trust_server_certificate": False,
            "authentication": "Entra ID Service Principal",
            "tenant_id": "my-tenant-id-456",
            "client_id": "my-client-id-123",
            "client_secret": "my-secret",
        }

    def test_client_secret_accepts_config_str(self) -> None:
        details = EntraIDServicePrincipalAuthConnectionDetails(
            host="myserver",
            database="mydb",
            schema="dbo",
            tenant_id="my-tenant-id",
            client_id="my-client-id",
            client_secret="${MY_CLIENT_SECRET}",
        )
        assert isinstance(details.client_secret, ConfigStr)
        assert str(details.client_secret) == "${MY_CLIENT_SECRET}"


@pytest.mark.unit
class TestBuildConnectionStringEntraID:
    def test_sql_server_auth_has_no_authentication_param(
        self,
        connection_details_default: ConnectionDetailsDict,
    ) -> None:
        """Ensure SQL Server auth does NOT add the Authentication query param."""
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        result = ds._build_connection_string()
        assert "Authentication=" not in result

    def test_entra_id_service_principal_connection_string(
        self,
        entra_id_service_principal_connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                **entra_id_service_principal_connection_details_default
            ),
        )
        result = ds._build_connection_string()
        assert "mssql+pyodbc://@myserver.database.windows.net:1433/mydb" in result
        assert "authentication=ActiveDirectoryServicePrincipal" in result
        assert "UID=my-client-id-123" in result
        assert "PWD=my-secret" in result

    def test_entra_id_service_principal_special_chars_in_client_secret(self) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                host="myserver.database.windows.net",
                database="mydb",
                schema="dbo",
                tenant_id="my-tenant-id",
                client_id="my-client-id",
                client_secret="p@ss:w/rd",
            ),
        )
        result = ds._build_connection_string()
        assert "p%40ss%3Aw%2Frd" in result
        assert "authentication=ActiveDirectoryServicePrincipal" in result

    def test_entra_id_service_principal_trust_server_certificate(self) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                host="myserver.database.windows.net",
                database="mydb",
                schema="dbo",
                trust_server_certificate=True,
                tenant_id="my-tenant-id",
                client_id="my-client-id",
                client_secret="secret",
            ),
        )
        result = ds._build_connection_string()
        assert "TrustServerCertificate=yes" in result
        assert "authentication=ActiveDirectoryServicePrincipal" in result

    def test_entra_id_service_principal_encrypt_optional(self) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                host="myserver.database.windows.net",
                database="mydb",
                schema="dbo",
                encrypt="Optional",
                tenant_id="my-tenant-id",
                client_id="my-client-id",
                client_secret="secret",
            ),
        )
        result = ds._build_connection_string()
        assert "Encrypt=no" in result
        assert "authentication=ActiveDirectoryServicePrincipal" in result


@pytest.mark.unit
class TestSQLServerDatasourceDiscriminatedUnion:
    def test_accepts_sql_server_auth(
        self,
        connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=SQLServerAuthConnectionDetails(**connection_details_default),
        )
        assert isinstance(ds.connection_string, SQLServerAuthConnectionDetails)

    def test_accepts_entra_id_service_principal_auth(
        self,
        entra_id_service_principal_connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                **entra_id_service_principal_connection_details_default
            ),
        )
        assert isinstance(ds.connection_string, EntraIDServicePrincipalAuthConnectionDetails)

    def test_schema_property_entra_id_service_principal(
        self,
        entra_id_service_principal_connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                **entra_id_service_principal_connection_details_default
            ),
        )
        assert ds.schema_ == "dbo"

    def test_schema_property_azure_ad_service_principal(
        self,
        entra_id_service_principal_connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                **entra_id_service_principal_connection_details_default
            ),
        )
        assert ds.schema_ == "dbo"

    @pytest.mark.usefixtures("create_engine_fake")
    def test_get_engine_entra_id_service_principal(
        self,
        entra_id_service_principal_connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                **entra_id_service_principal_connection_details_default
            ),
        )
        engine = ds.get_engine()
        assert engine is not None

    @pytest.mark.usefixtures("create_engine_fake")
    def test_get_engine_azure_ad_service_principal(
        self,
        entra_id_service_principal_connection_details_default: ConnectionDetailsDict,
    ) -> None:
        ds = SQLServerDatasource(
            name="test_ds",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                **entra_id_service_principal_connection_details_default
            ),
        )
        engine = ds.get_engine()
        assert engine is not None


@pytest.mark.unit
@pytest.mark.usefixtures("mock_test_connection")
class TestAddSQLServerDatasourceAPI:
    def test_add_sql_server_with_sql_server_auth(
        self,
        empty_data_context: AbstractDataContext,
    ) -> None:
        source = empty_data_context.data_sources.add_sql_server(
            name="my_sql_server",
            connection_string=SQLServerAuthConnectionDetails(
                host="myserver.database.windows.net",
                database="mydb",
                schema="dbo",
                username="myuser",
                password="mypassword",
            ),
        )
        assert source.dict(by_alias=True, exclude_unset=False, exclude={"id"}) == {
            "type": "sql_server",
            "name": "my_sql_server",
            "connection_string": {
                "host": "myserver.database.windows.net",
                "port": 1433,
                "database": "mydb",
                "schema": "dbo",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": "Mandatory",
                "trust_server_certificate": False,
                "authentication": "SQL Server",
                "username": "myuser",
                "password": "mypassword",
            },
            "create_temp_table": False,
            "kwargs": {},
            "assets": [],
        }

    def test_add_sql_server_with_entra_id_service_principal_auth(
        self,
        empty_data_context: AbstractDataContext,
    ) -> None:
        source = empty_data_context.data_sources.add_sql_server(
            name="my_azure_sp_sql",
            connection_string=EntraIDServicePrincipalAuthConnectionDetails(
                host="myserver.database.windows.net",
                database="mydb",
                schema="dbo",
                client_id="my-client-id-123",
                client_secret="my-secret",
                tenant_id="my-tenant-id-456",
            ),
        )
        assert source.dict(by_alias=True, exclude_unset=False, exclude={"id"}) == {
            "type": "sql_server",
            "name": "my_azure_sp_sql",
            "connection_string": {
                "host": "myserver.database.windows.net",
                "port": 1433,
                "database": "mydb",
                "schema": "dbo",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": "Mandatory",
                "trust_server_certificate": False,
                "authentication": "Entra ID Service Principal",
                "tenant_id": "my-tenant-id-456",
                "client_id": "my-client-id-123",
                "client_secret": "my-secret",
            },
            "create_temp_table": False,
            "kwargs": {},
            "assets": [],
        }

    def test_add_sql_server_with_flat_kwargs_sql_server_auth(
        self,
        empty_data_context: AbstractDataContext,
    ) -> None:
        source = empty_data_context.data_sources.add_sql_server(
            name="my_sql_server_flat",
            host="myserver.database.windows.net",
            database="mydb",
            schema="dbo",
            username="myuser",
            password="mypassword",
        )
        assert source.dict(by_alias=True, exclude_unset=False, exclude={"id"}) == {
            "type": "sql_server",
            "name": "my_sql_server_flat",
            "connection_string": {
                "host": "myserver.database.windows.net",
                "port": 1433,
                "database": "mydb",
                "schema": "dbo",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": "Mandatory",
                "trust_server_certificate": False,
                "authentication": "SQL Server",
                "username": "myuser",
                "password": "mypassword",
            },
            "create_temp_table": False,
            "kwargs": {},
            "assets": [],
        }

    def test_add_sql_server_with_flat_kwargs_entra_id_service_principal(
        self,
        empty_data_context: AbstractDataContext,
    ) -> None:
        source = empty_data_context.data_sources.add_sql_server(
            name="my_azure_sp_flat",
            host="myserver.database.windows.net",
            database="mydb",
            schema="dbo",
            authentication="Entra ID Service Principal",
            tenant_id="my-tenant-id-456",
            client_id="my-client-id-123",
            client_secret="my-secret",
        )
        assert source.dict(by_alias=True, exclude_unset=False, exclude={"id"}) == {
            "type": "sql_server",
            "name": "my_azure_sp_flat",
            "connection_string": {
                "host": "myserver.database.windows.net",
                "port": 1433,
                "database": "mydb",
                "schema": "dbo",
                "driver": "ODBC Driver 18 for SQL Server",
                "encrypt": "Mandatory",
                "trust_server_certificate": False,
                "authentication": "Entra ID Service Principal",
                "tenant_id": "my-tenant-id-456",
                "client_id": "my-client-id-123",
                "client_secret": "my-secret",
            },
            "create_temp_table": False,
            "kwargs": {},
            "assets": [],
        }

    def test_add_sql_server_flat_kwargs_rejects_connection_string_and_kwargs(
        self,
        empty_data_context: AbstractDataContext,
    ) -> None:
        with pytest.raises(ValueError, match="not both"):
            empty_data_context.data_sources.add_sql_server(  # type: ignore[call-overload]
                name="bad",
                connection_string=SQLServerAuthConnectionDetails(
                    host="h",
                    database="d",
                    schema="s",
                    username="u",
                    password="p",
                ),
                host="other_host",
            )


def with_mock_engine_raising(
    connect_exception: Union[Exception, Callable[[], Exception]],
) -> Callable:
    """Patch get_engine to return an engine whose connect() raises the given exception.

    Args:
        connect_exception: The exception to raise, or a callable that returns it
            (for lazy/conditional creation, e.g. when the exception may not be available).
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            exc = connect_exception() if callable(connect_exception) else connect_exception
            mock_engine = _make_mock_engine(exc)
            with patch.object(SQLServerDatasource, "get_engine", return_value=mock_engine):
                return func(*args, **kwargs)

        return wrapper

    return decorator


def _make_mock_engine(connect_exception: Exception):
    """Create a mock engine whose connect() raises the given exception."""

    class _MockEngine:
        def connect(self):
            raise connect_exception

    return _MockEngine()


def _get_pyodbc_login_error() -> Exception:
    """Return pyodbc.OperationalError or skip if pyodbc not available."""
    try:
        from great_expectations.compatibility import pyodbc

        return pyodbc.OperationalError("Login failed for user")  # type: ignore[attr-defined] # pyodbc is either the module or a NotImported sentinel
    except (TypeError, AttributeError):
        pytest.skip("pyodbc not installed or OperationalError not available")


@pytest.fixture
def sql_server_datasource() -> SQLServerDatasource:
    """SQL Server datasource with SQL Server auth (for non-Azure tests)."""
    return SQLServerDatasource(
        name="test_ds",
        connection_string=SQLServerAuthConnectionDetails(
            host="myserver.database.windows.net",
            database="mydb",
            schema="dbo",
            username="myuser",
            password="mypassword",
        ),
    )


@pytest.fixture
def azure_ad_service_principal_datasource() -> SQLServerDatasource:
    """SQL Server datasource with Azure AD Service Principal auth."""
    return SQLServerDatasource(
        name="test_ds",
        connection_string=EntraIDServicePrincipalAuthConnectionDetails(
            host="myserver.database.windows.net",
            database="mydb",
            schema="dbo",
            tenant_id="my-tenant-id",
            client_id="my-client-id",
            client_secret="my-secret",
        ),
    )


@pytest.mark.sql_server
class TestSQLServerDatasourceTestConnectionErrors:
    """Tests for error handling in SQLServerDatasource.test_connection."""

    @with_mock_engine_raising(
        sa.exc.OperationalError("Login failed for user 'myuser'", None, Exception())
    )
    def test_login_failure_sql_server_auth_raises_sql_password_auth_error(
        self, sql_server_datasource: SQLServerDatasource
    ) -> None:
        """OperationalError with 'Login' and SQL Server auth -> MSSQLPasswordAuthError."""
        with pytest.raises(MSSQLPasswordAuthError):
            sql_server_datasource.test_connection()

    @with_mock_engine_raising(sa.exc.OperationalError("Login failed for user", None, Exception()))
    def test_login_failure_azure_ad_service_principal_raises_sql_principal_auth_error(
        self, azure_ad_service_principal_datasource: SQLServerDatasource
    ) -> None:
        """OperationalError with 'Login' -> MSSQLPrincipalAuthError."""
        with pytest.raises(MSSQLPrincipalAuthError):
            azure_ad_service_principal_datasource.test_connection()

    @with_mock_engine_raising(
        sa.exc.OperationalError("Unable to connect: connection refused", None, Exception())
    )
    def test_network_error_raises_sql_server_network_error(
        self, sql_server_datasource: SQLServerDatasource
    ) -> None:
        """OperationalError without 'Login' -> MSSQLNetworkError."""
        with pytest.raises(MSSQLNetworkError):
            sql_server_datasource.test_connection()

    @with_mock_engine_raising(
        sa.exc.DBAPIError(
            "ODBC Driver 18 for SQL Server not found: file not found", None, Exception()
        )
    )
    def test_missing_odbc_driver_raises_missing_odbc_driver_error(
        self, sql_server_datasource: SQLServerDatasource
    ) -> None:
        """DBAPIError with 'file not found' -> MissingODBCDriverError."""
        with pytest.raises(MissingODBCDriverError):
            sql_server_datasource.test_connection()

    @with_mock_engine_raising(_get_pyodbc_login_error)
    def test_pyodbc_operational_error_login_raises_sql_password_auth_error(
        self, sql_server_datasource: SQLServerDatasource
    ) -> None:
        """pyodbc.OperationalError with 'Login' -> MSSQLPasswordAuthError."""
        with pytest.raises(MSSQLPasswordAuthError):
            sql_server_datasource.test_connection()

    @with_mock_engine_raising(ValueError("Something unexpected happened"))
    def test_unhandled_error_reraises_test_connection_error(
        self, sql_server_datasource: SQLServerDatasource
    ) -> None:
        """Unhandled exception types -> original TestConnectionError re-raised."""
        with pytest.raises(TestConnectionError) as exc_info:
            sql_server_datasource.test_connection()

        # Should re-raise the TestConnectionError that wraps the original
        assert isinstance(exc_info.value.cause, ValueError)
        assert "Something unexpected" in str(exc_info.value.cause)
