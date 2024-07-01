from __future__ import annotations

import base64
import functools
import logging
import re
import urllib.parse
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Final,
    Literal,
    Optional,
    Type,
    Union,
)

from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import AnyUrl, errors
from great_expectations.compatibility.snowflake import URL
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.core._docs_decorators import public_api
from great_expectations.datasource.fluent import GxContextWarning, GxDatasourceWarning
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
    _check_config_substitutions_needed,
)
from great_expectations.datasource.fluent.constants import (
    SNOWFLAKE_PARTNER_APPLICATION_CLOUD,
    SNOWFLAKE_PARTNER_APPLICATION_OSS,
)
from great_expectations.datasource.fluent.sql_datasource import (
    FluentBaseModel,
    SQLDatasource,
    SQLDatasourceError,
    TestConnectionError,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.compatibility.pydantic.networks import Parts
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

MISSING: Final = object()  # sentinel value to indicate missing values


@functools.lru_cache(maxsize=4)
def _is_b64_encoded(sb: str | bytes) -> bool:
    """
    Check if a string or bytes is base64 encoded.
    By decoding and then encoding it, we can check if it's the same.

    Copied from: https://stackoverflow.com/a/45928164/6304433
    """
    try:
        if isinstance(sb, str):
            # If there's any unicode here, an exception will be thrown and the function will return false
            sb_bytes = bytes(sb, "ascii")
        elif isinstance(sb, bytes):
            sb_bytes = sb
        else:
            raise TypeError("Argument must be string or bytes")
        return base64.b64encode(base64.b64decode(sb_bytes)) == sb_bytes
    except Exception:
        return False


@functools.lru_cache(maxsize=4)
def _get_database_and_schema_from_path(
    url_path: str,
) -> dict[Literal["schema", "database"], str | None]:
    """
    Extracts the database and schema from the path of a URL.

    snowflake://user:password@account/database/schema
    """
    try:
        _, database, schema, *_ = url_path.split("/")
    except (ValueError, AttributeError) as e:
        LOGGER.debug(f"Unable to split path - {e!r}")
        database = None
        schema = None
    return {"database": database, "schema": schema}


def _get_config_substituted_connection_string(
    datasource: SnowflakeDatasource,
    warning_msg: str = "Unable to perform config substitution",
) -> str | None:
    if not isinstance(datasource.connection_string, ConfigStr):
        raise TypeError("Config substitution is only supported for `ConfigStr`")
    if not datasource._data_context:
        warnings.warn(
            f"{warning_msg} for {datasource.connection_string.template_str}. Likely missing a context.",
            category=GxContextWarning,
        )
        return None
    return datasource.connection_string.get_config_value(
        datasource._data_context.config_provider
    )


class AccountIdentifier(str):
    """
    Custom Pydantic compatible `str` type for the account identifier in
    a `SnowflakeDsn` or `ConnectionDetails`.

    https://docs.snowflake.com/en/user-guide/admin-account-identifier
    https://docs.snowflake.com/user-guide/organizations-connect

    Expected formats:
    1. Account name in your organization
        a. `<orgname>-<account_name>` - e.g. `"myOrg-my_account"`
        b. `<orgname>-<account-name>` - e.g. `"myOrg-my-account"`
    2. Account locator in a region
        a. `<account_locator>.<region>.<cloud>` - e.g. `"abc12345.us-east-1.aws"`

    The cloud group is optional if on aws but expecting it to be there makes it easier to distinguish formats.
    GX does not throw errors based on the regex parsing result.

    The `.` separated version of the Account name format is not supported with SQLAlchemy.
    """

    FORMATS: ClassVar[
        str
    ] = "<orgname>-<account_name> or <account_locator>.<region>.<cloud>"

    FMT_1: ClassVar[
        str
    ] = r"^(?P<orgname>[a-zA-Z0-9]+)[-](?P<account_name>[a-zA-Z0-9-_]+)$"
    FMT_2: ClassVar[
        str
    ] = r"^(?P<account_locator>[a-zA-Z0-9]+)\.(?P<region>[a-zA-Z0-9-]+)\.(?P<cloud>aws|gcp|azure)$"
    PATTERN: ClassVar[re.Pattern] = re.compile(f"{FMT_1}|{FMT_2}")

    MSG_TEMPLATE: ClassVar[str] = (
        "Account identifier '{value}' does not match expected format {formats} ; it MAY be invalid. "
        "https://docs.snowflake.com/en/user-guide/admin-account-identifier"
    )

    def __init__(self, value: str) -> None:
        self._match = self.PATTERN.match(value)

    @override
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({super().__repr__()})"

    @classmethod
    def __get_validators__(cls) -> Any:
        yield cls._validate

    @classmethod
    def get_schema(cls) -> dict:
        """Can be used by Pydantic models to customize the generated jsonschema."""
        return {
            "title": cls.__name__,
            "type": "string",
            "description": f"Snowflake Account identifiers can be in one of two formats: {cls.FORMATS}",
            "pattern": cls.PATTERN.pattern,
            "examples": [
                "myOrg-my_account",
                "myOrg-my-account",
                "abc12345.us-east-1.aws",
            ],
        }

    @classmethod
    def _validate(cls, value: str) -> AccountIdentifier:
        if not value:
            raise ValueError("Account identifier cannot be empty")
        v = cls(value)
        if not v._match:
            LOGGER.info(
                cls.MSG_TEMPLATE.format(value=value, formats=cls.FORMATS),
            )
        return v

    @property
    def match(self) -> re.Match[str] | None:
        return self._match

    @property
    def orgname(self) -> str | None:
        """
        Part of format 1:
        * `<orgname>-<account_name>`
        * `<orgname>-<account-name>`
        """
        if self._match:
            return self._match.group("orgname")
        return None

    @property
    def account_name(self) -> str | None:
        """
        Part of format 1:
        * `<orgname>-<account_name>`
        * `<orgname>-<account-name>`
        """
        if self._match:
            return self._match.group("account_name")
        return None

    @property
    def account_locator(self) -> str | None:
        """Part of format 2: `<account_locator>.<region>.<cloud>`"""
        if self._match:
            return self._match.group("account_locator")
        return None

    @property
    def region(self) -> str | None:
        """Part of format 2: `<account_locator>.<region>.<cloud>`"""
        if self._match:
            return self._match.group("region")
        return None

    @property
    def cloud(self) -> Literal["aws", "gcp", "azure"] | None:
        """Part of format 2: `<account_locator>.<region>.<cloud>`"""
        if self._match:
            return self._match.group("cloud")  # type: ignore[return-value] # regex
        return None

    def as_tuple(
        self,
    ) -> tuple[str | None, str | None, str | None] | tuple[str | None, str | None]:
        fmt1 = (self.account_locator, self.region, self.cloud)
        if any(fmt1):
            return fmt1
        fmt2 = (self.orgname, self.account_name)
        if any(fmt2):
            return fmt2
        raise ValueError("Account identifier does not match either expected format")


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


class _UrlMissingQueryError(pydantic.UrlError):
    """
    Custom Pydantic error for missing query parameters in SnowflakeDsn.
    """

    def __init__(self, **ctx: Any) -> None:
        super().__init__(**ctx)

    code = "url.query"
    msg_template = "URL query param missing"


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

        validated_parts = AnyUrl.validate_parts(
            parts=parts, validate_port=validate_port
        )

        return validated_parts

    @property
    def params(self) -> dict[str, list[str]]:
        """The query parameters as a dictionary."""
        if not self.query:
            return {}
        return urllib.parse.parse_qs(self.query)

    @property
    def account_identifier(self) -> AccountIdentifier:
        """Alias for host."""
        assert self.host
        return AccountIdentifier(self.host)

    @property
    def account(self) -> AccountIdentifier:
        return self.account_identifier

    @property
    def database(self) -> str | None:
        if self.path:
            return _get_database_and_schema_from_path(self.path)["database"]
        return None

    @property
    def schema_(self) -> str | None:
        if self.path:
            return _get_database_and_schema_from_path(self.path)["schema"]
        return None

    @property
    def warehouse(self) -> str | None:
        return self.params.get("warehouse", [None])[0]

    @property
    def role(self) -> str | None:
        return self.params.get("role", [None])[0]


class ConnectionDetails(FluentBaseModel):
    """
    Information needed to connect to a Snowflake database.
    Alternative to a connection string.

    https://docs.snowflake.com/en/developer-guide/python-connector/sqlalchemy#additional-connection-parameters
    """

    account: AccountIdentifier
    user: str
    password: Union[ConfigStr, str]
    database: Optional[str] = pydantic.Field(
        None, description="Default database for the connection"
    )
    schema_: str = pydantic.Field(
        None, alias="schema", description="Default schema for the connection"
    )  # schema is a reserved attr in BaseModel
    warehouse: Optional[str] = None
    role: Optional[str] = None
    numpy: bool = False

    class Config:
        @staticmethod
        def schema_extra(schema, model) -> None:
            schema["properties"]["account"] = AccountIdentifier.get_schema()


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

    @property
    def account(self) -> AccountIdentifier | None:
        """Convenience property to get the `account` regardless of the connection string format."""
        if isinstance(self.connection_string, (ConnectionDetails, SnowflakeDsn)):
            return self.connection_string.account

        subbed_str: str | None = _get_config_substituted_connection_string(
            self, warning_msg="Unable to determine account"
        )
        if not subbed_str:
            return None
        hostname = urllib.parse.urlparse(subbed_str).hostname
        if hostname:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=GxDatasourceWarning)
            return AccountIdentifier(hostname)
        return None

    @property
    def schema_(self) -> str | None:
        """
        Convenience property to get the `schema` regardless of the connection string format.

        `schema_` to avoid conflict with Pydantic models schema property.
        """
        if isinstance(self.connection_string, (ConnectionDetails, SnowflakeDsn)):
            return self.connection_string.schema_

        subbed_str: str | None = _get_config_substituted_connection_string(
            self, warning_msg="Unable to determine schema"
        )
        if not subbed_str:
            return None
        url_path: str = urllib.parse.urlparse(subbed_str).path
        return _get_database_and_schema_from_path(url_path)["schema"]

    @property
    def database(self) -> str | None:
        """Convenience property to get the `database` regardless of the connection string format."""
        if isinstance(self.connection_string, (ConnectionDetails, SnowflakeDsn)):
            return self.connection_string.database

        subbed_str: str | None = _get_config_substituted_connection_string(
            self, warning_msg="Unable to determine database"
        )
        if not subbed_str:
            return None
        url_path: str = urllib.parse.urlparse(subbed_str).path
        return _get_database_and_schema_from_path(url_path)["database"]

    @property
    def warehouse(self) -> str | None:
        """Convenience property to get the `warehouse` regardless of the connection string format."""
        if isinstance(self.connection_string, (ConnectionDetails, SnowflakeDsn)):
            return self.connection_string.warehouse

        subbed_str: str | None = _get_config_substituted_connection_string(
            self, warning_msg="Unable to determine warehouse"
        )
        if not subbed_str:
            return None
        return urllib.parse.parse_qs(urllib.parse.urlparse(subbed_str).query).get(
            "warehouse", [None]
        )[0]

    @property
    def role(self) -> str | None:
        """Convenience property to get the `role` regardless of the connection string format."""
        if isinstance(self.connection_string, (ConnectionDetails, SnowflakeDsn)):
            return self.connection_string.role

        subbed_str: str | None = _get_config_substituted_connection_string(
            self, warning_msg="Unable to determine role"
        )
        if not subbed_str:
            return None
        return urllib.parse.parse_qs(urllib.parse.urlparse(subbed_str).query).get(
            "role", [None]
        )[0]

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SnowflakeDatasource.

        Args:
            test_assets: If assets have been passed to the SQLDatasource, whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            super().test_connection(test_assets=test_assets)
        except TestConnectionError as e:
            if self.account and not self.account.match:
                raise TestConnectionError(
                    AccountIdentifier.MSG_TEMPLATE.format(
                        value=self.account,
                        formats=AccountIdentifier.FORMATS,
                    )
                ) from e
            raise

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

        if values.get("connection_string") and connection_details:
            raise ValueError(
                "Cannot provide both a connection string and a combination of account, user, and password."
            )

        if connection_details:
            values["connection_string"] = connection_details
        return values

    @pydantic.root_validator
    def _check_connection_string(cls, values: dict) -> dict:
        # keeping this validator isn't strictly necessary, but it provides a better error message
        connection_string: str | ConfigStr | ConnectionDetails | None = values.get(
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

    @pydantic.validator("assets", pre=True)
    def _asset_forward_compatibility(cls, assets: list[dict]) -> list[dict]:
        """
        This validator is here to maintain compatibility with future versions of GX.
        """
        modified_assets: list[str] = []
        try:
            # if the incoming asset has a database_name, item we need to remove it.
            # future versions of GX will support a `database_name`.
            if assets:
                for asset in assets:
                    database = asset.pop("database", None)
                    database_name = asset.pop("database_name", None)
                    if asset_name := asset.get("name") and (database or database_name):
                        modified_assets.append(asset_name)
            if modified_assets:
                warnings.warn(
                    f"Assets modified for forward compatibility: {', '.join(modified_assets)}."
                    " Consider updating to the latest version of `great_expectations`.",
                    category=GxDatasourceWarning,
                    stacklevel=2,
                )
        except Exception as e:
            LOGGER.error(f"Error attempting forward compatibility modifications: {e!r}")
        return assets

    @pydantic.validator("kwargs")
    def _base64_encode_private_key(cls, kwargs: dict) -> dict:
        if connect_args := kwargs.get("connect_args", {}):
            if private_key := connect_args.get("private_key"):
                # test if it's already base64 encoded
                if _is_b64_encoded(private_key):
                    LOGGER.info("private_key is already base64 encoded")
                else:
                    LOGGER.info("private_key is not base64 encoded, encoding now")
                    connect_args["private_key"] = base64.standard_b64encode(private_key)
        return kwargs

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

    def _get_url_args(self) -> dict[str, str | bool]:
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

    def _get_snowflake_partner_application(self) -> str:
        """
        This is used to set the application query parameter in the Snowflake connection URL, which provides attribution
        to GX for the Snowflake Partner program.
        """

        # This import is here to avoid a circular import
        from great_expectations.data_context import CloudDataContext

        if isinstance(self._data_context, CloudDataContext):
            return SNOWFLAKE_PARTNER_APPLICATION_CLOUD
        return SNOWFLAKE_PARTNER_APPLICATION_OSS

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
                    url = sa.engine.url.make_url(connection_string)
                    url = url.update_query_dict(
                        query_parameters={
                            "application": self._get_snowflake_partner_application()
                        }
                    )
                    self._engine = self._build_engine_with_connect_args(
                        url=url, **kwargs
                    )
                else:
                    self._engine = self._build_engine_with_connect_args(
                        application=self._get_snowflake_partner_application(),
                        **connection_string,
                        **kwargs,
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

    def _build_engine_with_connect_args(
        self,
        url: URL | None = None,
        connect_args: dict[str, Any] | None = None,
        **kwargs,
    ) -> sqlalchemy.Engine:
        if not url:
            url_args = self._get_url_args()
            url_args.update(kwargs)
            url = URL(**url_args)
        else:
            url_args = {}

        engine_kwargs: dict[Literal["url", "connect_args"], Any] = {}
        if connect_args:
            if private_key := connect_args.get("private_key"):
                url_args.pop(  # TODO: update models + validation to handle this
                    "password", None
                )
                LOGGER.info(
                    "private_key detected, ignoring password and using private_key for authentication"
                )
                # assume the private_key is base64 encoded
                connect_args["private_key"] = base64.standard_b64decode(private_key)

            engine_kwargs["connect_args"] = connect_args

        engine_kwargs["url"] = url

        return sa.create_engine(**engine_kwargs)
