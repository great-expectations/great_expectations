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
    Iterable,
    Literal,
    Optional,
    Type,
    Union,
)

from great_expectations._docs_decorators import deprecated_method_or_class, public_api
from great_expectations.compatibility import pydantic
from great_expectations.compatibility.pydantic import AnyUrl, errors
from great_expectations.compatibility.snowflake import (
    IS_SNOWFLAKE_INSTALLED,
    SNOWFLAKE_NOT_IMPORTED,
)
from great_expectations.compatibility.snowflake import URL as SnowflakeURL
from great_expectations.compatibility.sqlalchemy import sqlalchemy as sa
from great_expectations.compatibility.typing_extensions import override
from great_expectations.datasource.fluent import GxContextWarning, GxDatasourceWarning
from great_expectations.datasource.fluent.config_str import (
    ConfigStr,
    ConfigUri,
    _check_config_substitutions_needed,
)
from great_expectations.datasource.fluent.constants import (
    SNOWFLAKE_PARTNER_APPLICATION_CLOUD,
    SNOWFLAKE_PARTNER_APPLICATION_OSS,
)
from great_expectations.datasource.fluent.sql_datasource import (
    FluentBaseModel,
    SQLAlchemyCreateEngineError,
    SQLDatasource,
    TableAsset,
    TestConnectionError,
    to_lower_if_not_quoted,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.compatibility.pydantic.networks import Parts
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
    )
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

REQUIRED_QUERY_PARAMS: Final[Iterable[str]] = {  # errors will be thrown if any of these are missing
    "warehouse",
    "role",
}

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
            # If there's unicode, an exception will be thrown and the function will return false
            sb_bytes = bytes(sb, "ascii")
        elif isinstance(sb, bytes):
            sb_bytes = sb
        else:
            return False
        return base64.b64encode(base64.b64decode(sb_bytes)) == sb_bytes
    except Exception as exc:
        LOGGER.debug(f"Exception handled while checking base64 encoding - {exc!r}")
    return False


@functools.lru_cache(maxsize=4)
def _get_database_and_schema_from_path(url_path: str) -> dict[Literal["database", "schema"], str]:
    """
    Extracts the database and schema from the path of a URL.

    Raises UrlPathError if the path is missing database/schema.

    snowflake://user:password@account/database/schema
    """
    try:
        _, database, schema, *_ = url_path.split("/")
    except (ValueError, AttributeError) as e:
        LOGGER.info(f"Unable to split path - {e!r}")
        raise UrlPathError() from e
    if not database:
        raise UrlPathError(msg="missing database")
    if not schema:
        raise UrlPathError(msg="missing schema")
    return {"database": database, "schema": schema}


def _get_config_substituted_connection_string(
    datasource: SnowflakeDatasource,
    warning_msg: str = "Unable to perform config substitution",
) -> AnyUrl | None:
    if not isinstance(datasource.connection_string, ConfigUri):
        raise TypeError("Config substitution is only supported for `ConfigUri`")  # noqa: TRY003
    if not datasource._data_context:
        warnings.warn(
            f"{warning_msg} for {datasource.connection_string.template_str}."
            " Likely missing a context.",
            category=GxContextWarning,
        )
        return None
    return datasource.connection_string.get_config_value(datasource._data_context.config_provider)


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

    The cloud group is optional if on aws but expecting it to be there makes it easier to
    distinguish formats.
    GX does not throw errors based on the regex parsing result.

    The `.` separated version of the Account name format is not supported with SQLAlchemy.
    """

    FORMATS: ClassVar[str] = "<orgname>-<account_name> or <account_locator>.<region>.<cloud>"

    FMT_1: ClassVar[str] = r"^(?P<orgname>[a-zA-Z0-9]+)[-](?P<account_name>[a-zA-Z0-9-_]+)$"
    FMT_2: ClassVar[str] = (
        r"^(?P<account_locator>[a-zA-Z0-9]+)\.(?P<region>[a-zA-Z0-9-]+)\.(?P<cloud>aws|gcp|azure)$"
    )
    PATTERN: ClassVar[re.Pattern] = re.compile(f"{FMT_1}|{FMT_2}")

    MSG_TEMPLATE: ClassVar[str] = (
        "Account identifier '{value}' does not match expected format {formats} ;"
        " it MAY be invalid. https://docs.snowflake.com/en/user-guide/admin-account-identifier"
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
            "description": "Snowflake Account identifiers can be in one of two formats:"
            f" {cls.FORMATS}",
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
            raise ValueError("Account identifier cannot be empty")  # noqa: TRY003
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
        raise ValueError("Account identifier does not match either expected format")  # noqa: TRY003


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


class UrlPathError(pydantic.UrlError):
    """
    Custom Pydantic error for missing path in SnowflakeDsn.
    """

    code = "url.path"
    msg_template = "URL path missing database/schema"

    def __init__(self, **ctx: Any) -> None:
        super().__init__(**ctx)


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

        validated_parts = AnyUrl.validate_parts(parts=parts, validate_port=validate_port)

        path: str = parts["path"]
        # raises UrlPathError if path is missing database/schema
        _get_database_and_schema_from_path(path)

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
        """Alias for account_identifier."""
        return self.account_identifier

    @property
    def database(self) -> str:
        assert self.path
        return self.path.split("/")[1]

    @property
    def schema_(self) -> str:
        assert self.path
        return self.path.split("/")[2]

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
    database: str = pydantic.Field(
        ...,
        description="`database` that the Datasource is mapped to.",
    )
    schema_: str = pydantic.Field(
        ..., alias="schema", description="`schema` that the Datasource is mapped to."
    )  # schema is a reserved attr in BaseModel
    warehouse: str
    role: str
    numpy: bool = False

    @classmethod
    def required_fields(cls) -> list[str]:
        """Returns the required fields for this model as defined in the schema."""
        return cls.schema()["required"]

    class Config:
        @staticmethod
        def schema_extra(schema: dict, model: type[ConnectionDetails]) -> None:
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
    """  # noqa: E501

    type: Literal["snowflake"] = "snowflake"  # type: ignore[assignment]
    # TODO: rename this to `connection` for v1?
    connection_string: Union[ConnectionDetails, ConfigUri, SnowflakeDsn]  # type: ignore[assignment] # Deviation from parent class as individual args are supported for connection

    # TODO: add props for user, password, etc?

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
            return to_lower_if_not_quoted(self.connection_string.schema_)

        subbed_str: str | None = _get_config_substituted_connection_string(
            self, warning_msg="Unable to determine schema"
        )
        if not subbed_str:
            return None
        url_path: str = urllib.parse.urlparse(subbed_str).path

        return to_lower_if_not_quoted(_get_database_and_schema_from_path(url_path)["schema"])

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
        """
        Convenience property to get the `warehouse` regardless of the connection string format.
        """
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
        return urllib.parse.parse_qs(urllib.parse.urlparse(subbed_str).query).get("role", [None])[0]

    @override
    def test_connection(self, test_assets: bool = True) -> None:
        """Test the connection for the SnowflakeDatasource.

        Args:
            test_assets: If assets have been passed to the SQLDatasource,
            whether to test them as well.

        Raises:
            TestConnectionError: If the connection test fails.
        """
        try:
            super().test_connection(test_assets=test_assets)
        except TestConnectionError as e:
            if self.account and not self.account.match:
                raise TestConnectionError(
                    message=e.__class__.__name__,
                    addendum=AccountIdentifier.MSG_TEMPLATE.format(
                        value=self.account,
                        formats=AccountIdentifier.FORMATS,
                    ),
                ) from e
            raise

    @deprecated_method_or_class(
        version="1.0.0a4",
        message="`schema_name` is deprecated." " The schema now comes from the datasource.",
    )
    @public_api
    @override
    def add_table_asset(
        self,
        name: str,
        table_name: str = "",
        schema_name: Optional[str] = MISSING,  # type: ignore[assignment] # sentinel value
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema_name: The schema that holds the table. Will use the datasource schema if not
                provided.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches
                derived from it.

        Returns:
            The table asset that is added to the datasource.
            The type of this object will match the necessary type for this datasource.
        """
        if schema_name is MISSING:
            # using MISSING to indicate that the user did not provide a value
            schema_name = self.schema_
        else:
            # deprecated-v0.18.16
            warnings.warn(
                "The `schema_name argument` is deprecated and will be removed in a future release."
                " The schema now comes from the datasource.",
                category=DeprecationWarning,
            )
            if schema_name != self.schema_:
                warnings.warn(
                    f"schema_name {schema_name} does not match datasource schema {self.schema_}",
                    category=GxDatasourceWarning,
                )

        return super().add_table_asset(
            name=name,
            table_name=table_name,
            schema_name=schema_name,
            batch_metadata=batch_metadata,
        )

    @pydantic.root_validator(pre=True)
    def _convert_root_connection_detail_fields(cls, values: dict) -> dict:
        """
        Convert root level connection detail fields to a ConnectionDetails compatible object.
        This preserves backwards compatibility with the previous implementation of SnowflakeDatasource.

        It also allows for users to continue to provide connection details in the
        `context.data_sources.add_snowflake()` factory functions without nesting it in a
        `connection_string` dict.
        """  # noqa: E501
        connection_detail_fields: set[str] = {
            "schema",  # field name in ConnectionDetails is schema_ (with underscore)
            *ConnectionDetails.__fields__.keys(),
        }

        connection_string: Any | None = values.get("connection_string")
        provided_fields = tuple(values.keys())

        connection_details = {}
        for field_name in provided_fields:
            if field_name in connection_detail_fields:
                if connection_string:
                    raise ValueError(  # noqa: TRY003
                        "Provided both connection detail keyword args and `connection_string`."
                    )
                connection_details[field_name] = values.pop(field_name)
        if connection_details:
            values["connection_string"] = connection_details
        return values

    @pydantic.validator("connection_string", pre=True)
    def _check_config_template(cls, connection_string: Any) -> Any:
        """
        If connection_string has a config template, parse it as a ConfigUri, ignore other errors.
        """
        if isinstance(connection_string, str):
            if ConfigUri.str_contains_config_template(connection_string):
                LOGGER.debug("`connection_string` contains config template")
                return pydantic.parse_obj_as(ConfigUri, connection_string)
        return connection_string

    @pydantic.root_validator
    def _check_xor_input_args(cls, values: dict) -> dict:
        # keeping this validator isn't strictly necessary, but it provides a better error message
        connection_string: str | ConfigUri | ConnectionDetails | None = values.get(
            "connection_string"
        )
        if connection_string:
            # Method 1 - connection string
            is_connection_string: bool = isinstance(
                connection_string, (str, ConfigStr, SnowflakeDsn)
            )
            # Method 2 - individual args (account, user, and password are bare minimum)
            has_min_connection_detail_values: bool = isinstance(
                connection_string, ConnectionDetails
            ) and bool(
                connection_string.account and connection_string.user and connection_string.password
            )
            if is_connection_string or has_min_connection_detail_values:
                return values
        raise ValueError(  # noqa: TRY003
            "Must provide either a connection string or"
            f" a combination of {', '.join(ConnectionDetails.required_fields())} as keyword args."
        )

    @pydantic.validator("connection_string")
    def _check_for_required_query_params(
        cls, connection_string: ConnectionDetails | SnowflakeDsn | ConfigUri
    ) -> ConnectionDetails | SnowflakeDsn | ConfigUri:
        """
        If connection_string is a SnowflakeDsn,
        check for required query parameters according to `REQUIRED_QUERY_PARAMS`.
        """
        if not isinstance(connection_string, (SnowflakeDsn, ConfigUri)):
            return connection_string

        missing_keys: set[str] = set(REQUIRED_QUERY_PARAMS)

        if connection_string.query:
            query_params: dict[str, list[str]] = urllib.parse.parse_qs(connection_string.query)

            for key in REQUIRED_QUERY_PARAMS:
                if key in query_params:
                    missing_keys.remove(key)

        if missing_keys:
            raise _UrlMissingQueryError(
                msg=f"missing {', '.join(sorted(missing_keys))}",
            )
        return connection_string

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
            connection_string_prop = schema["properties"]["connection_string"].pop("anyOf")
            schema["properties"]["connection_string"].update({"oneOf": connection_string_prop})

    def _get_connect_args(self) -> dict[str, str | bool]:
        excluded_fields: set[str] = set(SQLDatasource.__fields__.keys())
        # dump as json dict to force serialization of things like AnyUrl
        return self._json_dict(exclude=excluded_fields, exclude_none=True)

    def _get_snowflake_partner_application(self) -> str:
        """
        This is used to set the application query parameter in the Snowflake connection URL,
        which provides attribution to GX for the Snowflake Partner program.
        """

        # This import is here to avoid a circular import
        from great_expectations.data_context import CloudDataContext

        if isinstance(self._data_context, CloudDataContext):
            return SNOWFLAKE_PARTNER_APPLICATION_CLOUD
        return SNOWFLAKE_PARTNER_APPLICATION_OSS

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

        for SQLAlchemy this would lead to creating 2 different `sqlalchemy.engine.Engine` objects
        one for the Datasource and one for the ExecutionEngine. This is wasteful and causes multiple connections to
        the database to be created.

        For Snowflake specifically we may represent the connection_string as a dict, which is not supported by SQLAlchemy.
        """  # noqa: E501
        gx_execution_engine_type: Type[SqlAlchemyExecutionEngine] = self.execution_engine_type

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
                    url = sa.engine.url.make_url(connection_string)
                    url = url.update_query_dict(
                        query_parameters={"application": self._get_snowflake_partner_application()}
                    )
                    self._engine = self._build_engine_with_connect_args(url=url, **kwargs)
                else:
                    self._engine = self._build_engine_with_connect_args(
                        application=self._get_snowflake_partner_application(),
                        **connection_string,
                        **kwargs,
                    )

            except Exception as e:
                # connection_string passed pydantic validation, but fails to create sqla engine
                # Possible case is a missing plugin (e.g. snowflake-sqlalchemy)
                if IS_SNOWFLAKE_INSTALLED:
                    raise SQLAlchemyCreateEngineError(
                        cause=e, addendum=str(SNOWFLAKE_NOT_IMPORTED)
                    ) from e
                raise SQLAlchemyCreateEngineError(cause=e) from e
            # connection string isn't strictly required for Snowflake, so we conditionally cache
            if isinstance(self.connection_string, str):
                self._cached_connection_string = self.connection_string
        return self._engine

    def _build_engine_with_connect_args(
        self,
        url: SnowflakeURL | None = None,
        connect_args: dict[str, Any] | None = None,
        **kwargs,
    ) -> sqlalchemy.Engine:
        if not url:
            url_args = self._get_url_args()
            url_args.update(kwargs)
            url = SnowflakeURL(**url_args)
        else:
            url_args = {}

        engine_kwargs: dict[Literal["url", "connect_args"], Any] = {}
        if connect_args:
            if private_key := connect_args.get("private_key"):
                url_args.pop(  # TODO: update models + validation to handle this
                    "password", None
                )
                LOGGER.info(
                    "private_key detected,"
                    " ignoring password and using private_key for authentication"
                )
                # assume the private_key is base64 encoded
                connect_args["private_key"] = base64.standard_b64decode(private_key)

            engine_kwargs["connect_args"] = connect_args

        engine_kwargs["url"] = url

        return sa.create_engine(**engine_kwargs)  # type: ignore[misc]
