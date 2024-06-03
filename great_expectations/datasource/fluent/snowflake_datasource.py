from __future__ import annotations

import functools
import logging
import urllib.parse
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Final,
    Iterable,
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
from great_expectations.core._docs_decorators import (
    deprecated_method_or_class,
    public_api,
)
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
    TableAsset,
)

if TYPE_CHECKING:
    from great_expectations.compatibility import sqlalchemy
    from great_expectations.compatibility.pydantic.networks import Parts
    from great_expectations.datasource.fluent.interfaces import (
        BatchMetadata,
        SortersDefinition,
    )
    from great_expectations.execution_engine import SqlAlchemyExecutionEngine

LOGGER: Final[logging.Logger] = logging.getLogger(__name__)

REQUIRED_QUERY_PARAMS: Final[
    Iterable[str]
] = {  # errors will be thrown if any of these are missing
    # TODO: require warehouse and role
    # "warehouse",
    # "role",
}

MISSING: Final = object()  # sentinel value to indicate missing values


def _extract_query_section(url: str) -> str | None:
    """
    Extracts the query section of a URL if it exists.

    snowflake://user:password@account?warehouse=warehouse&role=role
    """
    return url.partition("?")[2]


@functools.lru_cache(maxsize=4)
def _extract_path_sections(url_path: str) -> dict[str, str]:
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

        validated_parts = AnyUrl.validate_parts(
            parts=parts, validate_port=validate_port
        )

        path: str = parts["path"]
        # raises UrlPathError if path is missing database/schema
        _extract_path_sections(path)

        return validated_parts

    @property
    def params(self) -> dict[str, list[str]]:
        """The query parameters as a dictionary."""
        if not self.query:
            return {}
        return urllib.parse.parse_qs(self.query)

    @property
    def account_identifier(self) -> str:
        """Alias for host."""
        assert self.host
        return self.host

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

    account: str
    user: str
    password: Union[ConfigStr, str]
    database: str = pydantic.Field(
        ...,
        description="`database` that the Datasource is mapped to.",
    )
    schema_: str = pydantic.Field(
        ..., alias="schema", description="`schema` that the Datasource is mapped to."
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
        return _extract_path_sections(url_path)["schema"]

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
        return _extract_path_sections(url_path)["database"]

    @deprecated_method_or_class(
        version="0.18.16",
        message="`schema_name` is deprecated."
        " The schema now comes from the datasource.",
    )
    @public_api
    @override
    def add_table_asset(  # noqa: PLR0913
        self,
        name: str,
        table_name: str = "",
        schema_name: Optional[str] = MISSING,  # type: ignore[assignment] # sentinel value
        order_by: Optional[SortersDefinition] = None,
        batch_metadata: Optional[BatchMetadata] = None,
    ) -> TableAsset:
        """Adds a table asset to this datasource.

        Args:
            name: The name of this table asset.
            table_name: The table where the data resides.
            schema_name: The schema that holds the table. Will use the datasource schema if not provided.
            order_by: A list of Sorters or Sorter strings.
            batch_metadata: BatchMetadata we want to associate with this DataAsset and all batches derived from it.

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
            order_by=order_by,
            batch_metadata=batch_metadata,
        )

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

    @pydantic.validator("connection_string")
    def _check_for_required_query_params(
        cls, connection_string: ConnectionDetails | SnowflakeDsn | ConfigStr
    ) -> ConnectionDetails | SnowflakeDsn | ConfigStr:
        """
        If connection_string is a SnowflakeDsn,
        check for required query parameters according to `REQUIRED_QUERY_PARAMS`.
        """
        if not isinstance(connection_string, (SnowflakeDsn, ConfigStr)):
            return connection_string

        missing_keys: set[str] = set(REQUIRED_QUERY_PARAMS)
        if isinstance(connection_string, ConfigStr):
            query_str = _extract_query_section(connection_string.template_str)
            # best effort: query could be part of the config substitution. Have to check this when adding assets.
            if not query_str or ConfigStr.str_contains_config_template(query_str):
                LOGGER.info(
                    f"Unable to validate query parameters for {connection_string}"
                )
                return connection_string
        else:
            query_str = connection_string.query

        if query_str:
            query_params: dict[str, list[str]] = urllib.parse.parse_qs(query_str)

            for key in REQUIRED_QUERY_PARAMS:
                if key in query_params:
                    missing_keys.remove(key)

        if missing_keys:
            raise _UrlMissingQueryError(
                msg=f"missing {', '.join(sorted(missing_keys))}",
            )
        return connection_string

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
                    self._engine = sa.create_engine(
                        url,
                        **kwargs,
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

    def _build_engine_with_connect_args(self, **kwargs) -> sqlalchemy.Engine:
        connect_args = self._get_connect_args()
        connect_args.update(kwargs)
        url = URL(**connect_args)
        return sa.create_engine(url)
