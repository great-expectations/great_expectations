from __future__ import annotations

import copy
import datetime
import hashlib
import logging
import math
import os
import re
import traceback
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    MutableMapping,
    Optional,
    Tuple,
    Union,
    cast,
)

from packaging import version

from great_expectations.compatibility.typing_extensions import override

from great_expectations._version import get_versions  # isort:skip


__version__ = get_versions()["version"]  # isort:skip

from great_expectations._docs_decorators import new_method_or_class
from great_expectations.compatibility import snowflake, sqlalchemy
from great_expectations.compatibility.not_imported import is_version_greater_or_equal
from great_expectations.compatibility.sqlalchemy import (
    ColumnElement,
    DatabaseError,
    PendingRollbackError,
    Subquery,
)
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.execution_engine.execution_engine import (
    MetricComputationConfiguration,
    PartitionDomainKwargs,
)
from great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_partitioner import (
    SqlAlchemyDataPartitioner,
)
from great_expectations.execution_engine.partition_and_sample.sqlalchemy_data_sampler import (
    SqlAlchemyDataSampler,
)
from great_expectations.expectations.model_field_types import (
    CONDITION_PARSER_GREAT_EXPECTATIONS,
    CONDITION_PARSER_GREAT_EXPECTATIONS_DEPRECATED,
)
from great_expectations.util import (
    convert_to_json_serializable,  # noqa: TID251 # Required for SQL result serialization
)

del get_versions  # isort:skip


from great_expectations.core.batch import BatchMarkers, BatchSpec
from great_expectations.core.batch_spec import (
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.id_dict import IDDict, IDDictID
from great_expectations.exceptions import (
    DatasourceKeyPairAuthBadPassphraseError,
    ExecutionEngineError,
    GreatExpectationsError,
    InvalidBatchSpecError,
    InvalidConfigError,
)
from great_expectations.exceptions import exceptions as gx_exceptions
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.execution_engine.sqlalchemy_dialect import GXSqlDialect
from great_expectations.expectations.legacy_row_conditions import (
    RowCondition,
    RowConditionParserType,
    parse_condition_to_sqlalchemy,
)
from great_expectations.expectations.row_conditions import (
    Condition,
    Operator,
    PassThroughCondition,
    deserialize_row_condition,
)
from great_expectations.util import (
    filter_properties_dict,
    get_sqlalchemy_selectable,
    get_sqlalchemy_url,
    import_library_module,
    import_make_url,
)

if TYPE_CHECKING:
    from typing_extensions import TypeAlias

    from great_expectations.expectations.row_conditions import (
        AndCondition,
        ComparisonCondition,
        NullityCondition,
        OrCondition,
    )
    from great_expectations.validator.computed_metric import (
        MetricValue,
    )
    from great_expectations.validator.metric_configuration import (
        MetricConfiguration,
        MetricConfigurationID,
    )


logger = logging.getLogger(__name__)
DATABRICKS_MAX_PARAMS_PER_QUERY = 256


if sa:
    make_url = import_make_url()


try:
    import psycopg2  # noqa: F401 # FIXME CoP
    import sqlalchemy.dialects.postgresql.psycopg2 as sqlalchemy_psycopg2  # noqa: TID251 # FIXME CoP
except (ImportError, KeyError):
    sqlalchemy_psycopg2 = None  # type: ignore[assignment] # FIXME CoP

try:
    import sqlalchemy_dremio.pyodbc

    if sa:
        sa.dialects.registry.register(GXSqlDialect.DREMIO, "sqlalchemy_dremio.pyodbc", "dialect")  # type: ignore[arg-type] # FIXME CoP
except ImportError:
    sqlalchemy_dremio = None

if snowflake.snowflakedialect:
    if sa:
        # Sometimes "snowflake-sqlalchemy" fails to self-register in certain environments, so we do it explicitly.  # noqa: E501 # FIXME CoP
        # (see https://stackoverflow.com/questions/53284762/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectssnowflake)
        sa.dialects.registry.register(GXSqlDialect.SNOWFLAKE, "snowflake.sqlalchemy", "dialect")  # type: ignore[arg-type] # FIXME CoP

from great_expectations.compatibility.bigquery import (
    _BIGQUERY_MODULE_NAME,
)
from great_expectations.compatibility.bigquery import (
    sqlalchemy_bigquery as sqla_bigquery,
)

if sqla_bigquery and sa:
    sa.dialects.registry.register(GXSqlDialect.BIGQUERY, _BIGQUERY_MODULE_NAME, "BigQueryDialect")  # type: ignore[arg-type] # FIXME CoP

try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None
    teradatatypes = None

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine as SaEngine  # noqa: TID251 # FIXME CoP


SQLAColumnClause: TypeAlias = object  # sqlalchemy isn't installed in all environments

_PERSISTED_CONNECTION_DIALECTS = (
    GXSqlDialect.SQLITE,
    GXSqlDialect.SQL_SERVER,
    GXSqlDialect.BIGQUERY,
    GXSqlDialect.DATABRICKS,
)


class InvalidOperatorError(ValueError):
    def __init__(self, operator: Any) -> None:
        super().__init__(f"Invalid operator: {operator!r}")


class InvalidFilterClause(ValueError):
    def __init__(self, filter_clause: Any) -> None:
        super().__init__(f"Invalid filter clause: {type(filter_clause)}")


def _dialect_requires_persisted_connection(
    connection_string: str | None = None,
    credentials: dict | None = None,
    url: str | None = None,
) -> bool:
    """Determine if the dialect needs a persisted connection.

    dialect_name isn't available yet since the engine isn't yet created when we call this method,
    so we determine the dialect from the creds/url/params.

    Args:
        connection_string: Database connection string to check
        credentials: Dictionary of database connection credentials. Only `drivername` is checked.
        url: Database connection URL to parse and check.

    Returns:
        Boolean indicating whether the dialect requires a persisted connection.
    """
    if sum(bool(x) for x in [connection_string, credentials, url is not None]) != 1:
        raise ValueError("Exactly one of connection_string, credentials, url must be specified")  # noqa: TRY003 # FIXME CoP
    return_val = False
    if connection_string is not None:
        str_to_check = connection_string

    elif credentials is not None:
        drivername = credentials.get("drivername", "")
        str_to_check = drivername

    else:
        parsed_url = make_url(url)
        str_to_check = parsed_url.drivername

    if any(
        str_to_check.startswith(dialect_name.value)
        for dialect_name in _PERSISTED_CONNECTION_DIALECTS
    ):
        return_val = True

    return return_val


def _ensure_sql_text_ends_with_newline(text: str) -> str:
    """Ensure raw SQL text ends on a fresh line before it is wrapped as a subquery.

    SQLAlchemy compiles wrapped raw SQL text verbatim inside "(<text>) AS alias" on a
    single line. If the text ends in a line comment (e.g. "-- note"), the appended ")"
    and alias land inside that comment and the resulting statement is never terminated.
    Ending the text on a fresh line keeps any appended syntax out of reach of a comment
    that runs to the end of the text, without changing what the query selects.

    Args:
        text: The raw SQL text about to be wrapped as a subquery.

    Returns:
        The same text, with a trailing newline added if it didn't already have one.
    """
    return text if text.endswith("\n") else f"{text}\n"


def _wrap_raw_sql_as_subquery(selectable: sqlalchemy.TextClause) -> Subquery:
    """Wrap a raw-SQL TextClause (e.g. from a query asset) as a subquery.

    See _ensure_sql_text_ends_with_newline for why the text is normalized first.

    Args:
        selectable: The raw-SQL TextClause to wrap.

    Returns:
        A Subquery built from the TextClause's columns.
    """
    text = _ensure_sql_text_ends_with_newline(selectable.text)
    return sa.text(text).columns().subquery()


class SqlAlchemyExecutionEngine(ExecutionEngine[SQLAColumnClause]):
    """SparkDFExecutionEngine instantiates the ExecutionEngine API to support computations using Spark platform.

    Constructor builds a SqlAlchemyExecutionEngine, using a provided connection string/url/engine/credentials to \
    access the desired database.

    Also initializes the dialect to be used.

    Args:
        name (str): The name of the SqlAlchemyExecutionEngine
        credentials: If the Execution Engine is not provided, the credentials can be used to build the \
            ExecutionEngine object. If the Engine is provided, it will be used instead.
        data_context (DataContext): An object representing a Great Expectations project that can be used to \
            access ExpectationSuite objects and the Project Data itself.
        engine (Engine): A SqlAlchemy Engine used to set the SqlAlchemyExecutionEngine being configured, \
            useful if an Engine has already been configured and should be reused. Will override Credentials if \
            provided. If you are passing an engine that requires a single connection e.g. if temporary tables are \
            not persisted if the connection is closed (e.g. sqlite, SQL Server) then you should create the engine with \
            a StaticPool e.g. engine = sa.create_engine(connection_url, poolclass=sa.pool.StaticPool)
        connection_string (string): If neither the engines nor the credentials have been provided, a \
            connection string can be used to access the data. This will be overridden by both the engine and \
            credentials if those are provided.
        url (string): If neither the engines, the credentials, nor the connection_string have been provided, a \
            URL can be used to access the data. This will be overridden by all other configuration options if \
            any are provided.
        kwargs (dict): These will be passed as optional parameters to the SQLAlchemy engine, **not** the ExecutionEngine

    For example:
    ```python
        execution_engine: ExecutionEngine = SqlAlchemyExecutionEngine(connection_string="dbmstype://user:password@host:5432/database_name")
    ```
    """  # noqa: E501 # FIXME CoP

    # noinspection PyUnusedLocal
    def __init__(  # noqa: C901, PLR0912, PLR0913, PLR0915 # FIXME CoP
        self,
        name: Optional[str] = None,
        credentials: Optional[dict] = None,
        data_context: Optional[Any] = None,
        engine: Optional[SaEngine] = None,
        connection_string: Optional[str] = None,
        url: Optional[str] = None,
        batch_data_dict: Optional[dict] = None,
        create_temp_table: bool = True,
        # kwargs will be passed as optional parameters to the SQLAlchemy engine, **not** the ExecutionEngine  # noqa: E501 # FIXME CoP
        **kwargs,
    ) -> None:
        super().__init__(name=name, batch_data_dict=batch_data_dict)
        self._name = name

        self._credentials = credentials
        self._connection_string = connection_string
        self._url = url
        self._create_temp_table = create_temp_table
        os.environ["SF_PARTNER"] = "great_expectations_oss"  # noqa: TID251 # FIXME CoP

        # sqlite/SQL Server temp tables only persist within a connection, so we need to keep the connection alive by  # noqa: E501 # FIXME CoP
        # keeping a reference to it.
        # Even though we use a single connection pool for dialects that need a single persisted connection  # noqa: E501 # FIXME CoP
        # (e.g. for accessing temporary tables), if we don't keep a reference
        # then we get errors like sqlite3.ProgrammingError: Cannot operate on a closed database.
        self._connection: sqlalchemy.Connection | None = None

        # Use a single instance of SQLAlchemy engine to avoid creating multiple engine instances
        # for the same SQLAlchemy engine. This allows us to take advantage of SQLAlchemy's
        # built-in caching.
        self._inspector = None

        if engine is not None:
            if credentials is not None:
                logger.warning(
                    "Both credentials and engine were provided during initialization of SqlAlchemyExecutionEngine. "  # noqa: E501 # FIXME CoP
                    "Ignoring credentials."
                )
            self.engine = engine
        else:
            self._setup_engine(
                kwargs=kwargs,
                connection_string=connection_string,
                credentials=credentials,
                url=url,
            )

        # these are two backends where temp_table_creation is not supported we set the default value to False.  # noqa: E501 # FIXME CoP
        if (
            self.dialect_name
            in [
                GXSqlDialect.TRINO,
                GXSqlDialect.AWSATHENA,  # WKS 202201 - AWS Athena currently doesn't support temp_tables.  # noqa: E501 # FIXME CoP
                GXSqlDialect.CLICKHOUSE,
            ]
        ):
            self._create_temp_table = False

        # Get the dialect **for purposes of identifying types**
        if self.dialect_name in [
            GXSqlDialect.POSTGRESQL,
            GXSqlDialect.MYSQL,
            GXSqlDialect.SQLITE,
            GXSqlDialect.ORACLE,
            GXSqlDialect.SQL_SERVER,
        ]:
            # These are the officially included and supported dialects by sqlalchemy
            self.dialect_module = import_library_module(
                module_name=f"sqlalchemy.dialects.{self.engine.dialect.name}"
            )

        elif self.dialect_name == GXSqlDialect.SNOWFLAKE:
            self.dialect_module = import_library_module(
                module_name="snowflake.sqlalchemy.snowdialect"
            )
        elif self.dialect_name == GXSqlDialect.DREMIO:
            # WARNING: Dremio Support is experimental, functionality is not fully under test
            self.dialect_module = import_library_module(module_name="sqlalchemy_dremio.pyodbc")
        elif self.dialect_name == GXSqlDialect.REDSHIFT:
            self.dialect_module = import_library_module(module_name="sqlalchemy_redshift.dialect")
        elif self.dialect_name == GXSqlDialect.BIGQUERY:
            self.dialect_module = import_library_module(module_name=_BIGQUERY_MODULE_NAME)
        elif self.dialect_name == GXSqlDialect.TERADATASQL:
            # WARNING: Teradata Support is experimental, functionality is not fully under test
            self.dialect_module = import_library_module(module_name="teradatasqlalchemy.dialect")
        elif self.dialect_name == GXSqlDialect.TRINO:
            # WARNING: Trino Support is experimental, functionality is not fully under test
            self.dialect_module = import_library_module(module_name="trino.sqlalchemy.dialect")
        elif self.dialect_name == GXSqlDialect.CLICKHOUSE:
            # WARNING: ClickHouse Support is experimental, functionality is not fully under test
            self.dialect_module = import_library_module(
                module_name="clickhouse_sqlalchemy.drivers.base"
            )
        elif self.dialect_name == GXSqlDialect.DATABRICKS:
            self.dialect_module = import_library_module("databricks.sqlalchemy")
        elif self.dialect_name == GXSqlDialect.SINGLESTOREDB:
            self.dialect_module = import_library_module("sqlalchemy_singlestoredb")
        else:
            self.dialect_module = None

        # <WILL> 20210726 - engine_backup is used by the snowflake connector, which requires connection and engine  # noqa: E501 # FIXME CoP
        # to be closed and disposed separately. Currently self.engine can refer to either a Connection or Engine,  # noqa: E501 # FIXME CoP
        # depending on the backend. This will need to be cleaned up in an upcoming refactor, so that Engine and  # noqa: E501 # FIXME CoP
        # Connection can be handled separately.
        self._engine_backup = None
        if self.engine and self.dialect_name in [
            GXSqlDialect.SQLITE,
            GXSqlDialect.SQL_SERVER,
            GXSqlDialect.SNOWFLAKE,
            GXSqlDialect.MYSQL,
        ]:
            if self.engine.dialect.name.lower() == GXSqlDialect.SQLITE:

                def _add_sqlite_functions(connection):
                    logger.info(f"Adding custom sqlite functions to connection {connection}")
                    connection.create_function("sqrt", 1, math.sqrt)
                    # not used for security: this is hash partitioning/sampling, not a
                    # cryptographic digest. usedforsecurity=False lets this run on hosts
                    # with FIPS-mode OpenSSL.
                    connection.create_function(
                        "md5",
                        2,
                        lambda x, d: hashlib.md5(
                            str(x).encode("utf-8"), usedforsecurity=False
                        ).hexdigest()[-1 * d :],
                    )

                # Add sqlite functions to any future connections.
                def _on_connect(dbapi_con, connection_record):
                    logger.info(
                        f"A new sqlite connection was created: {dbapi_con}, {connection_record}"
                    )
                    _add_sqlite_functions(dbapi_con)

                sa.event.listen(self.engine, "connect", _on_connect)
                # Also immediately add the sqlite functions in case there already exists an underlying  # noqa: E501 # FIXME CoP
                # sqlite3.Connection (distinct from a sqlalchemy Connection).
                _raw_dbapi_con = self.engine.raw_connection()
                try:
                    _add_sqlite_functions(_raw_dbapi_con)
                finally:
                    # Ensure the temporary raw DB-API connection is closed to avoid ResourceWarning.
                    try:
                        _raw_dbapi_con.close()
                    except Exception:
                        pass
            self._engine_backup = self.engine

        # Gather the call arguments of the present function (and add the "class_name"), filter out the Falsy values,  # noqa: E501 # FIXME CoP
        # and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "name": name,
            "credentials": credentials,
            "data_context": data_context,
            "engine": engine,  # type: ignore[dict-item] # FIXME CoP
            "connection_string": connection_string,
            "url": url,
            "batch_data_dict": batch_data_dict,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        self._config.update(kwargs)
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)

        self._data_partitioner = SqlAlchemyDataPartitioner(dialect=self.dialect_name)
        self._data_sampler = SqlAlchemyDataSampler()

    def _setup_engine(
        self,
        kwargs: MutableMapping[str, Any],
        connection_string: str | None = None,
        credentials: dict | None = None,
        url: str | None = None,
    ):
        """Create an engine and set the engine instance variable on the execution engine.

        Args:
            kwargs: These will be passed as optional parameters to the SQLAlchemy engine, **not** the ExecutionEngine
            connection_string: Used to connect to the database.
            credentials: Used to connect to the database.
            url: Used to connect to the database.

        Returns:
            Nothing, the engine instance variable is set.
        """  # noqa: E501 # FIXME CoP
        if credentials is not None:
            self.engine = self._build_engine(credentials=credentials, **kwargs)
        elif connection_string is not None:
            if _dialect_requires_persisted_connection(
                connection_string=connection_string, credentials=credentials, url=url
            ):
                self.engine = sa.create_engine(
                    connection_string, **kwargs, poolclass=sqlalchemy.StaticPool
                )
            else:
                self.engine = sa.create_engine(connection_string, **kwargs)
        elif url is not None:
            parsed_url = make_url(url)
            self.drivername = parsed_url.drivername
            if _dialect_requires_persisted_connection(
                connection_string=connection_string, credentials=credentials, url=url
            ):
                self.engine = sa.create_engine(url, **kwargs, poolclass=sqlalchemy.StaticPool)
            else:
                self.engine = sa.create_engine(url, **kwargs)
        else:
            raise InvalidConfigError(  # noqa: TRY003 # FIXME CoP
                "Credentials or an engine are required for a SqlAlchemyExecutionEngine."
            )

    @property
    def credentials(self) -> Optional[dict]:
        return self._credentials

    @property
    def connection_string(self) -> Optional[str]:
        return self._connection_string

    @property
    def url(self) -> Optional[str]:
        return self._url

    @property
    @override
    def dialect(self) -> sqlalchemy.Dialect:
        return self.engine.dialect

    @property
    def dialect_name(self) -> str:
        """Retrieve the string name of the engine dialect in lowercase e.g. "postgresql".

        Returns:
            String representation of the sql dialect.
        """
        return self.engine.dialect.name.lower()

    def _build_engine(self, credentials: dict, **kwargs) -> sa.engine.Engine:
        """
        Using a set of given credentials, constructs an Execution Engine , connecting to a database using a URL or a
        private key path.
        """  # noqa: E501 # FIXME CoP
        # Update credentials with anything passed during connection time
        drivername = credentials.pop("drivername")
        schema_name = credentials.pop("schema_name", None)
        if schema_name is not None:
            logger.warning(
                "schema_name specified creating a URL with schema is not supported. Set a default "
                "schema on the user connecting to your database."
            )

        create_engine_kwargs = kwargs
        connect_args = credentials.pop("connect_args", None)
        if connect_args:
            create_engine_kwargs["connect_args"] = connect_args

        if "private_key_path" in credentials:
            options, create_engine_kwargs = self._get_sqlalchemy_key_pair_auth_url(
                drivername, credentials
            )
        else:
            options = get_sqlalchemy_url(drivername, **credentials)

        self.drivername = drivername
        if _dialect_requires_persisted_connection(credentials=credentials):
            engine = sa.create_engine(
                options, **create_engine_kwargs, poolclass=sqlalchemy.StaticPool
            )
        else:
            engine = sa.create_engine(options, **create_engine_kwargs)

        return engine

    @staticmethod
    def _get_sqlalchemy_key_pair_auth_url(
        drivername: str,
        credentials: dict,
    ) -> Tuple[sa.engine.url.URL, dict]:
        """
        Utilizing a private key path and a passphrase in a given credentials dictionary, attempts to encode the provided
        values into a private key. If passphrase is incorrect, this will fail and an exception is raised.

        Args:
            drivername(str) - The name of the driver class
            credentials(dict) - A dictionary of database credentials used to access the database

        Returns:
            a tuple consisting of a url with the serialized key-pair authentication, and a dictionary of engine kwargs.
        """  # noqa: E501 # FIXME CoP
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        private_key_path = credentials.pop("private_key_path")
        private_key_passphrase = credentials.pop("private_key_passphrase")

        with Path(private_key_path).expanduser().resolve().open(mode="rb") as key:
            try:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=(private_key_passphrase.encode() if private_key_passphrase else None),
                    backend=default_backend(),
                )
            except ValueError as e:
                if "incorrect password" in str(e).lower():
                    raise DatasourceKeyPairAuthBadPassphraseError(
                        datasource_name="SqlAlchemyDatasource",
                        message="Decryption of key failed, was the passphrase incorrect?",
                    ) from e
                else:
                    raise e  # noqa: TRY201 # FIXME CoP
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        credentials_driver_name = credentials.pop("drivername", None)
        create_engine_kwargs = {"connect_args": {"private_key": pkb}}
        return (
            get_sqlalchemy_url(drivername or credentials_driver_name, **credentials),
            create_engine_kwargs,
        )

    @override
    def get_domain_records(  # noqa: C901, PLR0912, PLR0915 # FIXME CoP
        self,
        domain_kwargs: dict,
    ) -> sqlalchemy.Selectable:
        """Uses the given Domain kwargs (which include row_condition, condition_parser, and ignore_row_if directives) to obtain and/or query a Batch of data.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the Domain kwargs specifying which data to obtain

        Returns:
            An SqlAlchemy table/column(s) (the selectable object for obtaining data on which to compute returned in the format of an SqlAlchemy table/column(s) object)
        """  # noqa: E501 # FIXME CoP
        data_object: SqlAlchemyBatchData

        batch_id: Optional[str] = domain_kwargs.get("batch_id")
        if batch_id is None:
            # We allow no batch id specified if there is only one batch
            if self.batch_manager.active_batch_data:
                data_object = cast("SqlAlchemyBatchData", self.batch_manager.active_batch_data)
            else:
                raise GreatExpectationsError(  # noqa: TRY003 # FIXME CoP
                    "No batch is specified, but could not identify a loaded batch."
                )
        else:  # noqa: PLR5501 # FIXME CoP
            if batch_id in self.batch_manager.batch_data_cache:
                data_object = cast(
                    "SqlAlchemyBatchData", self.batch_manager.batch_data_cache[batch_id]
                )
            else:
                raise GreatExpectationsError(f"Unable to find batch with batch_id {batch_id}")  # noqa: TRY003 # FIXME CoP

        selectable: sqlalchemy.Selectable
        if "table" in domain_kwargs and domain_kwargs["table"] is not None:
            # TODO: Add logic to handle record_set_name once implemented
            # (i.e. multiple record sets (tables) in one batch
            if domain_kwargs["table"] != data_object.selectable.name:
                # noinspection PyProtectedMember
                # _schema_name is set when using the table_name path in SqlAlchemyBatchData.
                # _source_schema_name is set when using the selectable path (fluent API).
                # We need to check both to properly schema-qualify the "other" table in queries that
                # join across tables.
                schema = data_object._schema_name or data_object._source_schema_name
                selectable = sa.Table(
                    domain_kwargs["table"],
                    sa.MetaData(),
                    schema=schema,
                )
            else:
                selectable = data_object.selectable
        elif "query" in domain_kwargs:
            raise ValueError("query is not currently supported by SqlAlchemyExecutionEngine")  # noqa: TRY003 # FIXME CoP
        else:
            selectable = data_object.selectable

        """
        If a custom query is passed, selectable will be TextClause and not formatted
        as a subquery wrapped in "(subquery) alias". TextClause must first be converted
        to TextualSelect using sa.columns() before it can be converted to type Subquery
        """
        if sqlalchemy.TextClause and isinstance(selectable, sqlalchemy.TextClause):  # type: ignore[truthy-function] # FIXME CoP
            selectable = _wrap_raw_sql_as_subquery(selectable)

        # Filtering by row condition.
        if "row_condition" in domain_kwargs and domain_kwargs["row_condition"] is not None:
            row_condition = domain_kwargs["row_condition"]
            condition_parser = domain_kwargs.get("condition_parser", None)

            if isinstance(row_condition, dict):
                row_condition = deserialize_row_condition(row_condition)

            # PassThroughCondition is not supported for SQLAlchemy
            if isinstance(row_condition, PassThroughCondition):
                raise GreatExpectationsError(  # noqa: TRY003 # FIXME
                    "PassThroughCondition (pandas/spark syntax) is not supported for "
                    "SqlAlchemyExecutionEngine. Please use the latest documented "
                    "row_condition syntax, which does not require condition_parser."
                )

            if isinstance(row_condition, Condition):
                parsed_condition = self.condition_to_filter_clause(row_condition)
            elif condition_parser in [
                CONDITION_PARSER_GREAT_EXPECTATIONS,
                CONDITION_PARSER_GREAT_EXPECTATIONS_DEPRECATED,
            ]:
                parsed_condition = parse_condition_to_sqlalchemy(row_condition)
            else:
                raise GreatExpectationsError(  # noqa: TRY003 # FIXME CoP
                    "SqlAlchemyExecutionEngine only supports the great_expectations condition_parser."  # noqa: E501 # FIXME CoP
                )
            selectable = sa.select(sa.text("*")).select_from(selectable).where(parsed_condition)  # type: ignore[arg-type] # FIXME CoP

        # Filtering by filter_conditions
        filter_conditions: List[RowCondition] = domain_kwargs.get("filter_conditions", [])
        # For SqlAlchemyExecutionEngine only one filter condition is allowed
        if len(filter_conditions) == 1:
            filter_condition = filter_conditions[0]
            assert filter_condition.condition_type == RowConditionParserType.GE, (
                "filter_condition must be of type GX for SqlAlchemyExecutionEngine"
            )

            # SQLAlchemy 2.0 deprecated select_from() from a non-Table asset without a subquery.
            # Implicit coercion of SELECT and textual SELECT constructs into FROM clauses is deprecated.  # noqa: E501 # FIXME CoP
            if not isinstance(selectable, (sa.Table, Subquery)):
                selectable = selectable.subquery()  # type: ignore[attr-defined] # FIXME CoP

            selectable = (
                sa.select(sa.text("*"))
                .select_from(selectable)  # type: ignore[arg-type] # FIXME CoP
                .where(parse_condition_to_sqlalchemy(filter_condition.condition))
            )
        elif len(filter_conditions) > 1:
            raise GreatExpectationsError(  # noqa: TRY003 # FIXME CoP
                "SqlAlchemyExecutionEngine currently only supports a single filter condition."
            )

        if "column" in domain_kwargs:
            return selectable

        # Filtering by ignore_row_if directive
        if (
            "column_A" in domain_kwargs
            and "column_B" in domain_kwargs
            and "ignore_row_if" in domain_kwargs
        ):
            if cast("SqlAlchemyBatchData", self.batch_manager.active_batch_data).use_quoted_name:
                # Checking if case-sensitive and using appropriate name
                # noinspection PyPep8Naming
                column_A_name = sqlalchemy.quoted_name(domain_kwargs["column_A"], quote=True)
                # noinspection PyPep8Naming
                column_B_name = sqlalchemy.quoted_name(domain_kwargs["column_B"], quote=True)
            else:
                # noinspection PyPep8Naming
                column_A_name = domain_kwargs["column_A"]
                # noinspection PyPep8Naming
                column_B_name = domain_kwargs["column_B"]

            ignore_row_if = domain_kwargs["ignore_row_if"]
            if ignore_row_if == "both_values_are_missing":
                selectable = get_sqlalchemy_selectable(
                    sa.select(sa.text("*"))
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type] # FIXME CoP
                    .where(
                        sa.not_(
                            sa.and_(
                                sa.column(column_A_name) == None,  # noqa: E711 # FIXME CoP
                                sa.column(column_B_name) == None,  # noqa: E711 # FIXME CoP
                            )
                        )
                    )
                )
            elif ignore_row_if == "either_value_is_missing":
                selectable = get_sqlalchemy_selectable(
                    sa.select(sa.text("*"))
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type] # FIXME CoP
                    .where(
                        sa.not_(
                            sa.or_(
                                sa.column(column_A_name) == None,  # noqa: E711 # FIXME CoP
                                sa.column(column_B_name) == None,  # noqa: E711 # FIXME CoP
                            )
                        )
                    )
                )
            else:  # noqa: PLR5501 # FIXME CoP
                if ignore_row_if != "neither":
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')  # noqa: TRY003 # FIXME CoP

            return selectable

        if "column_list" in domain_kwargs and "ignore_row_if" in domain_kwargs:
            if cast("SqlAlchemyBatchData", self.batch_manager.active_batch_data).use_quoted_name:
                # Checking if case-sensitive and using appropriate name
                column_list = [
                    sqlalchemy.quoted_name(domain_kwargs[column_name], quote=True)
                    for column_name in domain_kwargs["column_list"]
                ]
            else:
                column_list = domain_kwargs["column_list"]

            ignore_row_if = domain_kwargs["ignore_row_if"]
            if ignore_row_if == "all_values_are_missing":
                selectable = get_sqlalchemy_selectable(
                    sa.select(sa.text("*"))
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type] # FIXME CoP
                    .where(
                        sa.not_(
                            sa.and_(
                                *(
                                    sa.column(column_name) == None  # noqa: E711 # FIXME CoP
                                    for column_name in column_list
                                )
                            )
                        )
                    )
                )
            elif ignore_row_if == "any_value_is_missing":
                selectable = get_sqlalchemy_selectable(
                    sa.select(sa.text("*"))
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type] # FIXME CoP
                    .where(
                        sa.not_(
                            sa.or_(
                                *(
                                    sa.column(column_name) == None  # noqa: E711 # FIXME CoP
                                    for column_name in column_list
                                )
                            )
                        )
                    )
                )
            else:  # noqa: PLR5501 # FIXME CoP
                if ignore_row_if != "never":
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')  # noqa: TRY003 # FIXME CoP

            return selectable

        return selectable

    @override
    def get_compute_domain(
        self,
        domain_kwargs: dict,
        domain_type: Union[str, MetricDomainTypes],
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> Tuple[sqlalchemy.Selectable, dict, dict]:
        """Uses a given batch dictionary and Domain kwargs to obtain a SqlAlchemy column object.

        Args:
            domain_kwargs (dict): a dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type (str or MetricDomainTypes): an Enum value indicating which metric Domain the user would like \
            to be using, or a corresponding string value representing it.  String types include "identity", "column", \
            "column_pair", "table" and "other".  Enum types include capitalized versions of these from the class \
            MetricDomainTypes.
            accessor_keys (str iterable): keys that are part of the compute Domain but should be ignored when \
            describing the Domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            SqlAlchemy column
        """  # noqa: E501 # FIXME CoP
        partitioned_domain_kwargs: PartitionDomainKwargs = self._partition_domain_kwargs(
            domain_kwargs, domain_type, accessor_keys
        )

        selectable: sqlalchemy.Selectable = self.get_domain_records(domain_kwargs=domain_kwargs)

        return (
            selectable,
            partitioned_domain_kwargs.compute,
            partitioned_domain_kwargs.accessor,
        )

    @override
    def _partition_column_metric_domain_kwargs(  # type: ignore[override] # ExecutionEngine method is static
        self,
        domain_kwargs: dict,
        domain_type: MetricDomainTypes,
    ) -> PartitionDomainKwargs:
        """Partition domain_kwargs for column Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs partition from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """  # noqa: E501 # FIXME CoP
        assert domain_type == MetricDomainTypes.COLUMN, (
            "This method only supports MetricDomainTypes.COLUMN"
        )

        compute_domain_kwargs: dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: dict = {}

        if "column" not in compute_domain_kwargs:
            raise gx_exceptions.GreatExpectationsError(  # noqa: TRY003 # FIXME CoP
                "Column not provided in compute_domain_kwargs"
            )

        # Checking if case-sensitive and using appropriate name
        if cast("SqlAlchemyBatchData", self.batch_manager.active_batch_data).use_quoted_name:
            accessor_domain_kwargs["column"] = sqlalchemy.quoted_name(
                compute_domain_kwargs.pop("column"), quote=True
            )
        else:
            accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")

        return PartitionDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @override
    def _partition_column_pair_metric_domain_kwargs(  # type: ignore[override] # ExecutionEngine method is static
        self,
        domain_kwargs: dict,
        domain_type: MetricDomainTypes,
    ) -> PartitionDomainKwargs:
        """Partition domain_kwargs for column pair Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs partition from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """  # noqa: E501 # FIXME CoP
        assert domain_type == MetricDomainTypes.COLUMN_PAIR, (
            "This method only supports MetricDomainTypes.COLUMN_PAIR"
        )

        compute_domain_kwargs: dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: dict = {}

        if not ("column_A" in compute_domain_kwargs and "column_B" in compute_domain_kwargs):
            raise gx_exceptions.GreatExpectationsError(  # noqa: TRY003 # FIXME CoP
                "column_A or column_B not found within compute_domain_kwargs"
            )

        # Checking if case-sensitive and using appropriate name
        if cast("SqlAlchemyBatchData", self.batch_manager.active_batch_data).use_quoted_name:
            accessor_domain_kwargs["column_A"] = sqlalchemy.quoted_name(
                compute_domain_kwargs.pop("column_A"), quote=True
            )
            accessor_domain_kwargs["column_B"] = sqlalchemy.quoted_name(
                compute_domain_kwargs.pop("column_B"), quote=True
            )
        else:
            accessor_domain_kwargs["column_A"] = compute_domain_kwargs.pop("column_A")
            accessor_domain_kwargs["column_B"] = compute_domain_kwargs.pop("column_B")

        return PartitionDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @override
    def _partition_multi_column_metric_domain_kwargs(  # type: ignore[override] # ExecutionEngine method is static
        self,
        domain_kwargs: dict,
        domain_type: MetricDomainTypes,
    ) -> PartitionDomainKwargs:
        """Partition domain_kwargs for multicolumn Domain types into compute and accessor Domain kwargs.

        Args:
            domain_kwargs: A dictionary consisting of the Domain kwargs specifying which data to obtain
            domain_type: an Enum value indicating which metric Domain the user would
            like to be using.

        Returns:
            compute_domain_kwargs, accessor_domain_kwargs partition from domain_kwargs
            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs
        """  # noqa: E501 # FIXME CoP
        assert domain_type == MetricDomainTypes.MULTICOLUMN, (
            "This method only supports MetricDomainTypes.MULTICOLUMN"
        )

        compute_domain_kwargs: dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: dict = {}

        if "column_list" not in domain_kwargs:
            raise GreatExpectationsError("column_list not found within domain_kwargs")  # noqa: TRY003 # FIXME CoP

        column_list = compute_domain_kwargs.pop("column_list")

        if len(column_list) < 2:  # noqa: PLR2004 # FIXME CoP
            raise GreatExpectationsError("column_list must contain at least 2 columns")  # noqa: TRY003 # FIXME CoP

        # Checking if case-sensitive and using appropriate name
        if cast("SqlAlchemyBatchData", self.batch_manager.active_batch_data).use_quoted_name:
            accessor_domain_kwargs["column_list"] = [
                sqlalchemy.quoted_name(column_name, quote=True) for column_name in column_list
            ]
        else:
            accessor_domain_kwargs["column_list"] = column_list

        return PartitionDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @override
    def resolve_metric_bundle(
        self,
        metric_fn_bundle: Iterable[MetricComputationConfiguration],
    ) -> dict[MetricConfigurationID, MetricValue]:
        """For every metric in a set of Metrics to resolve, obtains necessary metric keyword arguments and builds
        bundles of the metrics into one large query dictionary so that they are all executed simultaneously. Will fail
        if bundling the metrics together is not possible.

            Args:
                metric_fn_bundle (Iterable[MetricComputationConfiguration]): \
                    "MetricComputationConfiguration" contains MetricProvider's MetricConfiguration (its unique identifier),
                    its metric provider function (the function that actually executes the metric), and arguments to pass
                    to metric provider function (dictionary of metrics defined in registry and corresponding arguments).

            Returns:
                A dictionary of "MetricConfiguration" IDs and their corresponding now-queried (fully resolved) values.
        """  # noqa: E501 # FIXME CoP
        resolved_metrics: dict[MetricConfigurationID, MetricValue] = {}

        res: List[sqlalchemy.Row]

        queries: list[dict] = self._organize_metrics_by_domain(
            metric_fn_bundle,
            limit=DATABRICKS_MAX_PARAMS_PER_QUERY
            if self.engine.dialect.name.lower() == GXSqlDialect.DATABRICKS
            else None,
        )

        for query in queries:
            domain_kwargs: dict = query["domain_kwargs"]
            selectable: sqlalchemy.Selectable = self.get_domain_records(domain_kwargs=domain_kwargs)

            assert len(query["select"]) == len(query["metric_ids"])

            try:
                # Note: selectable here always comes from get_domain_records(), which converts
                # any raw-SQL TextClause into a Subquery before returning (see the wrap in
                # get_domain_records itself), so selectable can never be a TextClause at this
                # point and there is no corresponding branch for it here.
                if (sqlalchemy.Select and isinstance(selectable, sqlalchemy.Select)) or (  # type: ignore[truthy-function] # FIXME CoP
                    sqlalchemy.TextualSelect and isinstance(selectable, sqlalchemy.TextualSelect)  # type: ignore[truthy-function] # FIXME CoP
                ):
                    sa_query_object = sa.select(*query["select"]).select_from(selectable.subquery())
                else:
                    sa_query_object = sa.select(*query["select"]).select_from(selectable)  # type: ignore[arg-type] # FIXME CoP

                logger.debug(f"Attempting query {sa_query_object!s}")
                res = self.execute_query(sa_query_object).fetchall()  # type: ignore[assignment] # FIXME CoP

                logger.debug(
                    f"""SqlAlchemyExecutionEngine computed {len(res[0])} metrics on domain_id \
{IDDict(domain_kwargs).to_id()}"""
                )
            except sqlalchemy.OperationalError as oe:
                exception_message: str = "An SQL execution Exception occurred.  "
                exception_traceback: str = traceback.format_exc()
                exception_message += (
                    f'{type(oe).__name__}: "{oe!s}".  Traceback: "{exception_traceback}".'
                )
                logger.error(exception_message)  # noqa: TRY400 # FIXME CoP
                raise ExecutionEngineError(message=exception_message)

            assert len(res) == 1, "all bundle-computed metrics must be single-value statistics"
            assert len(query["metric_ids"]) == len(res[0]), "unexpected number of metrics returned"

            idx: int
            metric_id: MetricConfigurationID
            for idx, metric_id in enumerate(query["metric_ids"]):
                # Converting SQL query execution results into JSON-serializable format produces simple data types,  # noqa: E501 # FIXME CoP
                # amenable for subsequent post-processing by higher-level "Metric" and "Expectation" layers.  # noqa: E501 # FIXME CoP
                resolved_metrics[metric_id] = convert_to_json_serializable(data=res[0][idx])

        return resolved_metrics

    def close(self) -> None:
        """
        Note: Will 20210729

        This is a helper function that will close and dispose Sqlalchemy objects that are used to connect to a database.
        Databases like Snowflake require the connection and engine to be instantiated and closed separately, and not
        doing so has caused problems with hanging connections.

        Currently the ExecutionEngine does not support handling connections and engine separately, and will actually
        override the engine with a connection in some cases, obfuscating what object is used to actually used by the
        ExecutionEngine to connect to the external database. This will be handled in an upcoming refactor, which will
        allow this function to eventually become:

        self.connection.close()
        self.engine.dispose()

        More background can be found here: https://github.com/great-expectations/great_expectations/pull/3104/
        """  # noqa: E501 # FIXME CoP
        if self._engine_backup:
            if self._connection:
                self._connection.close()
            self._engine_backup.dispose()
        else:
            self.engine.dispose()

    def __del__(self) -> None:
        """Ensure database connections are closed when this object is garbage collected.

        Python 3.13 raises ResourceWarning for unclosed sqlite3.Connection objects.
        Calling close() here disposes the underlying SQLAlchemy engine (and its
        connection pool) before the raw DBAPI connections are collected, preventing
        those warnings from being emitted.
        """
        try:
            self.close()
        except Exception:
            pass

    def _finalize_domain_query(
        self,
        domain_id: IDDictID,
        domain_batches: dict,
        batch_counters: dict,
        domain_kwargs_map: dict,
    ) -> tuple[IDDictID | None, dict | None, int | None]:
        """Finalize the current accumulated metrics for a domain into a query.

        This method calculates what new query entry should be added and what
        batch state should be reset for the next parameter batch.

        Returns:
            Tuple of (final_domain_id, new_query_entry, new_batch_counter)
            Returns (None, None, None) if no finalization is needed
        """
        new_query_entry = None
        new_batch_counter = None
        final_domain_id = None

        if domain_id in domain_batches and domain_batches[domain_id]["select"]:
            batch_idx = batch_counters.get(domain_id, 0)
            domain_kwargs = domain_kwargs_map[domain_id]

            if batch_idx == 0:
                final_domain_id = domain_id
            else:
                final_domain_id = IDDict({**domain_kwargs, "_batch_idx": batch_idx}).to_id()

            new_query_entry = {
                "select": domain_batches[domain_id]["select"],
                "metric_ids": domain_batches[domain_id]["metric_ids"],
                "domain_kwargs": domain_kwargs,
            }
            new_batch_counter = batch_idx + 1

            return final_domain_id, new_query_entry, new_batch_counter

        return final_domain_id, new_query_entry, new_batch_counter

    def _organize_metrics_by_domain(  # noqa: C901 # FIXME
        self, metric_fn_bundle: Iterable[MetricComputationConfiguration], limit: int | None = None
    ) -> list[dict]:
        """Organize metrics from a bundle into domain-grouped queries.

        Args:
            metric_fn_bundle: The metric bundle containing configurations to organize.
            limit: The maximum number of parameters per query.

        Returns:
            Dictionary of domain IDs mapped to query configurations
            with select expressions and metric IDs.
        """
        queries: list[dict] = []
        domain_batches: dict[IDDictID, dict] = {}
        batch_counters: dict[IDDictID, int] = {}
        domain_kwargs_map: dict[IDDictID, dict] = {}

        for bundled_metric_configuration in metric_fn_bundle:
            metric_to_resolve: MetricConfiguration = (
                bundled_metric_configuration.metric_configuration
            )
            metric_fn: Any = bundled_metric_configuration.metric_fn
            domain_kwargs: dict = bundled_metric_configuration.compute_domain_kwargs or {}
            if not isinstance(domain_kwargs, IDDict):
                domain_kwargs = IDDict(domain_kwargs)

            domain_id = domain_kwargs.to_id()
            selectable: sqlalchemy.Selectable = self.get_domain_records(domain_kwargs=domain_kwargs)

            if domain_id not in domain_batches:
                domain_batches[domain_id] = {"select": [], "metric_ids": []}
                batch_counters[domain_id] = 0
                domain_kwargs_map[domain_id] = domain_kwargs

            if limit:
                test_selects = domain_batches[domain_id]["select"] + [
                    metric_fn.label(metric_to_resolve.metric_name)
                ]
                test_param_count = self._count_query_parameters(selectable, test_selects)

                if test_param_count > limit and domain_batches[domain_id]["select"]:
                    final_domain_id, new_query_entry, new_batch_counter = (
                        self._finalize_domain_query(
                            domain_id, domain_batches, batch_counters, domain_kwargs_map
                        )
                    )
                    if final_domain_id is not None:
                        assert new_query_entry is not None
                        assert new_batch_counter is not None
                        queries.append(new_query_entry)
                        domain_batches[domain_id] = {"select": [], "metric_ids": []}
                        batch_counters[domain_id] = new_batch_counter

            alias = metric_to_resolve.metric_name

            # Prevent "Duplicated field name in view schema" SQL errors by deduplicating aliases
            existing_aliases = {
                col.name for col in domain_batches[domain_id]["select"] if hasattr(col, "name")
            }

            if alias in existing_aliases:
                suffix = 1
                while f"{alias}_{suffix}" in existing_aliases:
                    suffix += 1
                alias = f"{alias}_{suffix}"

            domain_batches[domain_id]["select"].append(metric_fn.label(alias))
            domain_batches[domain_id]["metric_ids"].append(metric_to_resolve.id)

        for domain_id in list(domain_batches.keys()):
            final_domain_id, new_query_entry, new_batch_counter = self._finalize_domain_query(
                domain_id, domain_batches, batch_counters, domain_kwargs_map
            )
            if final_domain_id is not None:
                assert new_query_entry is not None
                assert new_batch_counter is not None
                queries.append(new_query_entry)
                domain_batches[domain_id] = {"select": [], "metric_ids": []}
                batch_counters[domain_id] = new_batch_counter

        return queries

    def _count_query_parameters(self, selectable: sqlalchemy.Selectable, select_list: list) -> int:
        """Count the total number of parameters in a query with the given select expressions.

        Args:
            selectable: The base selectable object
            select_list: List of SELECT expressions to include in the query

        Returns:
            Total number of parameters that would be generated when the query is compiled
        """
        DEFAULT_PARAMS_PER_SELECT = 2  # Conservative upper bound
        # Note: selectable's sole caller passes a value from get_domain_records(), which
        # converts any raw-SQL TextClause into a Subquery before returning, so selectable can
        # never be a TextClause here and there is no corresponding branch for it.
        if isinstance(selectable, (sqlalchemy.Select, sqlalchemy.TextualSelect)):
            test_query = sa.select(*select_list).select_from(selectable.subquery())
        elif isinstance(selectable, sa.sql.FromClause):
            test_query = sa.select(*select_list).select_from(selectable)
        else:
            return len(select_list) * DEFAULT_PARAMS_PER_SELECT
        try:
            compiled = test_query.compile(dialect=self.engine.dialect)
            return len(compiled.params)
        except Exception:
            # If compilation fails, fall back to conservative upper bound estimate
            return len(select_list) * DEFAULT_PARAMS_PER_SELECT

    def _get_partitioner_method(self, partitioner_method_name: str) -> Callable:
        """Get the appropriate partitioner method from the method name.

        Args:
            partitioner_method_name: name of the partitioner to retrieve.

        Returns:
            partitioner method.
        """
        return self._data_partitioner.get_partitioner_method(partitioner_method_name)

    def execute_partitioned_query(
        self, partitioned_query: sqlalchemy.Selectable
    ) -> List[sqlalchemy.Row]:
        """Use the execution engine to run the partitioned query and fetch all of the results.

        Args:
            partitioned_query: Query to be executed as a sqlalchemy Selectable.

        Returns:
            List of row results.
        """
        if self.dialect_name == "awsathena":
            # Note: Athena does not support casting to string, only to varchar
            # but sqlalchemy currently generates a query as `CAST(colname AS STRING)` instead
            # of `CAST(colname AS VARCHAR)` with other dialects.
            partitioned_query = str(  # type: ignore[assignment] # FIXME CoP
                partitioned_query.compile(self.engine, compile_kwargs={"literal_binds": True})
            )

            pattern = re.compile(r"(CAST\(EXTRACT\(.*?\))( AS STRING\))", re.IGNORECASE)
            partitioned_query = re.sub(pattern, r"\1 AS VARCHAR)", partitioned_query)  # type: ignore[call-overload] # FIXME CoP

        return self.execute_query(partitioned_query).fetchall()  # type: ignore[return-value] # FIXME CoP

    def get_data_for_batch_identifiers(
        self,
        selectable: sqlalchemy.Selectable,
        partitioner_method_name: str,
        partitioner_kwargs: dict,
    ) -> List[dict]:
        """Build data used to construct batch identifiers for the input table using the provided partitioner config.

        Sql partitioner configurations yield the unique values that comprise a batch by introspecting your data.

        Args:
            selectable: Selectable to partition.
            partitioner_method_name: Desired partitioner method to use.
            partitioner_kwargs: Dict of directives used by the partitioner method as keyword arguments of key=value.

        Returns:
            List of dicts of the form [{column_name: {"key": value}}]
        """  # noqa: E501 # FIXME CoP
        return self._data_partitioner.get_data_for_batch_identifiers(
            execution_engine=self,
            selectable=selectable,
            partitioner_method_name=partitioner_method_name,
            partitioner_kwargs=partitioner_kwargs,
        )

    def _build_selectable_from_batch_spec(
        self, batch_spec: BatchSpec
    ) -> Union[sqlalchemy.Selectable, sqlalchemy.TextClause]:
        if batch_spec.get("query") is not None and batch_spec.get("sampling_method") is not None:
            raise ValueError(  # noqa: TRY003 # FIXME CoP
                "Sampling is not supported on query data. "
                "It is currently only supported on table data."
            )

        if "partitioner_method" in batch_spec:
            partitioner_fn: Callable = self._get_partitioner_method(
                partitioner_method_name=batch_spec["partitioner_method"]
            )
            partition_clause = partitioner_fn(
                batch_identifiers=batch_spec["batch_identifiers"],
                **batch_spec["partitioner_kwargs"],
            )

        else:  # noqa: PLR5501 # FIXME CoP
            if self.dialect_name == GXSqlDialect.SQLITE:
                partition_clause = sa.text("1 = 1")
            else:
                partition_clause = sa.true()

        # If the data_source_query_asset query needs no partitioning or sampling, we don't need to wrap it in another select statement with _subselectable. # noqa: E501
        # We just trust and execute the query provided.
        # At this point, query has already been verified as a valid "SELECT " statement in sql_datasource.py:970. # noqa: E501
        # This will prevent FROM DUAL being tacked on to the end of the intended query by sqlalchemy when using the OracleCX dialect. # noqa: E501
        if (
            batch_spec.get("query") is not None
            and batch_spec.get("sampling_method") is None
            and "partitioner_method" not in batch_spec
            and partition_clause is sa.true()
        ):
            return sa.text(batch_spec["query"])

        selectable: sqlalchemy.Selectable = self._subselectable(batch_spec)
        sampling_method: Optional[str] = batch_spec.get("sampling_method")
        if sampling_method is not None:
            if sampling_method in [
                "_sample_using_limit",
                "sample_using_limit",
                "_sample_using_random",
                "sample_using_random",
            ]:
                sampler_fn = self._data_sampler.get_sampler_method(sampling_method)
                return sampler_fn(
                    execution_engine=self,
                    batch_spec=batch_spec,
                    where_clause=partition_clause,
                )
            else:
                sampler_fn = self._data_sampler.get_sampler_method(sampling_method)
                return (
                    sa.select("*")
                    .select_from(selectable)  # type: ignore[arg-type] # FIXME CoP
                    .where(
                        sa.and_(
                            partition_clause,
                            sampler_fn(batch_spec),
                        )
                    )
                )

        return sa.select("*").select_from(selectable).where(partition_clause)  # type: ignore[arg-type] # FIXME CoP

    def _subselectable(self, batch_spec: BatchSpec) -> sqlalchemy.Selectable:
        table_name = batch_spec.get("table_name")
        query = batch_spec.get("query")
        selectable: sqlalchemy.Selectable
        if table_name:
            selectable = sa.table(table_name, schema=batch_spec.get("schema_name", None))
        else:
            if not isinstance(query, str):
                raise ValueError(f"SQL query should be a str but got {query}")  # noqa: TRY003 # FIXME CoP
            # Query is a valid SELECT query that begins with r"\w+select\w"
            stripped_query = query.lstrip()[6:].strip().rstrip(";").rstrip()
            selectable = sa.select(
                sa.text(_ensure_sql_text_ends_with_newline(stripped_query))
            ).subquery()

        return selectable

    @override
    def get_batch_data_and_markers(
        self, batch_spec: BatchSpec
    ) -> Tuple[SqlAlchemyBatchData, BatchMarkers]:
        # The inspector caches everything it reflects, and this execution engine is reused
        # across validations. A batch fetched after the table's schema changed must not be
        # described by the column list reflected for an earlier one, so the inspector is
        # rebuilt lazily on the next request. Within one batch it is still reflected once.
        self._inspector = None
        if not isinstance(batch_spec, (SqlAlchemyDatasourceBatchSpec, RuntimeQueryBatchSpec)):
            raise InvalidBatchSpecError(  # noqa: TRY003 # FIXME CoP
                f"""SqlAlchemyExecutionEngine accepts batch_spec only of type SqlAlchemyDatasourceBatchSpec or
        RuntimeQueryBatchSpec (illegal type "{type(batch_spec)!s}" was received).
                        """  # noqa: E501 # FIXME CoP
            )
        if sum(1 if x else 0 for x in [batch_spec.get("query"), batch_spec.get("table_name")]) != 1:
            raise InvalidBatchSpecError(  # noqa: TRY003 # FIXME CoP
                "SqlAlchemyExecutionEngine only accepts a batch_spec where exactly 1 of "
                "'query' or 'table_name' is specified. "
                f"table_name={batch_spec.get('table_name')}, query={batch_spec.get('query')}"
            )

        batch_data: Optional[SqlAlchemyBatchData] = None
        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )
        temp_table_schema_name: Optional[str] = batch_spec.get("temp_table_schema_name")

        source_schema_name: Optional[str] = batch_spec.get("schema_name", None)
        source_table_name: Optional[str] = batch_spec.get("table_name", None)

        create_temp_table: bool = batch_spec.get("create_temp_table", self._create_temp_table)
        # this is where partitioner components are added to the selectable
        selectable: sqlalchemy.Selectable | sqlalchemy.TextClause = (
            self._build_selectable_from_batch_spec(batch_spec=batch_spec)
        )
        # NOTE: what's being checked here is the presence of a `query` attribute, we could check this directly  # noqa: E501 # FIXME CoP
        # instead of doing an instance check
        if isinstance(batch_spec, RuntimeQueryBatchSpec):
            # query != None is already checked when RuntimeQueryBatchSpec is instantiated
            # re-compile the query to include any new parameters
            compiled_query = selectable.compile(
                dialect=self.engine.dialect,
                compile_kwargs={"literal_binds": True},
            )
            query_str = str(compiled_query)
            batch_data = SqlAlchemyBatchData(
                execution_engine=self,
                query=query_str,
                temp_table_schema_name=temp_table_schema_name,
                create_temp_table=create_temp_table,
            )
        elif isinstance(batch_spec, SqlAlchemyDatasourceBatchSpec):
            batch_data = SqlAlchemyBatchData(
                execution_engine=self,
                selectable=selectable,
                create_temp_table=create_temp_table,
                source_table_name=source_table_name,
                source_schema_name=source_schema_name,
            )

        return batch_data, batch_markers

    def get_inspector(self) -> sqlalchemy.engine.reflection.Inspector:
        if self._inspector is None:
            if version.parse(sa.__version__) < version.parse("1.4"):
                # Inspector.from_engine deprecated since 1.4, sa.inspect() should be used instead
                self._inspector = sqlalchemy.reflection.Inspector.from_engine(self.engine)  # type: ignore[assignment] # FIXME CoP
            else:
                self._inspector = sa.inspect(self.engine)  # type: ignore[assignment] # FIXME CoP

        return self._inspector  # type: ignore[return-value] # FIXME CoP

    @contextmanager
    def get_connection(self) -> Generator[sqlalchemy.Connection, None, None]:
        """Get a connection for executing queries.

        Some databases sqlite/SQL Server temp tables only persist within a connection,
        so we need to keep the connection alive by keeping a reference to it.
        Even though we use a single connection pool for dialects that need a single persisted connection
        (e.g. for accessing temporary tables), if we don't keep a reference
        then we get errors like sqlite3.ProgrammingError: Cannot operate on a closed database.

        Returns:
            Sqlalchemy connection
        """  # noqa: E501 # FIXME CoP
        if self.dialect_name in _PERSISTED_CONNECTION_DIALECTS:
            try:
                if not self._connection:
                    self._connection = self.engine.connect()
                yield self._connection
            finally:
                # Temp tables only persist within a connection for some dialects,
                # so we need to keep the connection alive.
                pass
        else:
            with self.engine.connect() as connection:
                yield connection

    @staticmethod
    def _execute_query_with_recovery(
        connection: sqlalchemy.Connection,
        query: sqlalchemy.Selectable | sqlalchemy.TextClause,
    ) -> sqlalchemy.CursorResult | sqlalchemy.LegacyCursorResult:
        """Execute a query with automatic recovery from invalid transaction state.

        This handles PendingRollbackError which was introduced in SQLAlchemy 2.0.
        For SQLAlchemy 1.x, this error doesn't exist and won't be raised.

        Args:
            connection: SQLAlchemy connection to use
            query: Sqlalchemy selectable query.

        Returns:
            CursorResult for sqlalchemy 2.0+ or LegacyCursorResult for earlier versions.
        """
        try:
            return connection.execute(query)  # type: ignore[arg-type] # Selectable union type too broad
        except PendingRollbackError:
            # Connection has an invalid transaction from a previous failed operation
            # Roll back and retry with the same connection
            connection.rollback()
            try:
                return connection.execute(query)  # type: ignore[arg-type] # Selectable union type too broad
            except Exception:
                # Retry also failed - roll back again so the connection is left clean.
                # Without this, the deactivated transaction causes pyodbc to issue a
                # blocking ROLLBACK when the persistent SQL Server connection is closed
                # during teardown, which causes intermittent test hangs.
                try:
                    connection.rollback()
                except Exception:
                    pass
                raise

    @new_method_or_class(version="0.16.14")
    def execute_query(
        self, query: sqlalchemy.Selectable | sqlalchemy.TextClause
    ) -> sqlalchemy.CursorResult | sqlalchemy.LegacyCursorResult:
        """Execute a query using the underlying database engine.

        Args:
            query: Sqlalchemy selectable query.

        Returns:
            CursorResult for sqlalchemy 2.0+ or LegacyCursorResult for earlier versions.
        """
        with self.get_connection() as connection:
            result = self._execute_query_with_recovery(connection, query)

        return result

    @staticmethod
    def _connection_has_transaction(connection: sqlalchemy.Connection) -> bool:
        """Check if a connection has an active transaction.

        This is specifically for SQLAlchemy 2.0+ autobegin behavior where connections
        might not have an active transaction if the database is in autocommit mode.

        Args:
            connection: SQLAlchemy connection to check

        Returns:
            True if there's an active transaction, False otherwise
        """
        # This method is only called in the SQLAlchemy 2.0+ code path
        # The in_transaction() method was added in 1.4, but we check for 2.0+
        # because that's when autobegin behavior was introduced
        if is_version_greater_or_equal(sqlalchemy.sqlalchemy.__version__, "2.0.0"):
            return connection.in_transaction()
        # For SQLAlchemy < 2.0, we use explicit connection.begin(), so always in transaction
        return True

    @new_method_or_class(version="0.16.14")
    def execute_query_in_transaction(
        self, query: sqlalchemy.Selectable
    ) -> sqlalchemy.CursorResult | sqlalchemy.LegacyCursorResult:
        """Execute a query using the underlying database engine within a transaction
        that will auto commit.

        Begin once: https://docs.sqlalchemy.org/en/20/core/connections.html#begin-once

        Args:
            query: Sqlalchemy selectable query.

        Returns:
            CursorResult for sqlalchemy 2.0+ or LegacyCursorResult for earlier versions.
        """
        with self.get_connection() as connection:
            if (
                is_version_greater_or_equal(sqlalchemy.sqlalchemy.__version__, "2.0.0")
                and not connection.closed
            ):
                result = self._execute_query_with_recovery(connection, query)

                # Some databases auto-commit and don't support explicit transaction management
                # Try to commit, but ignore errors from databases that auto-commit
                if self._connection_has_transaction(connection):
                    try:
                        connection.commit()
                    except DatabaseError as e:
                        # Databricks and other auto-commit databases may not have
                        # an active transaction even though in_transaction() returns True
                        if "no active transaction" not in str(e).lower():
                            raise
            else:
                with connection.begin():
                    result = self._execute_query_with_recovery(connection, query)

        return result

    @override
    def condition_to_filter_clause(self, condition: Condition) -> sa.ColumnElement:
        # This override is just to help the type system,
        # since we can't make the class generic on sqlalchemy
        # since it's not installed in all environments."""
        output = super().condition_to_filter_clause(condition)
        if not isinstance(output, ColumnElement):
            raise InvalidFilterClause(output)
        return output

    @override
    def _comparison_condition_to_filter_clause(  # noqa: C901, PLR0911
        self, condition: ComparisonCondition
    ) -> sa.ColumnElement:
        col: sa.ColumnClause = sa.column(condition.column.name)
        val = sa.literal(condition.parameter)
        op = condition.operator
        if op == Operator.LESS_THAN:
            return col < val
        elif op == Operator.LESS_THAN_OR_EQUAL:
            return col <= val
        elif op == Operator.EQUAL:
            return col == val
        elif op == Operator.NOT_EQUAL:
            return col != val
        elif op == Operator.GREATER_THAN:
            return col > val
        elif op == Operator.GREATER_THAN_OR_EQUAL:
            return col >= val
        elif op == Operator.IN:
            return col.in_(condition.parameter)
        elif op == Operator.NOT_IN:
            return ~col.in_(condition.parameter)
        else:
            raise InvalidOperatorError(op)

    @override
    def _nullity_condition_to_filter_clause(self, condition: NullityCondition) -> sa.ColumnElement:
        col: sa.ColumnClause = sa.column(condition.column.name)
        return col.is_(None) if condition.is_null else col.isnot(None)

    @override
    def _and_condition_to_filter_clause(self, condition: AndCondition) -> sa.ColumnElement:
        output = sa.and_(*[self.condition_to_filter_clause(c) for c in condition.conditions])
        return output

    @override
    def _or_condition_to_filter_clause(self, condition: OrCondition) -> sa.ColumnElement:
        return sa.or_(*[self.condition_to_filter_clause(c) for c in condition.conditions])
