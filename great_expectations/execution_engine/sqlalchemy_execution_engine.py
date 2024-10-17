from __future__ import annotations

import copy
import datetime
import hashlib
import logging
import math
import os
import random
import re
import string
import traceback
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
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
from great_expectations.compatibility import aws, snowflake, sqlalchemy, trino
from great_expectations.compatibility.not_imported import is_version_greater_or_equal
from great_expectations.compatibility.sqlalchemy import Subquery
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
from great_expectations.expectations.model_field_types import ConditionParser
from great_expectations.util import convert_to_json_serializable  # noqa: TID251
from great_expectations.validator.computed_metric import MetricValue  # noqa: TCH001

del get_versions  # isort:skip


from great_expectations.core import IDDict
from great_expectations.core.batch import BatchMarkers, BatchSpec
from great_expectations.core.batch_spec import (
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
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
from great_expectations.expectations.row_conditions import (
    RowCondition,
    RowConditionParserType,
    parse_condition_to_sqlalchemy,
)
from great_expectations.util import (
    filter_properties_dict,
    get_sqlalchemy_selectable,
    get_sqlalchemy_url,
    import_library_module,
    import_make_url,
)
from great_expectations.validator.metric_configuration import (
    MetricConfiguration,  # noqa: TCH001
)

logger = logging.getLogger(__name__)


if sa:
    make_url = import_make_url()


try:
    import psycopg2  # noqa: F401
    import sqlalchemy.dialects.postgresql.psycopg2 as sqlalchemy_psycopg2  # noqa: TID251
except (ImportError, KeyError):
    sqlalchemy_psycopg2 = None  # type: ignore[assignment]

try:
    import sqlalchemy_dremio.pyodbc

    if sa:
        sa.dialects.registry.register(GXSqlDialect.DREMIO, "sqlalchemy_dremio.pyodbc", "dialect")  # type: ignore[arg-type]
except ImportError:
    sqlalchemy_dremio = None

if snowflake.snowflakedialect:
    if sa:
        # Sometimes "snowflake-sqlalchemy" fails to self-register in certain environments, so we do it explicitly.  # noqa: E501
        # (see https://stackoverflow.com/questions/53284762/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectssnowflake)
        sa.dialects.registry.register(GXSqlDialect.SNOWFLAKE, "snowflake.sqlalchemy", "dialect")  # type: ignore[arg-type]

from great_expectations.compatibility.bigquery import (
    _BIGQUERY_MODULE_NAME,
    bigquery_types_tuple,
)
from great_expectations.compatibility.bigquery import (
    sqlalchemy_bigquery as sqla_bigquery,
)

if sqla_bigquery and sa:
    sa.dialects.registry.register(GXSqlDialect.BIGQUERY, _BIGQUERY_MODULE_NAME, "BigQueryDialect")  # type: ignore[arg-type]

try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None
    teradatatypes = None

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine as SaEngine  # noqa: TID251


def _get_dialect_type_module(dialect):  # noqa: C901
    """Given a dialect, returns the dialect type, which is defines the engine/system that is used to communicates
    with the database/database implementation. Currently checks for RedShift/BigQuery dialects
    """  # noqa: E501
    if dialect is None:
        logger.warning("No sqlalchemy dialect found; relying in top-level sqlalchemy types.")
        return sa

    # Redshift does not (yet) export types to top level; only recognize base SA types
    if aws.redshiftdialect and isinstance(dialect, aws.redshiftdialect.RedshiftDialect):
        # noinspection PyUnresolvedReferences
        return dialect.sa
    else:
        pass

    # Bigquery works with newer versions, but use a patch if we had to define bigquery_types_tuple
    try:
        if (
            isinstance(
                dialect,
                sqla_bigquery,
            )
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    # Teradata types module
    try:
        if (
            issubclass(
                dialect,
                teradatasqlalchemy.dialect.TeradataDialect,
            )
            and teradatatypes is not None
        ):
            return teradatatypes
    except (TypeError, AttributeError):
        pass

    # Trino types module
    try:
        if (
            isinstance(
                dialect,
                trino.trinodialect.TrinoDialect,
            )
            and trino.trinotypes is not None
        ):
            return trino.trinotypes
    except (TypeError, AttributeError):
        pass

    return dialect


_PERSISTED_CONNECTION_DIALECTS = (
    GXSqlDialect.SQLITE,
    GXSqlDialect.MSSQL,
    GXSqlDialect.BIGQUERY,
)


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
        raise ValueError("Exactly one of connection_string, credentials, url must be specified")  # noqa: TRY003
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


class SqlAlchemyExecutionEngine(ExecutionEngine):
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
            not persisted if the connection is closed (e.g. sqlite, mssql) then you should create the engine with \
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
    """  # noqa: E501

    # noinspection PyUnusedLocal
    def __init__(  # noqa: C901, PLR0912, PLR0913, PLR0915
        self,
        name: Optional[str] = None,
        credentials: Optional[dict] = None,
        data_context: Optional[Any] = None,
        engine: Optional[SaEngine] = None,
        connection_string: Optional[str] = None,
        url: Optional[str] = None,
        batch_data_dict: Optional[dict] = None,
        create_temp_table: bool = True,
        # kwargs will be passed as optional parameters to the SQLAlchemy engine, **not** the ExecutionEngine  # noqa: E501
        **kwargs,
    ) -> None:
        super().__init__(name=name, batch_data_dict=batch_data_dict)
        self._name = name

        self._credentials = credentials
        self._connection_string = connection_string
        self._url = url
        self._create_temp_table = create_temp_table
        os.environ["SF_PARTNER"] = "great_expectations_oss"  # noqa: TID251

        # sqlite/mssql temp tables only persist within a connection, so we need to keep the connection alive by  # noqa: E501
        # keeping a reference to it.
        # Even though we use a single connection pool for dialects that need a single persisted connection  # noqa: E501
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
                    "Both credentials and engine were provided during initialization of SqlAlchemyExecutionEngine. "  # noqa: E501
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

        # these are two backends where temp_table_creation is not supported we set the default value to False.  # noqa: E501
        if (
            self.dialect_name
            in [
                GXSqlDialect.TRINO,
                GXSqlDialect.AWSATHENA,  # WKS 202201 - AWS Athena currently doesn't support temp_tables.  # noqa: E501
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
            GXSqlDialect.MSSQL,
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
        else:
            self.dialect_module = None

        # <WILL> 20210726 - engine_backup is used by the snowflake connector, which requires connection and engine  # noqa: E501
        # to be closed and disposed separately. Currently self.engine can refer to either a Connection or Engine,  # noqa: E501
        # depending on the backend. This will need to be cleaned up in an upcoming refactor, so that Engine and  # noqa: E501
        # Connection can be handled separately.
        self._engine_backup = None
        if self.engine and self.dialect_name in [
            GXSqlDialect.SQLITE,
            GXSqlDialect.MSSQL,
            GXSqlDialect.SNOWFLAKE,
            GXSqlDialect.MYSQL,
        ]:
            if self.engine.dialect.name.lower() == GXSqlDialect.SQLITE:

                def _add_sqlite_functions(connection):
                    logger.info(f"Adding custom sqlite functions to connection {connection}")
                    connection.create_function("sqrt", 1, lambda x: math.sqrt(x))
                    connection.create_function(
                        "md5",
                        2,
                        lambda x, d: hashlib.md5(str(x).encode("utf-8")).hexdigest()[-1 * d :],
                    )

                # Add sqlite functions to any future connections.
                def _on_connect(dbapi_con, connection_record):
                    logger.info(
                        f"A new sqlite connection was created: {dbapi_con}, {connection_record}"
                    )
                    _add_sqlite_functions(dbapi_con)

                sa.event.listen(self.engine, "connect", _on_connect)
                # Also immediately add the sqlite functions in case there already exists an underlying  # noqa: E501
                # sqlite3.Connection (distinct from a sqlalchemy Connection).
                _add_sqlite_functions(self.engine.raw_connection())
            self._engine_backup = self.engine

        # Gather the call arguments of the present function (and add the "class_name"), filter out the Falsy values,  # noqa: E501
        # and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "name": name,
            "credentials": credentials,
            "data_context": data_context,
            "engine": engine,  # type: ignore[dict-item]
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
        """  # noqa: E501
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
            raise InvalidConfigError(  # noqa: TRY003
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
        """  # noqa: E501
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
        """  # noqa: E501
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        private_key_path = credentials.pop("private_key_path")
        private_key_passphrase = credentials.pop("private_key_passphrase")

        with Path(private_key_path).expanduser().resolve().open(mode="rb") as key:
            try:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=private_key_passphrase.encode() if private_key_passphrase else None,
                    backend=default_backend(),
                )
            except ValueError as e:
                if "incorrect password" in str(e).lower():
                    raise DatasourceKeyPairAuthBadPassphraseError(
                        datasource_name="SqlAlchemyDatasource",
                        message="Decryption of key failed, was the passphrase incorrect?",
                    ) from e
                else:
                    raise e  # noqa: TRY201
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
    def get_domain_records(  # noqa: C901, PLR0912, PLR0915
        self,
        domain_kwargs: dict,
    ) -> sqlalchemy.Selectable:
        """Uses the given Domain kwargs (which include row_condition, condition_parser, and ignore_row_if directives) to obtain and/or query a Batch of data.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the Domain kwargs specifying which data to obtain

        Returns:
            An SqlAlchemy table/column(s) (the selectable object for obtaining data on which to compute returned in the format of an SqlAlchemy table/column(s) object)
        """  # noqa: E501
        data_object: SqlAlchemyBatchData

        batch_id: Optional[str] = domain_kwargs.get("batch_id")
        if batch_id is None:
            # We allow no batch id specified if there is only one batch
            if self.batch_manager.active_batch_data:
                data_object = cast(SqlAlchemyBatchData, self.batch_manager.active_batch_data)
            else:
                raise GreatExpectationsError(  # noqa: TRY003
                    "No batch is specified, but could not identify a loaded batch."
                )
        else:  # noqa: PLR5501
            if batch_id in self.batch_manager.batch_data_cache:
                data_object = cast(
                    SqlAlchemyBatchData, self.batch_manager.batch_data_cache[batch_id]
                )
            else:
                raise GreatExpectationsError(f"Unable to find batch with batch_id {batch_id}")  # noqa: TRY003

        selectable: sqlalchemy.Selectable
        if "table" in domain_kwargs and domain_kwargs["table"] is not None:
            # TODO: Add logic to handle record_set_name once implemented
            # (i.e. multiple record sets (tables) in one batch
            if domain_kwargs["table"] != data_object.selectable.name:
                # noinspection PyProtectedMember
                selectable = sa.Table(
                    domain_kwargs["table"],
                    sa.MetaData(),
                    schema=data_object._schema_name,
                )
            else:
                selectable = data_object.selectable
        elif "query" in domain_kwargs:
            raise ValueError("query is not currently supported by SqlAlchemyExecutionEngine")  # noqa: TRY003
        else:
            selectable = data_object.selectable

        """
        If a custom query is passed, selectable will be TextClause and not formatted
        as a subquery wrapped in "(subquery) alias". TextClause must first be converted
        to TextualSelect using sa.columns() before it can be converted to type Subquery
        """
        if sqlalchemy.TextClause and isinstance(selectable, sqlalchemy.TextClause):  # type: ignore[truthy-function]
            selectable = selectable.columns().subquery()

        # Filtering by row condition.
        if "row_condition" in domain_kwargs and domain_kwargs["row_condition"] is not None:
            condition_parser = domain_kwargs["condition_parser"]
            if condition_parser in [ConditionParser.GX, ConditionParser.GX_DEPRECATED]:
                parsed_condition = parse_condition_to_sqlalchemy(domain_kwargs["row_condition"])
                selectable = sa.select(sa.text("*")).select_from(selectable).where(parsed_condition)  # type: ignore[arg-type]
            else:
                raise GreatExpectationsError(  # noqa: TRY003
                    "SqlAlchemyExecutionEngine only supports the great_expectations condition_parser."  # noqa: E501
                )

        # Filtering by filter_conditions
        filter_conditions: List[RowCondition] = domain_kwargs.get("filter_conditions", [])
        # For SqlAlchemyExecutionEngine only one filter condition is allowed
        if len(filter_conditions) == 1:
            filter_condition = filter_conditions[0]
            assert (
                filter_condition.condition_type == RowConditionParserType.GE
            ), "filter_condition must be of type GX for SqlAlchemyExecutionEngine"

            # SQLAlchemy 2.0 deprecated select_from() from a non-Table asset without a subquery.
            # Implicit coercion of SELECT and textual SELECT constructs into FROM clauses is deprecated.  # noqa: E501
            if not isinstance(selectable, (sa.Table, Subquery)):
                selectable = selectable.subquery()  # type: ignore[attr-defined]

            selectable = (
                sa.select(sa.text("*"))
                .select_from(selectable)  # type: ignore[arg-type]
                .where(parse_condition_to_sqlalchemy(filter_condition.condition))
            )
        elif len(filter_conditions) > 1:
            raise GreatExpectationsError(  # noqa: TRY003
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
            if cast(SqlAlchemyBatchData, self.batch_manager.active_batch_data).use_quoted_name:
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
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type]
                    .where(
                        sa.not_(
                            sa.and_(
                                sa.column(column_A_name) == None,  # noqa: E711
                                sa.column(column_B_name) == None,  # noqa: E711
                            )
                        )
                    )
                )
            elif ignore_row_if == "either_value_is_missing":
                selectable = get_sqlalchemy_selectable(
                    sa.select(sa.text("*"))
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type]
                    .where(
                        sa.not_(
                            sa.or_(
                                sa.column(column_A_name) == None,  # noqa: E711
                                sa.column(column_B_name) == None,  # noqa: E711
                            )
                        )
                    )
                )
            else:  # noqa: PLR5501
                if ignore_row_if != "neither":
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')  # noqa: TRY003

            return selectable

        if "column_list" in domain_kwargs and "ignore_row_if" in domain_kwargs:
            if cast(SqlAlchemyBatchData, self.batch_manager.active_batch_data).use_quoted_name:
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
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type]
                    .where(
                        sa.not_(
                            sa.and_(
                                *(
                                    sa.column(column_name) == None  # noqa: E711
                                    for column_name in column_list
                                )
                            )
                        )
                    )
                )
            elif ignore_row_if == "any_value_is_missing":
                selectable = get_sqlalchemy_selectable(
                    sa.select(sa.text("*"))
                    .select_from(get_sqlalchemy_selectable(selectable))  # type: ignore[arg-type]
                    .where(
                        sa.not_(
                            sa.or_(
                                *(
                                    sa.column(column_name) == None  # noqa: E711
                                    for column_name in column_list
                                )
                            )
                        )
                    )
                )
            else:  # noqa: PLR5501
                if ignore_row_if != "never":
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')  # noqa: TRY003

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
        """  # noqa: E501
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
        """  # noqa: E501
        assert (
            domain_type == MetricDomainTypes.COLUMN
        ), "This method only supports MetricDomainTypes.COLUMN"

        compute_domain_kwargs: dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: dict = {}

        if "column" not in compute_domain_kwargs:
            raise gx_exceptions.GreatExpectationsError(  # noqa: TRY003
                "Column not provided in compute_domain_kwargs"
            )

        # Checking if case-sensitive and using appropriate name
        if cast(SqlAlchemyBatchData, self.batch_manager.active_batch_data).use_quoted_name:
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
        """  # noqa: E501
        assert (
            domain_type == MetricDomainTypes.COLUMN_PAIR
        ), "This method only supports MetricDomainTypes.COLUMN_PAIR"

        compute_domain_kwargs: dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: dict = {}

        if not ("column_A" in compute_domain_kwargs and "column_B" in compute_domain_kwargs):
            raise gx_exceptions.GreatExpectationsError(  # noqa: TRY003
                "column_A or column_B not found within compute_domain_kwargs"
            )

        # Checking if case-sensitive and using appropriate name
        if cast(SqlAlchemyBatchData, self.batch_manager.active_batch_data).use_quoted_name:
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
        """  # noqa: E501
        assert (
            domain_type == MetricDomainTypes.MULTICOLUMN
        ), "This method only supports MetricDomainTypes.MULTICOLUMN"

        compute_domain_kwargs: dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: dict = {}

        if "column_list" not in domain_kwargs:
            raise GreatExpectationsError("column_list not found within domain_kwargs")  # noqa: TRY003

        column_list = compute_domain_kwargs.pop("column_list")

        if len(column_list) < 2:  # noqa: PLR2004
            raise GreatExpectationsError("column_list must contain at least 2 columns")  # noqa: TRY003

        # Checking if case-sensitive and using appropriate name
        if cast(SqlAlchemyBatchData, self.batch_manager.active_batch_data).use_quoted_name:
            accessor_domain_kwargs["column_list"] = [
                sqlalchemy.quoted_name(column_name, quote=True) for column_name in column_list
            ]
        else:
            accessor_domain_kwargs["column_list"] = column_list

        return PartitionDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    @override
    def resolve_metric_bundle(  # noqa: C901 - too complex
        self,
        metric_fn_bundle: Iterable[MetricComputationConfiguration],
    ) -> Dict[Tuple[str, str, str], MetricValue]:
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
        """  # noqa: E501
        resolved_metrics: Dict[Tuple[str, str, str], MetricValue] = {}

        res: List[sqlalchemy.Row]

        # We need a different query for each Domain (where clause).
        queries: Dict[Tuple[str, str, str], dict] = {}

        query: dict

        domain_id: Tuple[str, str, str]

        bundled_metric_configuration: MetricComputationConfiguration
        for bundled_metric_configuration in metric_fn_bundle:
            metric_to_resolve: MetricConfiguration = (
                bundled_metric_configuration.metric_configuration
            )
            metric_fn: Any = bundled_metric_configuration.metric_fn
            compute_domain_kwargs: dict = bundled_metric_configuration.compute_domain_kwargs or {}
            if not isinstance(compute_domain_kwargs, IDDict):
                compute_domain_kwargs = IDDict(compute_domain_kwargs)

            domain_id = compute_domain_kwargs.to_id()
            if domain_id not in queries:
                queries[domain_id] = {
                    "select": [],
                    "metric_ids": [],
                    "domain_kwargs": compute_domain_kwargs,
                }

            if self.engine.dialect.name == "clickhouse":
                queries[domain_id]["select"].append(
                    metric_fn.label(
                        metric_to_resolve.metric_name.join(
                            random.choices(string.ascii_lowercase, k=4)
                        )
                    )
                )
            else:
                queries[domain_id]["select"].append(metric_fn.label(metric_to_resolve.metric_name))

            queries[domain_id]["metric_ids"].append(metric_to_resolve.id)

        for query in queries.values():
            domain_kwargs: dict = query["domain_kwargs"]
            selectable: sqlalchemy.Selectable = self.get_domain_records(domain_kwargs=domain_kwargs)

            assert len(query["select"]) == len(query["metric_ids"])

            try:
                """
                If a custom query is passed, selectable will be TextClause and not formatted
                as a subquery wrapped in "(subquery) alias". TextClause must first be converted
                to TextualSelect using sa.columns() before it can be converted to type Subquery
                """
                if sqlalchemy.TextClause and isinstance(selectable, sqlalchemy.TextClause):  # type: ignore[truthy-function]
                    sa_query_object = sa.select(*query["select"]).select_from(
                        selectable.columns().subquery()
                    )
                elif (sqlalchemy.Select and isinstance(selectable, sqlalchemy.Select)) or (  # type: ignore[truthy-function]
                    sqlalchemy.TextualSelect and isinstance(selectable, sqlalchemy.TextualSelect)  # type: ignore[truthy-function]
                ):
                    sa_query_object = sa.select(*query["select"]).select_from(selectable.subquery())
                else:
                    sa_query_object = sa.select(*query["select"]).select_from(selectable)  # type: ignore[arg-type]

                logger.debug(f"Attempting query {sa_query_object!s}")
                res = self.execute_query(sa_query_object).fetchall()  # type: ignore[assignment]

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
                logger.error(exception_message)  # noqa: TRY400
                raise ExecutionEngineError(message=exception_message)

            assert len(res) == 1, "all bundle-computed metrics must be single-value statistics"
            assert len(query["metric_ids"]) == len(res[0]), "unexpected number of metrics returned"

            idx: int
            metric_id: Tuple[str, str, str]
            for idx, metric_id in enumerate(query["metric_ids"]):
                # Converting SQL query execution results into JSON-serializable format produces simple data types,  # noqa: E501
                # amenable for subsequent post-processing by higher-level "Metric" and "Expectation" layers.  # noqa: E501
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
        """  # noqa: E501
        if self._engine_backup:
            if self._connection:
                self._connection.close()
            self._engine_backup.dispose()
        else:
            self.engine.dispose()

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
            partitioned_query = str(  # type: ignore[assignment]
                partitioned_query.compile(self.engine, compile_kwargs={"literal_binds": True})
            )

            pattern = re.compile(r"(CAST\(EXTRACT\(.*?\))( AS STRING\))", re.IGNORECASE)
            partitioned_query = re.sub(pattern, r"\1 AS VARCHAR)", partitioned_query)  # type: ignore[call-overload]

        return self.execute_query(partitioned_query).fetchall()  # type: ignore[return-value]

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
        """  # noqa: E501
        return self._data_partitioner.get_data_for_batch_identifiers(
            execution_engine=self,
            selectable=selectable,
            partitioner_method_name=partitioner_method_name,
            partitioner_kwargs=partitioner_kwargs,
        )

    def _build_selectable_from_batch_spec(self, batch_spec: BatchSpec) -> sqlalchemy.Selectable:
        if batch_spec.get("query") is not None and batch_spec.get("sampling_method") is not None:
            raise ValueError(  # noqa: TRY003
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

        else:  # noqa: PLR5501
            if self.dialect_name == GXSqlDialect.SQLITE:
                partition_clause = sa.text("1 = 1")
            else:
                partition_clause = sa.true()

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
                    .select_from(selectable)  # type: ignore[arg-type]
                    .where(
                        sa.and_(
                            partition_clause,
                            sampler_fn(batch_spec),
                        )
                    )
                )

        return sa.select("*").select_from(selectable).where(partition_clause)  # type: ignore[arg-type]

    def _subselectable(self, batch_spec: BatchSpec) -> sqlalchemy.Selectable:
        table_name = batch_spec.get("table_name")
        query = batch_spec.get("query")
        selectable: sqlalchemy.Selectable
        if table_name:
            selectable = sa.table(table_name, schema=batch_spec.get("schema_name", None))
        else:
            if not isinstance(query, str):
                raise ValueError(f"SQL query should be a str but got {query}")  # noqa: TRY003
            # Query is a valid SELECT query that begins with r"\w+select\w"
            selectable = sa.select(
                sa.text(query.lstrip()[6:].strip().rstrip(";").rstrip())
            ).subquery()

        return selectable

    @override
    def get_batch_data_and_markers(
        self, batch_spec: BatchSpec
    ) -> Tuple[SqlAlchemyBatchData, BatchMarkers]:
        if not isinstance(batch_spec, (SqlAlchemyDatasourceBatchSpec, RuntimeQueryBatchSpec)):
            raise InvalidBatchSpecError(  # noqa: TRY003
                f"""SqlAlchemyExecutionEngine accepts batch_spec only of type SqlAlchemyDatasourceBatchSpec or
        RuntimeQueryBatchSpec (illegal type "{type(batch_spec)!s}" was received).
                        """  # noqa: E501
            )
        if sum(1 if x else 0 for x in [batch_spec.get("query"), batch_spec.get("table_name")]) != 1:
            raise InvalidBatchSpecError(  # noqa: TRY003
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

        source_schema_name: str = batch_spec.get("schema_name", None)
        source_table_name: str = batch_spec.get("table_name", None)

        create_temp_table: bool = batch_spec.get("create_temp_table", self._create_temp_table)
        # this is where partitioner components are added to the selectable
        selectable: sqlalchemy.Selectable = self._build_selectable_from_batch_spec(
            batch_spec=batch_spec
        )
        # NOTE: what's being checked here is the presence of a `query` attribute, we could check this directly  # noqa: E501
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
                self._inspector = sqlalchemy.reflection.Inspector.from_engine(self.engine)  # type: ignore[assignment]
            else:
                self._inspector = sa.inspect(self.engine)  # type: ignore[assignment]

        return self._inspector  # type: ignore[return-value]

    @contextmanager
    def get_connection(self) -> Generator[sqlalchemy.Connection, None, None]:
        """Get a connection for executing queries.

        Some databases sqlite/mssql temp tables only persist within a connection,
        so we need to keep the connection alive by keeping a reference to it.
        Even though we use a single connection pool for dialects that need a single persisted connection
        (e.g. for accessing temporary tables), if we don't keep a reference
        then we get errors like sqlite3.ProgrammingError: Cannot operate on a closed database.

        Returns:
            Sqlalchemy connection
        """  # noqa: E501
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

    @new_method_or_class(version="0.16.14")
    def execute_query(
        self, query: sqlalchemy.Selectable
    ) -> sqlalchemy.CursorResult | sqlalchemy.LegacyCursorResult:
        """Execute a query using the underlying database engine.

        Args:
            query: Sqlalchemy selectable query.

        Returns:
            CursorResult for sqlalchemy 2.0+ or LegacyCursorResult for earlier versions.
        """
        with self.get_connection() as connection:
            result = connection.execute(query)  # type: ignore[call-overload] # FIXME:Selectable overly broad

        return result

    @new_method_or_class(version="0.16.14")
    def execute_query_in_transaction(
        self, query: sqlalchemy.Selectable
    ) -> sqlalchemy.CursorResult | sqlalchemy.LegacyCursorResult:
        """Execute a query using the underlying database engine within a transaction that will auto commit.

        Begin once: https://docs.sqlalchemy.org/en/20/core/connections.html#begin-once

        Args:
            query: Sqlalchemy selectable query.

        Returns:
            CursorResult for sqlalchemy 2.0+ or LegacyCursorResult for earlier versions.
        """  # noqa: E501
        with self.get_connection() as connection:
            if (
                is_version_greater_or_equal(sqlalchemy.sqlalchemy.__version__, "2.0.0")
                and not connection.closed
            ):
                result = connection.execute(query)  # type: ignore[call-overload] # FIXME:Selectable overly broad
                connection.commit()
            else:
                with connection.begin():
                    result = connection.execute(query)  # type: ignore[call-overload] # FIXME:Selectable overly broad

        return result
