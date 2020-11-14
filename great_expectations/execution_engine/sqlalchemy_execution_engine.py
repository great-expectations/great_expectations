import copy
import datetime
import json
import logging
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple, Union, Optional
from urllib.parse import urlparse

from great_expectations.core import IDDict
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.expectations.row_conditions import parse_condition_to_sqlalchemy
from great_expectations.util import import_library_module
from great_expectations.validator.validation_graph import MetricConfiguration

try:
    import sqlalchemy as sa
    from sqlalchemy.engine import reflection
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.sql import Select
    from sqlalchemy.sql.elements import TextClause, quoted_name
except ImportError:
    sa = None
    reflection = None
    DefaultDialect = None
    Select = None
    TextClause = None
    quoted_name = None

from great_expectations.core.batch import Batch, BatchMarkers
from great_expectations.exceptions import (
    BatchSpecError,
    DatasourceKeyPairAuthBadPassphraseError,
    GreatExpectationsError,
    InvalidConfigError,
    ValidationError,
)
from great_expectations.execution_engine import ExecutionEngine

logger = logging.getLogger(__name__)

try:
    import psycopg2
    import sqlalchemy.dialects.postgresql.psycopg2 as sqlalchemy_psycopg2
except (ImportError, KeyError):
    sqlalchemy_psycopg2 = None

try:
    import sqlalchemy_redshift.dialect
except ImportError:
    sqlalchemy_redshift = None

try:
    import snowflake.sqlalchemy.snowdialect

    # Sometimes "snowflake-sqlalchemy" fails to self-register in certain environments, so we do it explicitly.
    # (see https://stackoverflow.com/questions/53284762/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectssnowflake)
    sa.dialects.registry.register("snowflake", "snowflake.sqlalchemy", "dialect")
except (ImportError, KeyError):
    snowflake = None

try:
    import pybigquery.sqlalchemy_bigquery

    # Sometimes "pybigquery.sqlalchemy_bigquery" fails to self-register in certain environments, so we do it explicitly.
    # (see https://stackoverflow.com/questions/53284762/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectssnowflake)
    sa.dialects.registry.register(
        "bigquery", "pybigquery.sqlalchemy_bigquery", "BigQueryDialect"
    )
    try:
        getattr(pybigquery.sqlalchemy_bigquery, "INTEGER")
        bigquery_types_tuple = None
    except AttributeError:
        # In older versions of the pybigquery driver, types were not exported, so we use a hack
        logger.warning(
            "Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later."
        )
        from collections import namedtuple

        BigQueryTypes = namedtuple(
            "BigQueryTypes", sorted(pybigquery.sqlalchemy_bigquery._type_map)
        )
        bigquery_types_tuple = BigQueryTypes(**pybigquery.sqlalchemy_bigquery._type_map)
except ImportError:
    bigquery_types_tuple = None
    pybigquery = None


def _get_dialect_type_module(dialect):
    """Given a dialect, returns the dialect type, which is defines the engine/system that is used to communicates
    with the database/database implementation. Currently checks for RedShift/BigQuery dialects"""
    if dialect is None:
        logger.warning(
            "No sqlalchemy dialect found; relying in top-level sqlalchemy types."
        )
        return sa
    try:
        # Redshift does not (yet) export types to top level; only recognize base SA types
        if isinstance(dialect, sqlalchemy_redshift.dialect.RedshiftDialect):
            return dialect.sa
    except (TypeError, AttributeError):
        pass

    # Bigquery works with newer versions, but use a patch if we had to define bigquery_types_tuple
    try:
        if (
            isinstance(dialect, pybigquery.sqlalchemy_bigquery.BigQueryDialect,)
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    return dialect


class SqlAlchemyBatchData:
    """A class which represents a SQL alchemy batch, with properties including the construction of the batch itself
    and several getters used to access various properties."""

    def __init__(
        self, engine, table_name=None, schema=None, query=None, use_quoted_name=False
    ):
        """A Constructor used to initialize and SqlAlchemy Batch, create an id for it, and verify that all necessary
        parameters have been provided. If a Query is given, also builds a temporary table for this query

            Args:
                engine (SqlAlchemyExecutionEngineExecutionEngine): \
                    A SqlAlchemy Execution Engine that will act upon the data
                table_name (string or None): \
                    The name of the table that will be accessed. Either this parameter or the query parameter must be
                    specified. Default is 'None'.
                schema (string or None): \
                    The name of the schema in which the databases lie
                query (string or None): \
                    A query string representing a domain, which will be used to create a temporary table
            """
        self._engine = engine
        self._table_name = table_name
        self._schema = schema
        self._query = query
        self._use_quoted_name = use_quoted_name

        if table_name is None and query is None:
            raise ValueError("Table_name or query must be specified")

        if query and not table_name:
            # NOTE: Eugene 2020-01-31: @James, this is a not a proper fix, but without it the "public" schema
            # was used for a temp table and raising an error
            schema = None
            table_name = f"ge_tmp_{str(uuid.uuid4())[:8]}"
            # mssql expects all temporary table names to have a prefix '#'
            if engine.dialect.name.lower() == "mssql":
                table_name = f"#{table_name}"
            generated_table_name = table_name
        else:
            generated_table_name = None

        if table_name is None:
            raise ValueError("No table_name provided.")

        if use_quoted_name:
            table_name = quoted_name(table_name)

        if engine.dialect.name.lower() == "bigquery":
            # In BigQuery the table name is already qualified with its schema name
            self._table = sa.Table(table_name, sa.MetaData(), schema=None)

        else:
            self._table = sa.Table(table_name, sa.MetaData(), schema=schema)

        if schema is not None and query is not None:
            # temporary table will be written to temp schema, so don't allow
            # a user-defined schema
            # NOTE: 20200306 - JPC - Previously, this would disallow both custom_sql (a query) and a schema, but
            # that is overly restrictive -- snowflake could have had a schema specified, for example, in which to create
            # a temporary table.
            # raise ValueError("Cannot specify both schema and custom_sql.")
            pass

        if query is not None and engine.dialect.name.lower() == "bigquery":
            if generated_table_name is not None and engine.dialect.dataset_id is None:
                raise ValueError(
                    "No BigQuery dataset specified. Use bigquery_temp_table batch_kwarg or a specify a "
                    "default dataset in engine url"
                )

        if query:
            self.create_temporary_table(table_name, query, schema_name=schema)

            if (
                generated_table_name is not None
                and engine.dialect.name.lower() == "bigquery"
            ):
                logger.warning(
                    "Created permanent table {table_name}".format(table_name=table_name)
                )

        try:
            insp = reflection.Inspector.from_engine(engine)
            self.columns = insp.get_columns(table_name, schema=schema)
        except KeyError:
            # we will get a KeyError for temporary tables, since
            # reflection will not find the temporary schema
            self.columns = self.column_reflection_fallback()

        # Use fallback because for mssql reflection doesn't throw an error but returns an empty list
        if len(self.columns) == 0:
            self.columns = self.column_reflection_fallback()

    @property
    def sql_engine_dialect(self) -> DefaultDialect:
        """Returns the Batches' current engine dialect"""
        return self._engine.dialect

    @property
    def table(self):
        """Returns a table of the data inside the sqlalchemy_execution_engine"""
        return self._table

    @property
    def use_quoted_name(self):
        """Returns a table of the data inside the sqlalchemy_execution_engine"""
        return self._use_quoted_name

    @property
    def schema(self):
        return self._schema

    @property
    def selectable(self):
        if self._table is not None:
            return self._table
        else:
            return sa.text(self._query)

    def create_temporary_table(self, table_name, custom_sql, schema_name=None):
        """
        Create Temporary table based on sql query. This will be used as a basis for executing expectations.
        :param custom_sql:
        """

        ###
        # NOTE: 20200310 - The update to support snowflake transient table creation revealed several
        # import cases that are not fully handled.
        # The snowflake-related change updated behavior to allow both custom_sql and schema to be specified. But
        # the underlying incomplete handling of schema remains.
        #
        # Several cases we need to consider:
        #
        # 1. Distributed backends (e.g. Snowflake and BigQuery) often use a `<database>.<schema>.<table>`
        # syntax, but currently we are biased towards only allowing schema.table
        #
        # 2. In the wild, we see people using several ways to declare the schema they want to use:
        # a. In the connection string, the original RFC only specifies database, but schema is supported by some
        # backends (Snowflake) as a query parameter.
        # b. As a default for a user (the equivalent of USE SCHEMA being provided at the beginning of a session)
        # c. As part of individual queries.
        #
        # 3. We currently don't make it possible to select from a table in one query, but create a temporary
        # table in
        # another schema, except for with BigQuery and (now) snowflake, where you can specify the table name (and
        # potentially triple of database, schema, table) in the batch_kwargs.
        #
        # The SqlAlchemyDataset interface essentially predates the batch_kwargs concept and so part of what's going
        # on, I think, is a mismatch between those. I think we should rename custom_sql -> "temp_table_query" or
        # similar, for example.
        ###

        if self.sql_engine_dialect.name.lower() == "bigquery":
            stmt = "CREATE OR REPLACE TABLE `{table_name}` AS {custom_sql}".format(
                table_name=table_name, custom_sql=custom_sql
            )
        elif self.sql_engine_dialect.name.lower() == "snowflake":
            if schema_name is not None:
                table_name = schema_name + "." + table_name
            stmt = "CREATE OR REPLACE TEMPORARY TABLE {table_name} AS {custom_sql}".format(
                table_name=table_name, custom_sql=custom_sql
            )
        elif self.sql_engine_dialect.name == "mysql":
            # Note: We can keep the "MySQL" clause separate for clarity, even though it is the same as the
            # generic case.
            stmt = "CREATE TEMPORARY TABLE {table_name} AS {custom_sql}".format(
                table_name=table_name, custom_sql=custom_sql
            )
        elif self.sql_engine_dialect.name == "mssql":
            # Insert "into #{table_name}" in the custom sql query right before the "from" clause
            # Split is case sensitive so detect case.
            # Note: transforming custom_sql to uppercase/lowercase has uninteded consequences (i.e.,
            # changing column names), so this is not an option!
            if "from" in custom_sql:
                strsep = "from"
            else:
                strsep = "FROM"
            custom_sqlmod = custom_sql.split(strsep, maxsplit=1)
            stmt = (
                custom_sqlmod[0] + "into {table_name} from" + custom_sqlmod[1]
            ).format(table_name=table_name)
        else:
            stmt = 'CREATE TEMPORARY TABLE "{table_name}" AS {custom_sql}'.format(
                table_name=table_name, custom_sql=custom_sql
            )
        self._engine.execute(stmt)

    def column_reflection_fallback(self):
        """If we can't reflect the table, use a query to at least get column names."""
        col_info_dict_list: List[Dict]
        if self.sql_engine_dialect.name.lower() == "mssql":
            type_module = _get_dialect_type_module(self.sql_engine_dialect)
            # Get column names and types from the database
            # StackOverflow to the rescue: https://stackoverflow.com/a/38634368
            col_info_query: TextClause = sa.text(
                f"""
SELECT
    cols.NAME, ty.NAME
FROM
    tempdb.sys.columns AS cols
JOIN
    sys.types AS ty
ON
    cols.user_type_id = ty.user_type_id
WHERE
    object_id = OBJECT_ID('tempdb..{self._table}')
                """
            )
            col_info_tuples_list = self._engine.execute(col_info_query).fetchall()
            col_info_dict_list = [
                {"name": col_name, "type": getattr(type_module, col_type.upper())()}
                for col_name, col_type in col_info_tuples_list
            ]
        else:
            query: Select = sa.select([sa.text("*")]).select_from(self._table).limit(1)
            col_names: list = self._engine.execute(query).keys()
            col_info_dict_list = [{"name": col_name} for col_name in col_names]
        return col_info_dict_list


class SqlAlchemyExecutionEngine(ExecutionEngine):
    def __init__(
        self,
        name=None,
        credentials=None,
        data_context=None,
        engine=None,
        connection_string=None,
        url=None,
        batch_data_dict=None,
        **kwargs,
    ):
        """Builds a SqlAlchemyExecutionEngine, using a provided connection string/url/engine/credentials to access the
        desired database. Also initializes the dialect to be used and configures usage statistics.

            Args:
                name (str): \
                    The name of the SqlAlchemyExecutionEngine
                credentials: \
                    If the Execution Engine is not provided, the credentials can be used to build the Execution
                    Engine. If the Engine is provided, it will be used instead
                data_context (DataContext): \
                    An object representing a Great Expectations project that can be used to access Expectation
                    Suites and the Project Data itself
                engine (Engine): \
                    A SqlAlchemy Engine used to set the SqlAlchemyExecutionEngine being configured, useful if an
                    Engine has already been configured and should be reused. Will override Credentials
                    if provided.
                connection_string (string): \
                    If neither the engines nor the credentials have been provided, a connection string can be used
                    to access the data. This will be overridden by both the engine and credentials if those are
                    provided.
                url (string): \
                    If neither the engines, the credentials, nor the connection_string have been provided,
                    a url can be used to access the data. This will be overridden by all other configuration
                    options if any are provided.
        """
        super().__init__(name=name, batch_data_dict=batch_data_dict, **kwargs)
        self._name = name
        if engine is not None:
            if credentials is not None:
                logger.warning(
                    "Both credentials and engine were provided during initialization of SqlAlchemyExecutionEngine. "
                    "Ignoring credentials."
                )
            self.engine = engine
        elif credentials is not None:
            self.engine = self._build_engine(credentials=credentials, **kwargs)
        elif connection_string is not None:
            self.engine = sa.create_engine(connection_string, **kwargs)
        elif url is not None:
            self.drivername = urlparse(url).scheme
            self.engine = sa.create_engine(url, **kwargs)
        else:
            raise InvalidConfigError(
                "Credentials or an engine are required for a SqlAlchemyExecutionEngine."
            )
        self.engine.connect()

        # Get the dialect **for purposes of identifying types**
        if self.engine.dialect.name.lower() in [
            "postgresql",
            "mysql",
            "sqlite",
            "oracle",
            "mssql",
            "oracle",
        ]:
            # These are the officially included and supported dialects by sqlalchemy
            self.dialect = import_library_module(
                module_name="sqlalchemy.dialects." + self.engine.dialect.name
            )

        elif self.engine.dialect.name.lower() == "snowflake":
            self.dialect = import_library_module(
                module_name="snowflake.sqlalchemy.snowdialect"
            )
        elif self.engine.dialect.name.lower() == "redshift":
            self.dialect = import_library_module(
                module_name="sqlalchemy_redshift.dialect"
            )
        elif self.engine.dialect.name.lower() == "bigquery":
            self.dialect = import_library_module(
                module_name="pybigquery.sqlalchemy_bigquery"
            )
        else:
            self.dialect = None

        if self.engine and self.engine.dialect.name.lower() in [
            "sqlite",
            "mssql",
            "snowflake",
        ]:
            # sqlite/mssql temp tables only persist within a connection so override the engine
            self.engine = engine.connect()

        # Send a connect event to provide dialect type
        if data_context is not None and getattr(
            data_context, "_usage_statistics_handler", None
        ):
            handler = data_context._usage_statistics_handler
            handler.send_usage_message(
                event="execution_engine.sqlalchemy.connect",
                event_payload={
                    "anonymized_name": handler._execution_engine_anonymizer.anonymize(
                        self.name
                    ),
                    "sqlalchemy_dialect": self.engine.name,
                },
                success=True,
            )

    def _build_engine(self, credentials, **kwargs) -> "sa.engine.Engine":
        """
        Using a set of given credentials, constructs an Execution Engine , connecting to a database using a URL or a
        private key path.
        """
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
            options = sa.engine.url.URL(drivername, **credentials)

        self.drivername = drivername
        engine = sa.create_engine(options, **create_engine_kwargs)
        return engine

    def _get_sqlalchemy_key_pair_auth_url(
        self, drivername: str, credentials: dict
    ) -> Tuple[str, dict]:
        """
        Utilizing a private key path and a passphrase in a given credentials dictionary, attempts to encode the provided
        values into a private key. If passphrase is incorrect, this will fail and an exception is raised.

        Args:
            drivername(str) - The name of the driver class
            credentials(dict) - A dictionary of database credentials used to access the database

        Returns:
            a tuple consisting of a url with the serialized key-pair authentication, and a dictionary of engine kwargs.
        """
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        private_key_path = credentials.pop("private_key_path")
        private_key_passphrase = credentials.pop("private_key_passphrase")

        with Path(private_key_path).expanduser().resolve().open(mode="rb") as key:
            try:
                p_key = serialization.load_pem_private_key(
                    key.read(),
                    password=private_key_passphrase.encode()
                    if private_key_passphrase
                    else None,
                    backend=default_backend(),
                )
            except ValueError as e:
                if "incorrect password" in str(e).lower():
                    raise DatasourceKeyPairAuthBadPassphraseError(
                        datasource_name="SqlAlchemyDatasource",
                        message="Decryption of key failed, was the passphrase incorrect?",
                    ) from e
                else:
                    raise e
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        credentials_driver_name = credentials.pop("drivername", None)
        create_engine_kwargs = {"connect_args": {"private_key": pkb}}
        return (
            sa.engine.url.URL(drivername or credentials_driver_name, **credentials),
            create_engine_kwargs,
        )

    def get_compute_domain(
        self,
        domain_kwargs: Dict,
        domain_type: Union[str, "MetricDomainTypes"],
        accessor_keys: Optional[Iterable[str]] = None
    ) -> Tuple["sa.sql.Selectable", dict, dict]:
        """Uses a given batch dictionary and domain kwargs to obtain a SqlAlchemy column object.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain
            batches (dict) - A dictionary specifying batch id and which batches to obtain
            domain_type (str or "MetricDomainTypes") - an Enum value indicating which metric domain the user would
            like to be using, or a corresponding string value representing it. String types include "identity", "column",
            "column_pair", "table" and "other". Enum types include capitalized versions of these from the class
            MetricDomainTypes.
            accessor_keys (str iterable) - keys that are part of the compute domain but should be ignored when describing
            the domain and simply transferred with their associated values into accessor_domain_kwargs.

        Returns:
            SqlAlchemy column
        """
        # Extracting value from enum if it is given for future computation
        domain_type = MetricDomainTypes(domain_type)
        batch_id = domain_kwargs.get("batch_id")
        if batch_id is None:
            # We allow no batch id specified if there is only one batch
            if self.active_batch_data:
                data_object = self.active_batch_data
            else:
                raise GreatExpectationsError(
                    "No batch is specified, but could not identify a loaded batch."
                )
        else:
            if batch_id in self.loaded_batch_data:
                data_object = self.loaded_batch_data[batch_id]
            else:
                raise GreatExpectationsError(
                    f"Unable to find batch with batch_id {batch_id}"
                )

        compute_domain_kwargs = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs = dict()
        if "table" in domain_kwargs and domain_kwargs["table"] is not None:
            if domain_kwargs["table"] != data_object.table:
                raise ValueError("Unrecognized table name.")
            else:
                selectable = data_object.table
        elif "query" in domain_kwargs:
            raise ValueError(
                "query is not currently supported by SqlAlchemyExecutionEngine"
            )
        else:
            selectable = data_object.table

        if (
            "row_condition" in domain_kwargs
            and domain_kwargs["row_condition"] is not None
        ):
            condition_parser = domain_kwargs["condition_parser"]
            if condition_parser == "great_expectations__experimental__":
                parsed_condition = parse_condition_to_sqlalchemy(
                    domain_kwargs["row_condition"]
                )
                selectable = sa.select(
                    "*", from_obj=selectable, whereclause=parsed_condition
                )

            else:
                raise GreatExpectationsError(
                    "SqlAlchemyExecutionEngine only supports the great_expectations condition_parser."
                )

        if domain_type == MetricDomainTypes.TABLE:
            for key in accessor_keys:
                accessor_domain_kwargs[key] = compute_domain_kwargs.pop(key)
            for key in compute_domain_kwargs.keys():
                # Warning user if kwarg not "normal"
                if key not in ["batch_id", "table", "row_condition", "condition_parser"]:
                    logger.warning(f"Unexpected key {key} found in domain_kwargs for domain type {domain_type.value}")
            return selectable, compute_domain_kwargs, accessor_domain_kwargs

        # If user has stated they want a column, checking if one is provided, and
        elif domain_type == MetricDomainTypes.COLUMN:
            if "column" in compute_domain_kwargs:
                # Checking if case- sensitive and using appropriate name
                if self.active_batch_data.use_quoted_name:
                    accessor_domain_kwargs["column"] = quoted_name(
                        compute_domain_kwargs.pop("column")
                    )
                else:
                    accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")
            else:
                # If column not given
                raise GreatExpectationsError("Column not provided in compute_domain_kwargs")

        # Else, if column pair values requested
        elif domain_type == MetricDomainTypes.COLUMN_PAIR:
            # Ensuring column_A and column_B parameters provided
            if "column_A" in compute_domain_kwargs and "column_B" in compute_domain_kwargs:
                if self.active_batch_data.use_quoted_name:
                    # If case matters...
                    accessor_domain_kwargs["column_A"] = quoted_name(compute_domain_kwargs.pop("column_A"))
                    accessor_domain_kwargs["column_B"] = quoted_name(compute_domain_kwargs.pop("column_B"))
                else:
                    accessor_domain_kwargs["column_A"] = compute_domain_kwargs.pop("column_A")
                    accessor_domain_kwargs["column_B"] = compute_domain_kwargs.pop("column_B")
            else:
                raise GreatExpectationsError("column_A or column_B not found within compute_domain_kwargs")

        # Checking if table or identity or other provided, column is not specified. If it is, warning the user
        elif domain_type == MetricDomainTypes.MULTICOLUMN:
            if "columns" in compute_domain_kwargs:
                # If columns exist
                for column in compute_domain_kwargs["columns"]:
                    try:
                        # Checking if capitalization matters
                        if self.active_batch_data.use_quoted_name:
                            accessor_domain_kwargs[column] = quoted_name(compute_domain_kwargs.pop(column))
                        else:
                            accessor_domain_kwargs[column] = compute_domain_kwargs.pop(column)

                    # Raising an error if column doesn't exist
                    except KeyError as k:
                        raise KeyError(f"Column {column} not found within compute_domain_kwargs")

        # Filtering if identity
        elif domain_type == MetricDomainTypes.IDENTITY:

            # If we would like our data to become a single column
            if "column" in compute_domain_kwargs:
                data = pd.DataFrame(data[compute_domain_kwargs["column"]])

            # If we would like our data to now become a column pair
            elif ("column_A" in compute_domain_kwargs) and ("column_B" in compute_domain_kwargs):
                data = data.get(compute_domain_kwargs["column_A"], compute_domain_kwargs["column_B"])
            else:

                # If we would like our data to become a multicolumn
                if "columns" in compute_domain_kwargs:
                    data = data.get(compute_domain_kwargs["columns"])


        if self.active_batch_data.use_quoted_name:
            accessor_domain_kwargs["column"] = quoted_name(
                compute_domain_kwargs.pop("column")
            )
        else:
            accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")

        return selectable, compute_domain_kwargs, accessor_domain_kwargs

    def resolve_metric_bundle(
        self, metric_fn_bundle: Iterable[Tuple[MetricConfiguration, Any, dict, dict]],
    ) -> dict:
        """For every metrics in a set of Metrics to resolve, obtains necessary metric keyword arguments and builds a
        bundles the metrics into one large query dictionary so that they are all executed simultaneously. Will fail if
        bundling the metrics together is not possible.

            Args:
                metric_fn_bundle (Iterable[Tuple[MetricConfiguration, Callable, dict]): \
                    A Dictionary containing a MetricProvider's MetricConfiguration (its unique identifier), its metric provider function
                    (the function that actually executes the metric), and the arguments to pass to the metric provider function.
                metrics (Dict[Tuple, Any]): \
                    A dictionary of metrics defined in the registry and corresponding arguments

            Returns:
                A dictionary of metric names and their corresponding now-queried values.
        """
        resolved_metrics = dict()

        # We need a different query for each domain (where clause).
        queries: Dict[Tuple, dict] = dict()
        for (
            metric_to_resolve,
            engine_fn,
            compute_domain_kwargs,
            accessor_domain_kwargs,
            metric_provider_kwargs,
        ) in metric_fn_bundle:
            if not isinstance(compute_domain_kwargs, IDDict):
                compute_domain_kwargs = IDDict(compute_domain_kwargs)
            domain_id = compute_domain_kwargs.to_id()
            if domain_id not in queries:
                queries[domain_id] = {
                    "select": [],
                    "ids": [],
                    "domain_kwargs": compute_domain_kwargs,
                }
            queries[domain_id]["select"].append(
                engine_fn.label(metric_to_resolve.metric_name)
            )
            queries[domain_id]["ids"].append(metric_to_resolve.id)
        for query in queries.values():
            selectable, compute_domain_kwargs, _ = self.get_compute_domain(
                query["domain_kwargs"], domain_type="identity"
            )
            assert len(query["select"]) == len(query["ids"])
            res = self.engine.execute(
                sa.select(query["select"]).select_from(selectable)
            ).fetchall()
            logger.debug(
                f"SqlAlchemyExecutionEngine computed {len(res[0])} metrics on domain_id {IDDict(compute_domain_kwargs).to_id()}"
            )
            assert (
                len(res) == 1
            ), "all bundle-computed metrics must be single-value statistics"
            assert len(query["ids"]) == len(
                res[0]
            ), "unexpected number of metrics returned"
            for idx, id in enumerate(query["ids"]):
                resolved_metrics[id] = res[0][idx]

        return resolved_metrics

    ### Splitter methods for partitioning tables ###

    def _split_on_whole_table(
        self,
        table_name: str,
        # column_name: str,
        partition_definition: dict,
    ):
        """'Split' by returning the whole table"""

        # return sa.column(column_name) == partition_definition[column_name]
        return 1 == 1

    def _split_on_column_value(
        self, table_name: str, column_name: str, partition_definition: dict,
    ):
        """Split using the values in the named column"""

        return sa.column(column_name) == partition_definition[column_name]

    def _split_on_converted_datetime(
        self,
        table_name: str,
        column_name: str,
        partition_definition: dict,
        date_format_string: str = "%Y-%m-%d",
    ):
        """Convert the values in the named column to the given date_format, and split on that"""

        return (
            sa.func.strftime(date_format_string, sa.column(column_name),)
            == partition_definition[column_name]
        )

    def _split_on_divided_integer(
        self,
        table_name: str,
        column_name: str,
        divisor: int,
        partition_definition: dict,
    ):
        """Divide the values in the named column by `divisor`, and split on that"""

        return (
            sa.cast(sa.column(column_name) / divisor, sa.Integer)
            == partition_definition[column_name]
        )

    def _split_on_mod_integer(
        self, table_name: str, column_name: str, mod: int, partition_definition: dict,
    ):
        """Divide the values in the named column by `divisor`, and split on that"""

        return sa.column(column_name) % mod == partition_definition[column_name]

    def _split_on_multi_column_values(
        self, table_name: str, column_names: List[str], partition_definition: dict,
    ):
        """Split on the joint values in the named columns"""

        return sa.and_(
            *[
                sa.column(column_name) == column_value
                for column_name, column_value in partition_definition.items()
            ]
        )

    def _split_on_hashed_column(
        self,
        table_name: str,
        column_name: str,
        hash_digits: int,
        partition_definition: dict,
    ):
        """Split on the hashed value of the named column"""

        return (
            sa.func.right(sa.func.md5(sa.column(column_name)), hash_digits)
            == partition_definition[column_name]
        )

    ### Sampling methods ###

    # _sample_using_limit
    # _sample_using_random
    # _sample_using_mod
    # _sample_using_a_list
    # _sample_using_md5

    def _sample_using_random(
        self, p: float = 0.1,
    ):
        """Take a random sample of rows, retaining proportion p

        Note: the Random function behaves differently on different dialects of SQL
        """
        return sa.func.random() < p

    def _sample_using_mod(
        self, column_name, mod: int, value: int,
    ):
        """Take the mod of named column, and only keep rows that match the given value"""
        return sa.column(column_name) % mod == value

    def _sample_using_a_list(
        self, column_name: str, value_list: list,
    ):
        """Match the values in the named column against value_list, and only keep the matches"""
        return sa.column(column_name).in_(value_list)

    def _sample_using_md5(
        self, column_name: str, hash_digits: int = 1, hash_value: str = "f",
    ):
        """Hash the values in the named column, and split on that"""
        return (
            sa.func.right(
                sa.func.md5(sa.cast(sa.column(column_name), sa.Text)), hash_digits
            )
            == hash_value
        )

    def _build_selector_from_batch_spec(self, batch_spec):
        table_name = batch_spec["table_name"]

        splitter_fn = getattr(self, batch_spec["splitter_method"])
        if "sampling_method" in batch_spec:
            if batch_spec["sampling_method"] == "_sample_using_limit":

                return (
                    sa.select("*")
                    .select_from(sa.text(table_name))
                    .where(
                        splitter_fn(
                            table_name=table_name,
                            partition_definition=batch_spec["partition_definition"],
                            **batch_spec["splitter_kwargs"],
                        )
                    )
                    .limit(batch_spec["sampling_kwargs"]["n"])
                )

            else:

                sampler_fn = getattr(self, batch_spec["sampling_method"])
                return (
                    sa.select("*")
                    .select_from(sa.text(table_name))
                    .where(
                        sa.and_(
                            splitter_fn(
                                table_name=table_name,
                                partition_definition=batch_spec["partition_definition"],
                                **batch_spec["splitter_kwargs"],
                            ),
                            sampler_fn(**batch_spec["sampling_kwargs"]),
                        )
                    )
                )

        else:

            return (
                sa.select("*")
                .select_from(sa.text(table_name))
                .where(
                    splitter_fn(
                        table_name=table_name,
                        partition_definition=batch_spec["partition_definition"],
                        **batch_spec["splitter_kwargs"],
                    )
                )
            )

    def get_batch_data_and_markers(
        self, batch_spec
    ) -> Tuple[SqlAlchemyBatchData, BatchMarkers]:

        selector = self._build_selector_from_batch_spec(batch_spec)
        batch_data = self.engine.execute(selector)
        # TODO: Abe 20201030: This method should return a SqlAlchemyBatchData as its first object, but that probably requires deeper changes.
        # SqlAlchemyBatchData(
        #     engine=self.engine,
        #     table_name=batch_spec.get("table"),
        #     schema=batch_spec.get("schema"),
        # )

        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        return batch_data, batch_markers
