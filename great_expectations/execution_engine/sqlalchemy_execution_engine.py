import copy
import datetime
import logging
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple
from urllib.parse import urlparse

from sqlalchemy.engine import reflection
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import Select
from sqlalchemy.sql.elements import TextClause

from great_expectations.core import IDDict
from great_expectations.execution_environment.types import (
    SqlAlchemyDatasourceQueryBatchSpec,
    SqlAlchemyDatasourceTableBatchSpec,
)
from great_expectations.expectations.row_conditions import parse_condition_to_sqlalchemy
from great_expectations.util import import_library_module
from great_expectations.validator.validation_graph import MetricConfiguration

try:
    import sqlalchemy as sa
except ImportError:
    sa = None

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

    def __init__(self, engine, table_name=None, schema=None, query=None):
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

        if (
            query is not None
            and engine.dialect.name.lower() == "snowflake"
            and generated_table_name is not None
        ):
            raise ValueError(
                "No snowflake_transient_table specified. Snowflake with a query batch_kwarg will create "
                "a transient table, so you must provide a user-selected name."
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
            logger.info("Creating transient table %s" % table_name)
            if schema_name is not None:
                table_name = schema_name + "." + table_name
            stmt = "CREATE OR REPLACE TRANSIENT TABLE {table_name} AS {custom_sql}".format(
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
                engine (SqlAlchemyExecutionEngine): \
                    An Execution Engine used to set the SqlAlchemyExecutionEngine being configured, useful if an
                    Execution Engine has already been configured and should be reused. Will override Credentials
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
        super().__init__(name=name)
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

            if engine and engine.dialect.name.lower() in ["sqlite", "mssql"]:
                # sqlite/mssql temp tables only persist within a connection so override the engine
                self.engine = engine.connect()
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

    def _build_engine(self, credentials, **kwargs) -> sa.engine.Engine:
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

    def load_batch(
        self, batch_request=None, batch_spec=None, in_memory_dataset=None
    ) -> Batch:
        """
        With the help of the execution environment and data connector specified within the batch request,
        builds a batch spec and utilizes it to load a batch using the appropriate file reader and the given file path.

        Args:
           batch_spec (dict): A dictionary specifying the parameters used to build the batch
           in_memory_dataset (A Pandas DataFrame or None): Optional specification of an in memory Dataset used
                                                            to load a batch. A Data Asset name and partition ID
                                                            must still be passed via batch request.

        """
        # We need to build a batch_markers to be used in the dataframe
        if not batch_spec and not batch_request:
            raise ValueError("must provide a batch spec or batch request")

        if batch_spec and batch_request:
            raise ValueError("only provide either batch spec or batch request")

        if batch_spec and not batch_request:
            logger.info("loading a batch without a batch_request")
            batch_request = {}
        else:
            execution_environment_name = batch_request.get("execution_environment")
            if not self._data_context:
                raise ValueError("Cannot use a batch request without a data context")
            execution_environment = self._data_context.get_execution_environment(
                execution_environment_name
            )

            data_connector_name = batch_request.get("data_connector")
            assert data_connector_name, "Batch definition must specify a data_connector"

            data_connector = execution_environment.get_data_connector(
                data_connector_name
            )
            # noinspection PyProtectedMember
            batch_spec = data_connector._build_batch_spec(batch_request=batch_request)

        batch_markers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )

        if isinstance(batch_spec, SqlAlchemyDatasourceTableBatchSpec):
            batch_reference = SqlAlchemyBatchData(
                engine=self.engine,
                table_name=batch_spec.get("table"),
                schema=batch_spec.get("schema"),
            )
        elif isinstance(batch_spec, SqlAlchemyDatasourceQueryBatchSpec):
            batch_reference = SqlAlchemyBatchData(
                engine=self.engine,
                query=batch_spec.get("query"),
                schema=batch_spec.get("schema"),
            )
        else:
            raise BatchSpecError("Unrecognized BatchSpec")

        batch_id = batch_spec.to_id()
        if not self.batches.get(batch_id):
            batch = Batch(
                execution_engine=self,
                batch_spec=batch_spec,
                data=batch_reference,
                batch_request=batch_request,
                batch_markers=batch_markers,
                data_context=self._data_context,
            )
            self.batches[batch_id] = batch

        self._active_batch_data_id = batch_id
        return batch

    def get_compute_domain(
        self, domain_kwargs: dict = None
    ) -> Tuple[sa.sql.Selectable, dict, dict]:
        """Uses a given batch dictionary and domain kwargs to obtain a SqlAlchemy column object.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain
            batches (dict) - A dictionary specifying batch id and which batches to obtain

        Returns:
            SqlAlchemy column
        """
        batch_id = domain_kwargs.get("batch_id")
        if batch_id is None:
            # We allow no batch id specified if there is only one batch
            if self.active_batch_data:
                data_object = self.active_batch_data
            else:
                raise ValidationError(
                    "No batch is specified, but could not identify a loaded batch."
                )
        else:
            if batch_id in self.batches:
                data_object = self.batches[batch_id]
            else:
                raise ValidationError(f"Unable to find batch with batch_id {batch_id}")

        compute_domain_kwargs = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs = dict()
        if "table" in domain_kwargs:
            if domain_kwargs["table_name"] != data_object.table_name:
                raise ValueError("Unrecognized table name.")
            else:
                selectable = data_object.table
        elif "query" in domain_kwargs:
            raise ValueError(
                "query is not currently supported by SqlAlchemyExecutionEngine"
            )
        else:
            selectable = data_object.table

        if "row_condition" in domain_kwargs:
            condition_engine = domain_kwargs["condition_engine"]
            if condition_engine == "great_expectations__experimental__":
                parsed_condition = parse_condition_to_sqlalchemy(
                    domain_kwargs["row_condition"]
                )
                selectable = selectable.where(parsed_condition)

            else:
                raise GreatExpectationsError(
                    "SqlAlchemyExecutionEngine only supports the great_expectations condition_parser."
                )

        if "column" in compute_domain_kwargs:
            accessor_domain_kwargs["column"] = compute_domain_kwargs.pop("column")

        return selectable, compute_domain_kwargs, accessor_domain_kwargs

    def resolve_metric_bundle(
        self, metric_fn_bundle: Iterable[Tuple[MetricConfiguration, Callable, dict]],
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
            metric_provider,
            metric_provider_kwargs,
        ) in metric_fn_bundle:
            # We have different semantics for bundled metric providers, so ensure we actually are working only with those.
            assert (
                metric_provider.metric_fn_type == "aggregate_fn"
            ), "resolve_metric_bundle only supports aggregate metrics"
            # batch_id and table are the only determining factors for bundled metrics
            batch_id = metric_to_resolve.metric_domain_kwargs.get("batch_id")
            table = metric_to_resolve.metric_domain_kwargs.get("table")
            statement, domain_kwargs = metric_provider(**metric_provider_kwargs)
            if not isinstance(domain_kwargs, IDDict):
                domain_kwargs = IDDict(domain_kwargs)
            domain_id = domain_kwargs.to_id()
            if domain_id not in queries:
                queries[domain_id] = {
                    "select": [],
                    "ids": [],
                    "domain_kwargs": domain_kwargs,
                }
            queries[domain_id]["select"].append(
                statement.label(metric_to_resolve.metric_name)
            )
            queries[domain_id]["ids"].append(metric_to_resolve.id)
        for query in queries.values():
            selectable, compute_domain_kwargs, _ = self.get_compute_domain(
                query["domain_kwargs"]
            )
            assert (
                compute_domain_kwargs == query["domain_kwargs"]
            ), "Invalid compute domain returned from a bundled metric. Verify that its target compute domain is a valid compute domain."
            assert len(query["select"]) == len(query["ids"])
            res = self.engine.execute(
                sa.select(query["select"]).select_from(selectable)
            ).fetchall()
            assert (
                len(res) == 1
            ), "all bundle-computed metrics must be single-value statistics"
            assert len(query["ids"]) == len(
                res[0]
            ), "unexpected number of metrics returned"
            for idx, id in enumerate(query["ids"]):
                resolved_metrics[id] = res[0][idx]

        return resolved_metrics
