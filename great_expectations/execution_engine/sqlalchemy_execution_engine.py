import copy
import datetime
import logging
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlparse

from great_expectations.core import IDDict
from great_expectations.core.batch import BatchMarkers, BatchSpec
from great_expectations.core.batch_spec import (
    RuntimeDataBatchSpec,
    RuntimeQueryBatchSpec,
    SqlAlchemyDatasourceBatchSpec,
)
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.exceptions import (
    DatasourceKeyPairAuthBadPassphraseError,
    ExecutionEngineError,
    GreatExpectationsError,
    InvalidBatchSpecError,
    InvalidConfigError,
)
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.expectations.row_conditions import parse_condition_to_sqlalchemy
from great_expectations.util import filter_properties_dict, import_library_module
from great_expectations.validator.validation_graph import MetricConfiguration

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sa
except ImportError:
    sa = None

try:
    from sqlalchemy.engine import reflection
    from sqlalchemy.engine.default import DefaultDialect
    from sqlalchemy.engine.url import URL
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.sql import Select
    from sqlalchemy.sql.elements import TextClause, quoted_name
except ImportError:
    reflection = None
    DefaultDialect = None
    Select = None
    TextClause = None
    quoted_name = None
    OperationalError = None


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
            isinstance(
                dialect,
                pybigquery.sqlalchemy_bigquery.BigQueryDialect,
            )
            and bigquery_types_tuple is not None
        ):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass

    return dialect


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
        create_temp_table=True,
        **kwargs,  # These will be passed as optional parameters to the SQLAlchemy engine, **not** the ExecutionEngine
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
        super().__init__(name=name, batch_data_dict=batch_data_dict)
        self._name = name

        self._credentials = credentials
        self._connection_string = connection_string
        self._url = url
        self._create_temp_table = create_temp_table

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
            self.dialect_module = import_library_module(
                module_name="sqlalchemy.dialects." + self.engine.dialect.name
            )

        elif self.engine.dialect.name.lower() == "snowflake":
            self.dialect_module = import_library_module(
                module_name="snowflake.sqlalchemy.snowdialect"
            )
        elif self.engine.dialect.name.lower() == "redshift":
            self.dialect_module = import_library_module(
                module_name="sqlalchemy_redshift.dialect"
            )
        elif self.engine.dialect.name.lower() == "bigquery":
            self.dialect_module = import_library_module(
                module_name="pybigquery.sqlalchemy_bigquery"
            )
        else:
            self.dialect_module = None

        if self.engine and self.engine.dialect.name.lower() in [
            "sqlite",
            "mssql",
            "snowflake",
            "mysql",
        ]:
            # sqlite/mssql temp tables only persist within a connection so override the engine
            self.engine = self.engine.connect()

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

        # Gather the call arguments of the present function (and add the "class_name"), filter out the Falsy values,
        # and set the instance "_config" variable equal to the resulting dictionary.
        self._config = {
            "name": name,
            "credentials": credentials,
            "data_context": data_context,
            "engine": engine,
            "connection_string": connection_string,
            "url": url,
            "batch_data_dict": batch_data_dict,
            "module_name": self.__class__.__module__,
            "class_name": self.__class__.__name__,
        }
        self._config.update(kwargs)
        filter_properties_dict(properties=self._config, inplace=True)

    @property
    def credentials(self):
        return self._credentials

    @property
    def connection_string(self):
        return self._connection_string

    @property
    def url(self):
        return self._url

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
    ) -> Tuple["sa.engine.url.URL", Dict]:
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
        domain_type: Union[str, MetricDomainTypes],
        accessor_keys: Optional[Iterable[str]] = None,
    ) -> Tuple[Select, dict, dict]:
        """Uses a given batch dictionary and domain kwargs to obtain a SqlAlchemy column object.

        Args:
            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain
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
            if batch_id in self.loaded_batch_data_dict:
                data_object = self.loaded_batch_data_dict[batch_id]
            else:
                raise GreatExpectationsError(
                    f"Unable to find batch with batch_id {batch_id}"
                )

        compute_domain_kwargs = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs = dict()
        if "table" in domain_kwargs and domain_kwargs["table"] is not None:
            # TODO: Add logic to handle record_set_name once implemented
            # (i.e. multiple record sets (tables) in one batch
            if domain_kwargs["table"] != data_object.selectable.name:
                selectable = sa.Table(
                    domain_kwargs["table"],
                    sa.MetaData(),
                    schema_name=data_object._schema_name,
                )
            else:
                selectable = data_object.selectable
        elif "query" in domain_kwargs:
            raise ValueError(
                "query is not currently supported by SqlAlchemyExecutionEngine"
            )
        else:
            selectable = data_object.selectable

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

        # Warning user if accessor keys are in any domain that is not of type table, will be ignored
        if (
            domain_type != MetricDomainTypes.TABLE
            and accessor_keys is not None
            and len(list(accessor_keys)) > 0
        ):
            logger.warning(
                "Accessor keys ignored since Metric Domain Type is not 'table'"
            )

        if domain_type == MetricDomainTypes.TABLE:
            if accessor_keys is not None and len(list(accessor_keys)) > 0:
                for key in accessor_keys:
                    accessor_domain_kwargs[key] = compute_domain_kwargs.pop(key)
            if len(domain_kwargs.keys()) > 0:
                # Warn user if kwarg not "normal".
                unexpected_keys: set = set(compute_domain_kwargs.keys()).difference(
                    {
                        "batch_id",
                        "table",
                        "row_condition",
                        "condition_parser",
                    }
                )
                if len(unexpected_keys) > 0:
                    unexpected_keys_str: str = ", ".join(
                        map(lambda element: f'"{element}"', unexpected_keys)
                    )
                    logger.warning(
                        f'Unexpected key(s) {unexpected_keys_str} found in domain_kwargs for domain type "{domain_type.value}".'
                    )
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
                    accessor_domain_kwargs["column"] = compute_domain_kwargs.pop(
                        "column"
                    )
            else:
                # If column not given
                raise GreatExpectationsError(
                    "Column not provided in compute_domain_kwargs"
                )

        # Else, if column pair values requested
        elif domain_type == MetricDomainTypes.COLUMN_PAIR:
            # Ensuring column_A and column_B parameters provided
            if (
                "column_A" in compute_domain_kwargs
                and "column_B" in compute_domain_kwargs
            ):
                if self.active_batch_data.use_quoted_name:
                    # If case matters...
                    accessor_domain_kwargs["column_A"] = quoted_name(
                        compute_domain_kwargs.pop("column_A")
                    )
                    accessor_domain_kwargs["column_B"] = quoted_name(
                        compute_domain_kwargs.pop("column_B")
                    )
                else:
                    accessor_domain_kwargs["column_A"] = compute_domain_kwargs.pop(
                        "column_A"
                    )
                    accessor_domain_kwargs["column_B"] = compute_domain_kwargs.pop(
                        "column_B"
                    )
            else:
                raise GreatExpectationsError(
                    "column_A or column_B not found within compute_domain_kwargs"
                )

        # Checking if table or identity or other provided, column is not specified. If it is, warning the user
        elif domain_type == MetricDomainTypes.MULTICOLUMN:
            if "columns" in compute_domain_kwargs:
                # If columns exist
                accessor_domain_kwargs["columns"] = compute_domain_kwargs.pop("columns")

        # Filtering if identity
        elif domain_type == MetricDomainTypes.IDENTITY:
            # If we would like our data to become a single column
            if "column" in compute_domain_kwargs:
                if self.active_batch_data.use_quoted_name:
                    selectable = sa.select(
                        [sa.column(quoted_name(compute_domain_kwargs["column"]))]
                    ).select_from(selectable)
                else:
                    selectable = sa.select(
                        [sa.column(compute_domain_kwargs["column"])]
                    ).select_from(selectable)

            # If we would like our data to now become a column pair
            elif ("column_A" in compute_domain_kwargs) and (
                "column_B" in compute_domain_kwargs
            ):
                if self.active_batch_data.use_quoted_name:
                    selectable = sa.select(
                        [
                            sa.column(quoted_name(compute_domain_kwargs["column_A"])),
                            sa.column(quoted_name(compute_domain_kwargs["column_B"])),
                        ]
                    ).select_from(selectable)
                else:
                    selectable = sa.select(
                        [
                            sa.column(compute_domain_kwargs["column_A"]),
                            sa.column(compute_domain_kwargs["column_B"]),
                        ]
                    ).select_from(selectable)
            else:
                # If we would like our data to become a multicolumn
                if "columns" in compute_domain_kwargs:
                    if self.active_batch_data.use_quoted_name:
                        # Building a list of column objects used for sql alchemy selection
                        to_select = [
                            sa.column(quoted_name(col))
                            for col in compute_domain_kwargs["columns"]
                        ]
                        selectable = sa.select(to_select).select_from(selectable)
                    else:
                        to_select = [
                            sa.column(col) for col in compute_domain_kwargs["columns"]
                        ]
                        selectable = sa.select(to_select).select_from(selectable)

        # Letting selectable fall through
        return selectable, compute_domain_kwargs, accessor_domain_kwargs

    def resolve_metric_bundle(
        self,
        metric_fn_bundle: Iterable[Tuple[MetricConfiguration, Any, dict, dict]],
    ) -> dict:
        """For every metric in a set of Metrics to resolve, obtains necessary metric keyword arguments and builds
        bundles of the metrics into one large query dictionary so that they are all executed simultaneously. Will fail
        if bundling the metrics together is not possible.

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
            try:
                res = self.engine.execute(
                    sa.select(query["select"]).select_from(selectable)
                ).fetchall()
                logger.debug(
                    f"SqlAlchemyExecutionEngine computed {len(res[0])} metrics on domain_id {IDDict(compute_domain_kwargs).to_id()}"
                )
            except OperationalError as oe:
                exception_message: str = "An SQL execution Exception occurred.  "
                exception_traceback: str = traceback.format_exc()
                exception_message += f'{type(oe).__name__}: "{str(oe)}".  Traceback: "{exception_traceback}".'
                logger.error(exception_message)
                raise ExecutionEngineError(message=exception_message)
            assert (
                len(res) == 1
            ), "all bundle-computed metrics must be single-value statistics"
            assert len(query["ids"]) == len(
                res[0]
            ), "unexpected number of metrics returned"
            for idx, id in enumerate(query["ids"]):
                resolved_metrics[id] = convert_to_json_serializable(res[0][idx])

        return resolved_metrics

    ### Splitter methods for partitioning tables ###

    def _split_on_whole_table(self, table_name: str, batch_identifiers: dict):
        """'Split' by returning the whole table"""

        # return sa.column(column_name) == batch_identifiers[column_name]
        return 1 == 1

    def _split_on_column_value(
        self, table_name: str, column_name: str, batch_identifiers: dict
    ):
        """Split using the values in the named column"""

        return sa.column(column_name) == batch_identifiers[column_name]

    def _split_on_converted_datetime(
        self,
        table_name: str,
        column_name: str,
        batch_identifiers: dict,
        date_format_string: str = "%Y-%m-%d",
    ):
        """Convert the values in the named column to the given date_format, and split on that"""

        return (
            sa.func.strftime(
                date_format_string,
                sa.column(column_name),
            )
            == batch_identifiers[column_name]
        )

    def _split_on_divided_integer(
        self, table_name: str, column_name: str, divisor: int, batch_identifiers: dict
    ):
        """Divide the values in the named column by `divisor`, and split on that"""

        return (
            sa.cast(sa.column(column_name) / divisor, sa.Integer)
            == batch_identifiers[column_name]
        )

    def _split_on_mod_integer(
        self, table_name: str, column_name: str, mod: int, batch_identifiers: dict
    ):
        """Divide the values in the named column by `divisor`, and split on that"""

        return sa.column(column_name) % mod == batch_identifiers[column_name]

    def _split_on_multi_column_values(
        self, table_name: str, column_names: List[str], batch_identifiers: dict
    ):
        """Split on the joint values in the named columns"""

        return sa.and_(
            *[
                sa.column(column_name) == column_value
                for column_name, column_value in batch_identifiers.items()
            ]
        )

    def _split_on_hashed_column(
        self,
        table_name: str,
        column_name: str,
        hash_digits: int,
        batch_identifiers: dict,
    ):
        """Split on the hashed value of the named column"""

        return (
            sa.func.right(sa.func.md5(sa.column(column_name)), hash_digits)
            == batch_identifiers[column_name]
        )

    ### Sampling methods ###

    # _sample_using_limit
    # _sample_using_random
    # _sample_using_mod
    # _sample_using_a_list
    # _sample_using_md5

    def _sample_using_random(
        self,
        p: float = 0.1,
    ):
        """Take a random sample of rows, retaining proportion p

        Note: the Random function behaves differently on different dialects of SQL
        """
        return sa.func.random() < p

    def _sample_using_mod(
        self,
        column_name,
        mod: int,
        value: int,
    ):
        """Take the mod of named column, and only keep rows that match the given value"""
        return sa.column(column_name) % mod == value

    def _sample_using_a_list(
        self,
        column_name: str,
        value_list: list,
    ):
        """Match the values in the named column against value_list, and only keep the matches"""
        return sa.column(column_name).in_(value_list)

    def _sample_using_md5(
        self,
        column_name: str,
        hash_digits: int = 1,
        hash_value: str = "f",
    ):
        """Hash the values in the named column, and split on that"""
        return (
            sa.func.right(
                sa.func.md5(sa.cast(sa.column(column_name), sa.Text)), hash_digits
            )
            == hash_value
        )

    def _build_selectable_from_batch_spec(self, batch_spec) -> Select:
        table_name: str = batch_spec["table_name"]

        if "splitter_method" in batch_spec:
            splitter_fn = getattr(self, batch_spec["splitter_method"])
            split_clause = splitter_fn(
                table_name=table_name,
                batch_identifiers=batch_spec["batch_identifiers"],
                **batch_spec["splitter_kwargs"],
            )

        else:
            split_clause = True

        if "sampling_method" in batch_spec:
            if batch_spec["sampling_method"] == "_sample_using_limit":
                # SQLalchemy's semantics for LIMIT are different than normal WHERE clauses,
                # so the business logic for building the query needs to be different.

                return (
                    sa.select("*")
                    .select_from(
                        sa.table(table_name, schema=batch_spec.get("schema_name", None))
                    )
                    .where(split_clause)
                    .limit(batch_spec["sampling_kwargs"]["n"])
                )

            else:

                sampler_fn = getattr(self, batch_spec["sampling_method"])
                return (
                    sa.select("*")
                    .select_from(sa.text(table_name))
                    .where(
                        sa.and_(
                            split_clause,
                            sampler_fn(**batch_spec["sampling_kwargs"]),
                        )
                    )
                )
        return (
            sa.select("*")
            .select_from(
                sa.table(table_name, schema=batch_spec.get("schema_name", None))
            )
            .where(split_clause)
        )

    def get_batch_data_and_markers(
        self, batch_spec: BatchSpec
    ) -> Tuple[Any, BatchMarkers]:
        if not isinstance(
            batch_spec, (SqlAlchemyDatasourceBatchSpec, RuntimeQueryBatchSpec)
        ):
            raise InvalidBatchSpecError(
                f"""SqlAlchemyExecutionEngine accepts batch_spec only of type SqlAlchemyDatasourceBatchSpec or
        RuntimeQueryBatchSpec (illegal type "{str(type(batch_spec))}" was received).
                        """
            )

        batch_data: SqlAlchemyBatchData
        batch_markers: BatchMarkers = BatchMarkers(
            {
                "ge_load_time": datetime.datetime.now(datetime.timezone.utc).strftime(
                    "%Y%m%dT%H%M%S.%fZ"
                )
            }
        )
        if isinstance(batch_spec, RuntimeQueryBatchSpec):
            # query != None is already checked when RuntimeQueryBatchSpec is instantiated
            query: str = batch_spec.query
            if query:
                batch_spec.query = "SQLQuery"
                batch_data = SqlAlchemyBatchData(execution_engine=self, query=query)
                return batch_data, batch_markers

        selectable: Select = self._build_selectable_from_batch_spec(
            batch_spec=batch_spec
        )
        temp_table_name: Optional[str]
        if "bigquery_temp_table" in batch_spec:
            temp_table_name = batch_spec.get("bigquery_temp_table")
        else:
            temp_table_name = None

        source_table_name = batch_spec.get("table_name", None)
        source_schema_name = batch_spec.get("schema_name", None)

        batch_data = SqlAlchemyBatchData(
            execution_engine=self,
            selectable=selectable,
            temp_table_name=temp_table_name,
            create_temp_table=batch_spec.get(
                "create_temp_table", self._create_temp_table
            ),
            source_table_name=source_table_name,
            source_schema_name=source_schema_name,
        )
        return batch_data, batch_markers
