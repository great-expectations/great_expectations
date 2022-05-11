
import copy
import datetime
import logging
import random
import string
import traceback
import warnings
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union
from great_expectations._version import get_versions
__version__ = get_versions()['version']
from great_expectations.core.usage_statistics.events import UsageStatsEvents
from great_expectations.execution_engine.split_and_sample.sqlalchemy_data_sampler import SqlAlchemyDataSampler
from great_expectations.execution_engine.split_and_sample.sqlalchemy_data_splitter import SqlAlchemyDataSplitter
del get_versions
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchMarkers, BatchSpec
from great_expectations.core.batch_spec import RuntimeQueryBatchSpec, SqlAlchemyDatasourceBatchSpec
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.types.base import ConcurrencyConfig
from great_expectations.exceptions import DatasourceKeyPairAuthBadPassphraseError, ExecutionEngineError, GreatExpectationsError, InvalidBatchSpecError, InvalidConfigError
from great_expectations.exceptions import exceptions as ge_exceptions
from great_expectations.execution_engine import ExecutionEngine
from great_expectations.execution_engine.execution_engine import MetricDomainTypes, SplitDomainKwargs
from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData
from great_expectations.expectations.row_conditions import RowCondition, RowConditionParserType, parse_condition_to_sqlalchemy
from great_expectations.util import filter_properties_dict, get_sqlalchemy_selectable, get_sqlalchemy_url, import_library_module, import_make_url
from great_expectations.validator.metric_configuration import MetricConfiguration
logger = logging.getLogger(__name__)
try:
    import sqlalchemy as sa
    make_url = import_make_url()
except ImportError:
    sa = None
try:
    from sqlalchemy.engine import LegacyRow
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.sql import Selectable
    from sqlalchemy.sql.elements import BooleanClauseList, Label, TextClause, quoted_name
except ImportError:
    LegacyRow = None
    reflection = None
    DefaultDialect = None
    Selectable = None
    BooleanClauseList = None
    TextClause = None
    quoted_name = None
    OperationalError = None
    Label = None
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
    import sqlalchemy_dremio.pyodbc
    if sa:
        sa.dialects.registry.register('dremio', 'sqlalchemy_dremio.pyodbc', 'dialect')
except ImportError:
    sqlalchemy_dremio = None
try:
    import snowflake.sqlalchemy.snowdialect
    if sa:
        sa.dialects.registry.register('snowflake', 'snowflake.sqlalchemy', 'dialect')
except (ImportError, KeyError, AttributeError):
    snowflake = None
_BIGQUERY_MODULE_NAME = 'sqlalchemy_bigquery'
try:
    import sqlalchemy_bigquery as sqla_bigquery
    sa.dialects.registry.register('bigquery', _BIGQUERY_MODULE_NAME, 'dialect')
    bigquery_types_tuple = None
except ImportError:
    try:
        import pybigquery.sqlalchemy_bigquery as sqla_bigquery
        warnings.warn('The pybigquery package is obsolete and its usage within Great Expectations is deprecated as of v0.14.7. As support will be removed in v0.17, please transition to sqlalchemy-bigquery', DeprecationWarning)
        _BIGQUERY_MODULE_NAME = 'pybigquery.sqlalchemy_bigquery'
        sa.dialects.registry.register('bigquery', _BIGQUERY_MODULE_NAME, 'dialect')
        try:
            getattr(sqla_bigquery, 'INTEGER')
            bigquery_types_tuple = None
        except AttributeError:
            logger.warning('Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later.')
            from collections import namedtuple
            BigQueryTypes = namedtuple('BigQueryTypes', sorted(sqla_bigquery._type_map))
            bigquery_types_tuple = BigQueryTypes(**sqla_bigquery._type_map)
    except (ImportError, AttributeError):
        sqla_bigquery = None
        bigquery_types_tuple = None
        pybigquery = None
try:
    import teradatasqlalchemy.dialect
    import teradatasqlalchemy.types as teradatatypes
except ImportError:
    teradatasqlalchemy = None

def _get_dialect_type_module(dialect):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Given a dialect, returns the dialect type, which is defines the engine/system that is used to communicates\n    with the database/database implementation. Currently checks for RedShift/BigQuery dialects'
    if (dialect is None):
        logger.warning('No sqlalchemy dialect found; relying in top-level sqlalchemy types.')
        return sa
    try:
        if isinstance(dialect, sqlalchemy_redshift.dialect.RedshiftDialect):
            return dialect.sa
    except (TypeError, AttributeError):
        pass
    try:
        if (isinstance(dialect, sqla_bigquery.BigQueryDialect) and (bigquery_types_tuple is not None)):
            return bigquery_types_tuple
    except (TypeError, AttributeError):
        pass
    try:
        if (issubclass(dialect, teradatasqlalchemy.dialect.TeradataDialect) and (teradatatypes is not None)):
            return teradatatypes
    except (TypeError, AttributeError):
        pass
    return dialect

class SqlAlchemyExecutionEngine(ExecutionEngine):

    def __init__(self, name: Optional[str]=None, credentials: Optional[dict]=None, data_context: Optional[Any]=None, engine=None, connection_string: Optional[str]=None, url: Optional[str]=None, batch_data_dict: Optional[dict]=None, create_temp_table: bool=True, concurrency: Optional[ConcurrencyConfig]=None, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Builds a SqlAlchemyExecutionEngine, using a provided connection string/url/engine/credentials to access the\n        desired database. Also initializes the dialect to be used and configures usage statistics.\n\n            Args:\n                name (str):                     The name of the SqlAlchemyExecutionEngine\n                credentials:                     If the Execution Engine is not provided, the credentials can be used to build the Execution\n                    Engine. If the Engine is provided, it will be used instead\n                data_context (DataContext):                     An object representing a Great Expectations project that can be used to access Expectation\n                    Suites and the Project Data itself\n                engine (Engine):                     A SqlAlchemy Engine used to set the SqlAlchemyExecutionEngine being configured, useful if an\n                    Engine has already been configured and should be reused. Will override Credentials\n                    if provided.\n                connection_string (string):                     If neither the engines nor the credentials have been provided, a connection string can be used\n                    to access the data. This will be overridden by both the engine and credentials if those are\n                    provided.\n                url (string):                     If neither the engines, the credentials, nor the connection_string have been provided,\n                    a url can be used to access the data. This will be overridden by all other configuration\n                    options if any are provided.\n                concurrency (ConcurrencyConfig): Concurrency config used to configure the sqlalchemy engine.\n        '
        super().__init__(name=name, batch_data_dict=batch_data_dict)
        self._name = name
        self._credentials = credentials
        self._connection_string = connection_string
        self._url = url
        self._create_temp_table = create_temp_table
        if (engine is not None):
            if (credentials is not None):
                logger.warning('Both credentials and engine were provided during initialization of SqlAlchemyExecutionEngine. Ignoring credentials.')
            self.engine = engine
        else:
            concurrency: ConcurrencyConfig
            if ((data_context is None) or (data_context.concurrency is None)):
                concurrency = ConcurrencyConfig()
            else:
                concurrency = data_context.concurrency
            concurrency.add_sqlalchemy_create_engine_parameters(kwargs)
            if (credentials is not None):
                self.engine = self._build_engine(credentials=credentials, **kwargs)
            elif (connection_string is not None):
                self.engine = sa.create_engine(connection_string, **kwargs)
            elif (url is not None):
                parsed_url = make_url(url)
                self.drivername = parsed_url.drivername
                self.engine = sa.create_engine(url, **kwargs)
            else:
                raise InvalidConfigError('Credentials or an engine are required for a SqlAlchemyExecutionEngine.')
        if (self.engine.dialect.name.lower() in ['trino', 'awsathena']):
            self._create_temp_table = False
        if (self.engine.dialect.name.lower() in ['postgresql', 'mysql', 'sqlite', 'oracle', 'mssql']):
            self.dialect_module = import_library_module(module_name=f'sqlalchemy.dialects.{self.engine.dialect.name}')
        elif (self.engine.dialect.name.lower() == 'snowflake'):
            self.dialect_module = import_library_module(module_name='snowflake.sqlalchemy.snowdialect')
        elif (self.engine.dialect.name.lower() == 'dremio'):
            self.dialect_module = import_library_module(module_name='sqlalchemy_dremio.pyodbc')
        elif (self.engine.dialect.name.lower() == 'redshift'):
            self.dialect_module = import_library_module(module_name='sqlalchemy_redshift.dialect')
        elif (self.engine.dialect.name.lower() == 'bigquery'):
            self.dialect_module = import_library_module(module_name=_BIGQUERY_MODULE_NAME)
        elif (self.engine.dialect.name.lower() == 'teradatasql'):
            self.dialect_module = import_library_module(module_name='teradatasqlalchemy.dialect')
        else:
            self.dialect_module = None
        self._engine_backup = None
        if (self.engine and (self.engine.dialect.name.lower() in ['sqlite', 'mssql', 'snowflake', 'mysql'])):
            self._engine_backup = self.engine
            self.engine = self.engine.connect()
        if ((data_context is not None) and getattr(data_context, '_usage_statistics_handler', None)):
            handler = data_context._usage_statistics_handler
            handler.send_usage_message(event=UsageStatsEvents.EXECUTION_ENGINE_SQLALCHEMY_CONNECT.value, event_payload={'anonymized_name': handler.anonymizer.anonymize(self.name), 'sqlalchemy_dialect': self.engine.name}, success=True)
        self._config = {'name': name, 'credentials': credentials, 'data_context': data_context, 'engine': engine, 'connection_string': connection_string, 'url': url, 'batch_data_dict': batch_data_dict, 'module_name': self.__class__.__module__, 'class_name': self.__class__.__name__}
        self._config.update(kwargs)
        filter_properties_dict(properties=self._config, clean_falsy=True, inplace=True)
        self._data_splitter = SqlAlchemyDataSplitter()
        self._data_sampler = SqlAlchemyDataSampler()

    @property
    def credentials(self) -> Optional[dict]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._credentials

    @property
    def connection_string(self) -> Optional[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._connection_string

    @property
    def url(self) -> Optional[str]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        return self._url

    def _build_engine(self, credentials: dict, **kwargs) -> 'sa.engine.Engine':
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Using a set of given credentials, constructs an Execution Engine , connecting to a database using a URL or a\n        private key path.\n        '
        drivername = credentials.pop('drivername')
        schema_name = credentials.pop('schema_name', None)
        if (schema_name is not None):
            logger.warning('schema_name specified creating a URL with schema is not supported. Set a default schema on the user connecting to your database.')
        create_engine_kwargs = kwargs
        connect_args = credentials.pop('connect_args', None)
        if connect_args:
            create_engine_kwargs['connect_args'] = connect_args
        if ('private_key_path' in credentials):
            (options, create_engine_kwargs) = self._get_sqlalchemy_key_pair_auth_url(drivername, credentials)
        else:
            options = get_sqlalchemy_url(drivername, **credentials)
        self.drivername = drivername
        engine = sa.create_engine(options, **create_engine_kwargs)
        return engine

    def _get_sqlalchemy_key_pair_auth_url(self, drivername: str, credentials: dict) -> Tuple[('sa.engine.url.URL', Dict)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Utilizing a private key path and a passphrase in a given credentials dictionary, attempts to encode the provided\n        values into a private key. If passphrase is incorrect, this will fail and an exception is raised.\n\n        Args:\n            drivername(str) - The name of the driver class\n            credentials(dict) - A dictionary of database credentials used to access the database\n\n        Returns:\n            a tuple consisting of a url with the serialized key-pair authentication, and a dictionary of engine kwargs.\n        '
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization
        private_key_path = credentials.pop('private_key_path')
        private_key_passphrase = credentials.pop('private_key_passphrase')
        with Path(private_key_path).expanduser().resolve().open(mode='rb') as key:
            try:
                p_key = serialization.load_pem_private_key(key.read(), password=(private_key_passphrase.encode() if private_key_passphrase else None), backend=default_backend())
            except ValueError as e:
                if ('incorrect password' in str(e).lower()):
                    raise DatasourceKeyPairAuthBadPassphraseError(datasource_name='SqlAlchemyDatasource', message='Decryption of key failed, was the passphrase incorrect?') from e
                else:
                    raise e
        pkb = p_key.private_bytes(encoding=serialization.Encoding.DER, format=serialization.PrivateFormat.PKCS8, encryption_algorithm=serialization.NoEncryption())
        credentials_driver_name = credentials.pop('drivername', None)
        create_engine_kwargs = {'connect_args': {'private_key': pkb}}
        return (get_sqlalchemy_url((drivername or credentials_driver_name), **credentials), create_engine_kwargs)

    def get_domain_records(self, domain_kwargs: Dict) -> Selectable:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Uses the given domain kwargs (which include row_condition, condition_parser, and ignore_row_if directives) to\n        obtain and/or query a batch. Returns in the format of an SqlAlchemy table/column(s) object.\n\n        Args:\n            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain\n\n        Returns:\n            An SqlAlchemy table/column(s) (the selectable object for obtaining data on which to compute)\n        '
        batch_id = domain_kwargs.get('batch_id')
        if (batch_id is None):
            if self.active_batch_data:
                data_object = self.active_batch_data
            else:
                raise GreatExpectationsError('No batch is specified, but could not identify a loaded batch.')
        elif (batch_id in self.loaded_batch_data_dict):
            data_object = self.loaded_batch_data_dict[batch_id]
        else:
            raise GreatExpectationsError(f'Unable to find batch with batch_id {batch_id}')
        selectable: Selectable
        if (('table' in domain_kwargs) and (domain_kwargs['table'] is not None)):
            if (domain_kwargs['table'] != data_object.selectable.name):
                selectable = sa.Table(domain_kwargs['table'], sa.MetaData(), schema=data_object._schema_name)
            else:
                selectable = data_object.selectable
        elif ('query' in domain_kwargs):
            raise ValueError('query is not currently supported by SqlAlchemyExecutionEngine')
        else:
            selectable = data_object.selectable
        '\n        If a custom query is passed, selectable will be TextClause and not formatted\n        as a subquery wrapped in "(subquery) alias". TextClause must first be converted\n        to TextualSelect using sa.columns() before it can be converted to type Subquery\n        '
        if (TextClause and isinstance(selectable, TextClause)):
            selectable = selectable.columns().subquery()
        if (('row_condition' in domain_kwargs) and (domain_kwargs['row_condition'] is not None)):
            condition_parser = domain_kwargs['condition_parser']
            if (condition_parser == 'great_expectations__experimental__'):
                parsed_condition = parse_condition_to_sqlalchemy(domain_kwargs['row_condition'])
                selectable = sa.select([sa.text('*')]).select_from(selectable).where(parsed_condition)
            else:
                raise GreatExpectationsError('SqlAlchemyExecutionEngine only supports the great_expectations condition_parser.')
        filter_conditions: List[RowCondition] = domain_kwargs.get('filter_conditions', [])
        if (len(filter_conditions) == 1):
            filter_condition = filter_conditions[0]
            assert (filter_condition.condition_type == RowConditionParserType.GE), 'filter_condition must be of type GE for SqlAlchemyExecutionEngine'
            selectable = sa.select([sa.text('*')]).select_from(selectable).where(parse_condition_to_sqlalchemy(filter_condition.condition))
        elif (len(filter_conditions) > 1):
            raise GreatExpectationsError('SqlAlchemyExecutionEngine currently only supports a single filter condition.')
        if ('column' in domain_kwargs):
            return selectable
        if (('column_A' in domain_kwargs) and ('column_B' in domain_kwargs) and ('ignore_row_if' in domain_kwargs)):
            if self.active_batch_data.use_quoted_name:
                column_A_name = quoted_name(domain_kwargs['column_A'], quote=True)
                column_B_name = quoted_name(domain_kwargs['column_B'], quote=True)
            else:
                column_A_name = domain_kwargs['column_A']
                column_B_name = domain_kwargs['column_B']
            ignore_row_if = domain_kwargs['ignore_row_if']
            if (ignore_row_if == 'both_values_are_missing'):
                selectable = get_sqlalchemy_selectable(sa.select([sa.text('*')]).select_from(get_sqlalchemy_selectable(selectable)).where(sa.not_(sa.and_((sa.column(column_A_name) == None), (sa.column(column_B_name) == None)))))
            elif (ignore_row_if == 'either_value_is_missing'):
                selectable = get_sqlalchemy_selectable(sa.select([sa.text('*')]).select_from(get_sqlalchemy_selectable(selectable)).where(sa.not_(sa.or_((sa.column(column_A_name) == None), (sa.column(column_B_name) == None)))))
            else:
                if (ignore_row_if not in ['neither', 'never']):
                    raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')
                if (ignore_row_if == 'never'):
                    warnings.warn(f'''The correct "no-action" value of the "ignore_row_if" directive for the column pair case is "neither" (the use of "{ignore_row_if}" is deprecated as of v0.13.29 and will be removed in v0.16).  Please use "neither" moving forward.
''', DeprecationWarning)
            return selectable
        if (('column_list' in domain_kwargs) and ('ignore_row_if' in domain_kwargs)):
            if self.active_batch_data.use_quoted_name:
                column_list = [quoted_name(domain_kwargs[column_name], quote=True) for column_name in domain_kwargs['column_list']]
            else:
                column_list = domain_kwargs['column_list']
            ignore_row_if = domain_kwargs['ignore_row_if']
            if (ignore_row_if == 'all_values_are_missing'):
                selectable = get_sqlalchemy_selectable(sa.select([sa.text('*')]).select_from(get_sqlalchemy_selectable(selectable)).where(sa.not_(sa.and_(*((sa.column(column_name) == None) for column_name in column_list)))))
            elif (ignore_row_if == 'any_value_is_missing'):
                selectable = get_sqlalchemy_selectable(sa.select([sa.text('*')]).select_from(get_sqlalchemy_selectable(selectable)).where(sa.not_(sa.or_(*((sa.column(column_name) == None) for column_name in column_list)))))
            elif (ignore_row_if != 'never'):
                raise ValueError(f'Unrecognized value of ignore_row_if ("{ignore_row_if}").')
            return selectable
        return selectable

    def get_compute_domain(self, domain_kwargs: Dict, domain_type: Union[(str, MetricDomainTypes)], accessor_keys: Optional[Iterable[str]]=None) -> Tuple[(Selectable, dict, dict)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Uses a given batch dictionary and domain kwargs to obtain a SqlAlchemy column object.\n\n        Args:\n            domain_kwargs (dict) - A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type (str or MetricDomainTypes) - an Enum value indicating which metric domain the user would\n            like to be using, or a corresponding string value representing it. String types include "identity",\n            "column", "column_pair", "table" and "other". Enum types include capitalized versions of these from the\n            class MetricDomainTypes.\n            accessor_keys (str iterable) - keys that are part of the compute domain but should be ignored when\n            describing the domain and simply transferred with their associated values into accessor_domain_kwargs.\n\n        Returns:\n            SqlAlchemy column\n        '
        selectable = self.get_domain_records(domain_kwargs)
        split_domain_kwargs = self._split_domain_kwargs(domain_kwargs, domain_type, accessor_keys)
        return (selectable, split_domain_kwargs.compute, split_domain_kwargs.accessor)

    def _split_column_metric_domain_kwargs(self, domain_kwargs: Dict, domain_type: MetricDomainTypes) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for column domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs split from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        assert (domain_type == MetricDomainTypes.COLUMN), 'This method only supports MetricDomainTypes.COLUMN'
        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}
        if ('column' not in compute_domain_kwargs):
            raise ge_exceptions.GreatExpectationsError('Column not provided in compute_domain_kwargs')
        if self.active_batch_data.use_quoted_name:
            accessor_domain_kwargs['column'] = quoted_name(compute_domain_kwargs.pop('column'), quote=True)
        else:
            accessor_domain_kwargs['column'] = compute_domain_kwargs.pop('column')
        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    def _split_column_pair_metric_domain_kwargs(self, domain_kwargs: Dict, domain_type: MetricDomainTypes) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for column pair domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs split from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        assert (domain_type == MetricDomainTypes.COLUMN_PAIR), 'This method only supports MetricDomainTypes.COLUMN_PAIR'
        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}
        if (not (('column_A' in compute_domain_kwargs) and ('column_B' in compute_domain_kwargs))):
            raise ge_exceptions.GreatExpectationsError('column_A or column_B not found within compute_domain_kwargs')
        if self.active_batch_data.use_quoted_name:
            accessor_domain_kwargs['column_A'] = quoted_name(compute_domain_kwargs.pop('column_A'), quote=True)
            accessor_domain_kwargs['column_B'] = quoted_name(compute_domain_kwargs.pop('column_B'), quote=True)
        else:
            accessor_domain_kwargs['column_A'] = compute_domain_kwargs.pop('column_A')
            accessor_domain_kwargs['column_B'] = compute_domain_kwargs.pop('column_B')
        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    def _split_multi_column_metric_domain_kwargs(self, domain_kwargs: Dict, domain_type: MetricDomainTypes) -> SplitDomainKwargs:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Split domain_kwargs for multicolumn domain types into compute and accessor domain kwargs.\n\n        Args:\n            domain_kwargs: A dictionary consisting of the domain kwargs specifying which data to obtain\n            domain_type: an Enum value indicating which metric domain the user would\n            like to be using.\n\n        Returns:\n            compute_domain_kwargs, accessor_domain_kwargs split from domain_kwargs\n            The union of compute_domain_kwargs, accessor_domain_kwargs is the input domain_kwargs\n        '
        assert (domain_type == MetricDomainTypes.MULTICOLUMN), 'This method only supports MetricDomainTypes.MULTICOLUMN'
        compute_domain_kwargs: Dict = copy.deepcopy(domain_kwargs)
        accessor_domain_kwargs: Dict = {}
        if ('column_list' not in domain_kwargs):
            raise GreatExpectationsError('column_list not found within domain_kwargs')
        column_list = compute_domain_kwargs.pop('column_list')
        if (len(column_list) < 2):
            raise GreatExpectationsError('column_list must contain at least 2 columns')
        if self.active_batch_data.use_quoted_name:
            accessor_domain_kwargs['column_list'] = [quoted_name(column_name, quote=True) for column_name in column_list]
        else:
            accessor_domain_kwargs['column_list'] = column_list
        return SplitDomainKwargs(compute_domain_kwargs, accessor_domain_kwargs)

    def resolve_metric_bundle(self, metric_fn_bundle: Iterable[Tuple[(MetricConfiguration, Any, dict, dict)]]) -> Dict[(Tuple[(str, str, str)], Any)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        "For every metric in a set of Metrics to resolve, obtains necessary metric keyword arguments and builds\n        bundles of the metrics into one large query dictionary so that they are all executed simultaneously. Will fail\n        if bundling the metrics together is not possible.\n\n            Args:\n                metric_fn_bundle (Iterable[Tuple[MetricConfiguration, Callable, dict]):                     A Dictionary containing a MetricProvider's MetricConfiguration (its unique identifier), its metric provider function\n                    (the function that actually executes the metric), and the arguments to pass to the metric provider function.\n                    A dictionary of metrics defined in the registry and corresponding arguments\n\n            Returns:\n                A dictionary of metric names and their corresponding now-queried values.\n        "
        resolved_metrics = {}
        queries: Dict[(Tuple, dict)] = {}
        for (metric_to_resolve, engine_fn, compute_domain_kwargs, accessor_domain_kwargs, metric_provider_kwargs) in metric_fn_bundle:
            if (not isinstance(compute_domain_kwargs, IDDict)):
                compute_domain_kwargs = IDDict(compute_domain_kwargs)
            domain_id = compute_domain_kwargs.to_id()
            if (domain_id not in queries):
                queries[domain_id] = {'select': [], 'ids': [], 'domain_kwargs': compute_domain_kwargs}
            if (self.engine.dialect.name == 'clickhouse'):
                queries[domain_id]['select'].append(engine_fn.label(metric_to_resolve.metric_name.join(random.choices(string.ascii_lowercase, k=2))))
            else:
                queries[domain_id]['select'].append(engine_fn.label(metric_to_resolve.metric_name))
            queries[domain_id]['ids'].append(metric_to_resolve.id)
        for query in queries.values():
            domain_kwargs = query['domain_kwargs']
            selectable = self.get_domain_records(domain_kwargs=domain_kwargs)
            assert (len(query['select']) == len(query['ids']))
            try:
                '\n                If a custom query is passed, selectable will be TextClause and not formatted\n                as a subquery wrapped in "(subquery) alias". TextClause must first be converted\n                to TextualSelect using sa.columns() before it can be converted to type Subquery\n                '
                if (TextClause and isinstance(selectable, TextClause)):
                    res = self.engine.execute(sa.select(query['select']).select_from(selectable.columns().subquery())).fetchall()
                else:
                    res = self.engine.execute(sa.select(query['select']).select_from(selectable)).fetchall()
                logger.debug(f'SqlAlchemyExecutionEngine computed {len(res[0])} metrics on domain_id {IDDict(domain_kwargs).to_id()}')
            except OperationalError as oe:
                exception_message: str = 'An SQL execution Exception occurred.  '
                exception_traceback: str = traceback.format_exc()
                exception_message += f'{type(oe).__name__}: "{str(oe)}".  Traceback: "{exception_traceback}".'
                logger.error(exception_message)
                raise ExecutionEngineError(message=exception_message)
            assert (len(res) == 1), 'all bundle-computed metrics must be single-value statistics'
            assert (len(query['ids']) == len(res[0])), 'unexpected number of metrics returned'
            for (idx, id) in enumerate(query['ids']):
                resolved_metrics[id] = convert_to_json_serializable(res[0][idx])
        return resolved_metrics

    def close(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        '\n        Note: Will 20210729\n\n        This is a helper function that will close and dispose Sqlalchemy objects that are used to connect to a database.\n        Databases like Snowflake require the connection and engine to be instantiated and closed separately, and not\n        doing so has caused problems with hanging connections.\n\n        Currently the ExecutionEngine does not support handling connections and engine separately, and will actually\n        override the engine with a connection in some cases, obfuscating what object is used to actually used by the\n        ExecutionEngine to connect to the external database. This will be handled in an upcoming refactor, which will\n        allow this function to eventually become:\n\n        self.connection.close()\n        self.engine.dispose()\n\n        More background can be found here: https://github.com/great-expectations/great_expectations/pull/3104/\n        '
        if self._engine_backup:
            self.engine.close()
            self._engine_backup.dispose()
        else:
            self.engine.dispose()

    def _get_splitter_method(self, splitter_method_name: str) -> Callable:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Get the appropriate splitter method from the method name.\n\n        Args:\n            splitter_method_name: name of the splitter to retrieve.\n\n        Returns:\n            splitter method.\n        '
        return self._data_splitter.get_splitter_method(splitter_method_name)

    def execute_split_query(self, split_query: Selectable) -> List[LegacyRow]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Use the execution engine to run the split query and fetch all of the results.\n\n        Args:\n            split_query: Query to be executed as a sqlalchemy Selectable.\n\n        Returns:\n            List of row results.\n        '
        return self.engine.execute(split_query).fetchall()

    def get_data_for_batch_identifiers(self, table_name: str, splitter_method_name: str, splitter_kwargs: dict) -> List[dict]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        'Build data used to construct batch identifiers for the input table using the provided splitter config.\n\n        Sql splitter configurations yield the unique values that comprise a batch by introspecting your data.\n\n        Args:\n            table_name: Table to split.\n            splitter_method_name: Desired splitter method to use.\n            splitter_kwargs: Dict of directives used by the splitter method as keyword arguments of key=value.\n\n        Returns:\n            List of dicts of the form [{column_name: {"key": value}}]\n        '
        return self._data_splitter.get_data_for_batch_identifiers(execution_engine=self, table_name=table_name, splitter_method_name=splitter_method_name, splitter_kwargs=splitter_kwargs)

    def _build_selectable_from_batch_spec(self, batch_spec: BatchSpec) -> Union[(Selectable, str)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if ('splitter_method' in batch_spec):
            splitter_fn: Callable = self._get_splitter_method(splitter_method_name=batch_spec['splitter_method'])
            split_clause = splitter_fn(batch_identifiers=batch_spec['batch_identifiers'], **batch_spec['splitter_kwargs'])
        else:
            split_clause = True
        table_name: str = batch_spec['table_name']
        if ('sampling_method' in batch_spec):
            if (batch_spec['sampling_method'] in ['_sample_using_limit', 'sample_using_limit']):
                return self._data_sampler.sample_using_limit(execution_engine=self, batch_spec=batch_spec, where_clause=split_clause)
            elif (batch_spec['sampling_method'] in ['_sample_using_random', 'sample_using_random']):
                sampler_fn = self._data_sampler.get_sampler_method(batch_spec['sampling_method'])
                return sampler_fn(execution_engine=self, batch_spec=batch_spec, where_clause=split_clause)
            else:
                sampler_fn = self._data_sampler.get_sampler_method(batch_spec['sampling_method'])
                return sa.select('*').select_from(sa.table(table_name, schema=batch_spec.get('schema_name', None))).where(sa.and_(split_clause, sampler_fn(**batch_spec['sampling_kwargs'])))
        return sa.select('*').select_from(sa.table(table_name, schema=batch_spec.get('schema_name', None))).where(split_clause)

    def get_batch_data_and_markers(self, batch_spec: BatchSpec) -> Tuple[(Any, BatchMarkers)]:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (not isinstance(batch_spec, (SqlAlchemyDatasourceBatchSpec, RuntimeQueryBatchSpec))):
            raise InvalidBatchSpecError(f'''SqlAlchemyExecutionEngine accepts batch_spec only of type SqlAlchemyDatasourceBatchSpec or
        RuntimeQueryBatchSpec (illegal type "{str(type(batch_spec))}" was received).
                        ''')
        batch_data: Optional[SqlAlchemyBatchData] = None
        batch_markers: BatchMarkers = BatchMarkers({'ge_load_time': datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')})
        source_schema_name: str = batch_spec.get('schema_name', None)
        source_table_name: str = batch_spec.get('table_name', None)
        temp_table_schema_name: Optional[str] = batch_spec.get('temp_table_schema_name')
        if batch_spec.get('bigquery_temp_table'):
            warnings.warn('BigQuery tables that are created as the result of a query are no longer created as permanent tables. Thus, a named permanent table through the `bigquery_temp_table`parameter is not required. The `bigquery_temp_table` parameter is deprecated as ofv0.15.3 and will be removed in v0.18.', DeprecationWarning)
        create_temp_table: bool = batch_spec.get('create_temp_table', self._create_temp_table)
        if isinstance(batch_spec, RuntimeQueryBatchSpec):
            query: str = batch_spec.query
            batch_spec.query = 'SQLQuery'
            batch_data = SqlAlchemyBatchData(execution_engine=self, query=query, temp_table_schema_name=temp_table_schema_name, create_temp_table=create_temp_table, source_table_name=source_table_name, source_schema_name=source_schema_name)
        elif isinstance(batch_spec, SqlAlchemyDatasourceBatchSpec):
            if (self.engine.dialect.name.lower() == 'oracle'):
                selectable: str = self._build_selectable_from_batch_spec(batch_spec=batch_spec)
            else:
                selectable: Selectable = self._build_selectable_from_batch_spec(batch_spec=batch_spec)
            batch_data = SqlAlchemyBatchData(execution_engine=self, selectable=selectable, create_temp_table=create_temp_table, source_table_name=source_table_name, source_schema_name=source_schema_name)
        return (batch_data, batch_markers)
