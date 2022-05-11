
import copy
import locale
import logging
import os
import platform
import random
import string
import threading
import traceback
import warnings
from functools import wraps
from types import ModuleType
from typing import Dict, List, Optional, Union
import numpy as np
import pandas as pd
from dateutil.parser import parse
from great_expectations.core import ExpectationConfigurationSchema, ExpectationSuite, ExpectationSuiteSchema, ExpectationSuiteValidationResultSchema, ExpectationValidationResultSchema
from great_expectations.core.batch import Batch, BatchDefinition
from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import ExpectationTestCase, ExpectationTestDataCases
from great_expectations.core.expectation_diagnostics.supporting_types import ExpectationExecutionEngineDiagnostics
from great_expectations.core.util import get_or_create_spark_application, get_sql_dialect_floating_point_infinity_value
from great_expectations.dataset import PandasDataset, SparkDFDataset, SqlAlchemyDataset
from great_expectations.exceptions.exceptions import MetricProviderError, MetricResolutionError
from great_expectations.execution_engine import PandasExecutionEngine, SparkDFExecutionEngine, SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sparkdf_batch_data import SparkDFBatchData
from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData
from great_expectations.profile import ColumnsExistProfiler
from great_expectations.util import import_library_module
from great_expectations.validator.validator import Validator
expectationValidationResultSchema = ExpectationValidationResultSchema()
expectationSuiteValidationResultSchema = ExpectationSuiteValidationResultSchema()
expectationConfigurationSchema = ExpectationConfigurationSchema()
expectationSuiteSchema = ExpectationSuiteSchema()
logger = logging.getLogger(__name__)
try:
    import sqlalchemy as sqlalchemy
    from sqlalchemy import create_engine
    from sqlalchemy.engine import Engine
    from sqlalchemy.exc import SQLAlchemyError
except ImportError:
    sqlalchemy = None
    create_engine = None
    Engine = None
    SQLAlchemyError = None
    logger.debug('Unable to load SqlAlchemy or one of its subclasses.')
try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = None
    SparkDataFrame = type(None)
try:
    from pyspark.sql import DataFrame as spark_DataFrame
except ImportError:
    spark_DataFrame = type(None)
try:
    import sqlalchemy.dialects.sqlite as sqlitetypes
    from sqlalchemy.dialects.sqlite import dialect as sqliteDialect
    SQLITE_TYPES = {'VARCHAR': sqlitetypes.VARCHAR, 'CHAR': sqlitetypes.CHAR, 'INTEGER': sqlitetypes.INTEGER, 'SMALLINT': sqlitetypes.SMALLINT, 'DATETIME': sqlitetypes.DATETIME(truncate_microseconds=True), 'DATE': sqlitetypes.DATE, 'FLOAT': sqlitetypes.FLOAT, 'BOOLEAN': sqlitetypes.BOOLEAN, 'TIMESTAMP': sqlitetypes.TIMESTAMP}
except (ImportError, KeyError):
    sqlitetypes = None
    sqliteDialect = None
    SQLITE_TYPES = {}
_BIGQUERY_MODULE_NAME = 'sqlalchemy_bigquery'
try:
    import sqlalchemy_bigquery as sqla_bigquery
    import sqlalchemy_bigquery as BigQueryDialect
    sqlalchemy.dialects.registry.register('bigquery', _BIGQUERY_MODULE_NAME, 'dialect')
    bigquery_types_tuple = None
    BIGQUERY_TYPES = {'INTEGER': sqla_bigquery.INTEGER, 'NUMERIC': sqla_bigquery.NUMERIC, 'STRING': sqla_bigquery.STRING, 'BIGNUMERIC': sqla_bigquery.BIGNUMERIC, 'BYTES': sqla_bigquery.BYTES, 'BOOL': sqla_bigquery.BOOL, 'BOOLEAN': sqla_bigquery.BOOLEAN, 'TIMESTAMP': sqla_bigquery.TIMESTAMP, 'TIME': sqla_bigquery.TIME, 'FLOAT': sqla_bigquery.FLOAT, 'DATE': sqla_bigquery.DATE, 'DATETIME': sqla_bigquery.DATETIME}
    try:
        from sqlalchemy_bigquery import GEOGRAPHY
        BIGQUERY_TYPES['GEOGRAPHY'] = GEOGRAPHY
    except ImportError:
        pass
except ImportError:
    try:
        import pybigquery.sqlalchemy_bigquery as sqla_bigquery
        import pybigquery.sqlalchemy_bigquery as BigQueryDialect
        warnings.warn('The pybigquery package is obsolete and its usage within Great Expectations is deprecated as of v0.14.7. As support will be removed in v0.17, please transition to sqlalchemy-bigquery', DeprecationWarning)
        _BIGQUERY_MODULE_NAME = 'pybigquery.sqlalchemy_bigquery'
        sqlalchemy.dialects.registry.register('bigquery', _BIGQUERY_MODULE_NAME, 'dialect')
        try:
            getattr(sqla_bigquery, 'INTEGER')
            bigquery_types_tuple = {}
            BIGQUERY_TYPES = {'INTEGER': sqla_bigquery.INTEGER, 'NUMERIC': sqla_bigquery.NUMERIC, 'STRING': sqla_bigquery.STRING, 'BIGNUMERIC': sqla_bigquery.BIGNUMERIC, 'BYTES': sqla_bigquery.BYTES, 'BOOL': sqla_bigquery.BOOL, 'BOOLEAN': sqla_bigquery.BOOLEAN, 'TIMESTAMP': sqla_bigquery.TIMESTAMP, 'TIME': sqla_bigquery.TIME, 'FLOAT': sqla_bigquery.FLOAT, 'DATE': sqla_bigquery.DATE, 'DATETIME': sqla_bigquery.DATETIME}
        except AttributeError:
            logger.warning('Old pybigquery driver version detected. Consider upgrading to 0.4.14 or later.')
            from collections import namedtuple
            BigQueryTypes = namedtuple('BigQueryTypes', sorted(sqla_bigquery._type_map))
            bigquery_types_tuple = BigQueryTypes(**sqla_bigquery._type_map)
            BIGQUERY_TYPES = {}
    except (ImportError, AttributeError):
        sqla_bigquery = None
        bigquery_types_tuple = None
        BigQueryDialect = None
        pybigquery = None
        BIGQUERY_TYPES = {}
try:
    import sqlalchemy.dialects.postgresql as postgresqltypes
    from sqlalchemy.dialects.postgresql import dialect as postgresqlDialect
    POSTGRESQL_TYPES = {'TEXT': postgresqltypes.TEXT, 'CHAR': postgresqltypes.CHAR, 'INTEGER': postgresqltypes.INTEGER, 'SMALLINT': postgresqltypes.SMALLINT, 'BIGINT': postgresqltypes.BIGINT, 'TIMESTAMP': postgresqltypes.TIMESTAMP, 'DATE': postgresqltypes.DATE, 'DOUBLE_PRECISION': postgresqltypes.DOUBLE_PRECISION, 'BOOLEAN': postgresqltypes.BOOLEAN, 'NUMERIC': postgresqltypes.NUMERIC}
except (ImportError, KeyError):
    postgresqltypes = None
    postgresqlDialect = None
    POSTGRESQL_TYPES = {}
try:
    import sqlalchemy.dialects.mysql as mysqltypes
    from sqlalchemy.dialects.mysql import dialect as mysqlDialect
    MYSQL_TYPES = {'TEXT': mysqltypes.TEXT, 'CHAR': mysqltypes.CHAR, 'INTEGER': mysqltypes.INTEGER, 'SMALLINT': mysqltypes.SMALLINT, 'BIGINT': mysqltypes.BIGINT, 'DATETIME': mysqltypes.DATETIME, 'TIMESTAMP': mysqltypes.TIMESTAMP, 'DATE': mysqltypes.DATE, 'FLOAT': mysqltypes.FLOAT, 'DOUBLE': mysqltypes.DOUBLE, 'BOOLEAN': mysqltypes.BOOLEAN, 'TINYINT': mysqltypes.TINYINT}
except (ImportError, KeyError):
    mysqltypes = None
    mysqlDialect = None
    MYSQL_TYPES = {}
try:
    import sqlalchemy.dialects.mssql as mssqltypes
    from sqlalchemy.dialects.mssql import dialect as mssqlDialect
    MSSQL_TYPES = {'BIGINT': mssqltypes.BIGINT, 'BINARY': mssqltypes.BINARY, 'BIT': mssqltypes.BIT, 'CHAR': mssqltypes.CHAR, 'DATE': mssqltypes.DATE, 'DATETIME': mssqltypes.DATETIME, 'DATETIME2': mssqltypes.DATETIME2, 'DATETIMEOFFSET': mssqltypes.DATETIMEOFFSET, 'DECIMAL': mssqltypes.DECIMAL, 'FLOAT': mssqltypes.FLOAT, 'IMAGE': mssqltypes.IMAGE, 'INTEGER': mssqltypes.INTEGER, 'MONEY': mssqltypes.MONEY, 'NCHAR': mssqltypes.NCHAR, 'NTEXT': mssqltypes.NTEXT, 'NUMERIC': mssqltypes.NUMERIC, 'NVARCHAR': mssqltypes.NVARCHAR, 'REAL': mssqltypes.REAL, 'SMALLDATETIME': mssqltypes.SMALLDATETIME, 'SMALLINT': mssqltypes.SMALLINT, 'SMALLMONEY': mssqltypes.SMALLMONEY, 'SQL_VARIANT': mssqltypes.SQL_VARIANT, 'TEXT': mssqltypes.TEXT, 'TIME': mssqltypes.TIME, 'TIMESTAMP': mssqltypes.TIMESTAMP, 'TINYINT': mssqltypes.TINYINT, 'UNIQUEIDENTIFIER': mssqltypes.UNIQUEIDENTIFIER, 'VARBINARY': mssqltypes.VARBINARY, 'VARCHAR': mssqltypes.VARCHAR}
except (ImportError, KeyError):
    mssqltypes = None
    mssqlDialect = None
    MSSQL_TYPES = {}
try:
    import trino
    import trino.sqlalchemy.datatype as trinotypes
    from trino.sqlalchemy.dialect import TrinoDialect as trinoDialect
    TRINO_TYPES = {'BOOLEAN': trinotypes._type_map['boolean'], 'TINYINT': trinotypes._type_map['tinyint'], 'SMALLINT': trinotypes._type_map['smallint'], 'INT': trinotypes._type_map['int'], 'INTEGER': trinotypes._type_map['integer'], 'BIGINT': trinotypes._type_map['bigint'], 'REAL': trinotypes._type_map['real'], 'DOUBLE': trinotypes._type_map['double'], 'DECIMAL': trinotypes._type_map['decimal'], 'VARCHAR': trinotypes._type_map['varchar'], 'CHAR': trinotypes._type_map['char'], 'VARBINARY': trinotypes._type_map['varbinary'], 'JSON': trinotypes._type_map['json'], 'DATE': trinotypes._type_map['date'], 'TIME': trinotypes._type_map['time'], 'TIMESTAMP': trinotypes._type_map['timestamp']}
except (ImportError, KeyError):
    trino = None
    trinotypes = None
    trinoDialect = None
    TRINO_TYPES = {}
import tempfile
SQL_DIALECT_NAMES = ('sqlite', 'postgresql', 'mysql', 'mssql', 'bigquery', 'trino')

class SqlAlchemyConnectionManager():

    def __init__(self) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.lock = threading.Lock()
        self._connections = {}

    def get_engine(self, connection_string):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        if (sqlalchemy is not None):
            with self.lock:
                if (connection_string not in self._connections):
                    try:
                        engine = create_engine(connection_string)
                        conn = engine.connect()
                        self._connections[connection_string] = conn
                    except (ImportError, SQLAlchemyError):
                        print(f'Unable to establish connection with {connection_string}')
                        raise
                return self._connections[connection_string]
        return None
connection_manager = SqlAlchemyConnectionManager()

class LockingConnectionCheck():

    def __init__(self, sa, connection_string) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        self.lock = threading.Lock()
        self.sa = sa
        self.connection_string = connection_string
        self._is_valid = None

    def is_valid(self):
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        with self.lock:
            if (self._is_valid is None):
                try:
                    engine = self.sa.create_engine(self.connection_string)
                    conn = engine.connect()
                    conn.close()
                    self._is_valid = True
                except (ImportError, self.sa.exc.SQLAlchemyError) as e:
                    print(f'{str(e)}')
                    self._is_valid = False
            return self._is_valid

def get_sqlite_connection_url(sqlite_db_path):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    url = 'sqlite://'
    if (sqlite_db_path is not None):
        extra_slash = ''
        if (platform.system() != 'Windows'):
            extra_slash = '/'
        url = f'{url}/{extra_slash}{sqlite_db_path}'
    return url

def get_dataset(dataset_type, data, schemas=None, profiler=ColumnsExistProfiler, caching=True, table_name=None, sqlite_db_path=None):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Utility to create datasets for json-formatted tests'
    df = pd.DataFrame(data)
    if (dataset_type == 'PandasDataset'):
        if (schemas and ('pandas' in schemas)):
            schema = schemas['pandas']
            pandas_schema = {}
            for (key, value) in schema.items():
                if (value.lower() in ['timestamp', 'datetime64[ns, tz]']):
                    df[key] = pd.to_datetime(df[key], utc=True)
                    continue
                elif (value.lower() in ['datetime', 'datetime64', 'datetime64[ns]']):
                    df[key] = pd.to_datetime(df[key])
                    continue
                elif (value.lower() in ['date']):
                    df[key] = pd.to_datetime(df[key]).dt.date
                    value = 'object'
                try:
                    type_ = np.dtype(value)
                except TypeError:
                    type_ = getattr(pd.core.dtypes.dtypes, value)
                pandas_schema[key] = type_
            df = df.astype(pandas_schema)
        return PandasDataset(df, profiler=profiler, caching=caching)
    elif (dataset_type == 'sqlite'):
        if ((not create_engine) or (not SQLITE_TYPES)):
            return None
        engine = create_engine(get_sqlite_connection_url(sqlite_db_path=sqlite_db_path))
        sql_dtypes = {}
        if (schemas and ('sqlite' in schemas) and isinstance(engine.dialect, sqlitetypes.dialect)):
            schema = schemas['sqlite']
            sql_dtypes = {col: SQLITE_TYPES[dtype] for (col, dtype) in schema.items()}
            for col in schema:
                type_ = schema[col]
                if (type_ in ['INTEGER', 'SMALLINT', 'BIGINT']):
                    df[col] = pd.to_numeric(df[col], downcast='signed')
                elif (type_ in ['FLOAT', 'DOUBLE', 'DOUBLE_PRECISION']):
                    df[col] = pd.to_numeric(df[col])
                    min_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=True)
                    max_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=False)
                    for api_schema_type in ['api_np', 'api_cast']:
                        min_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=True)
                        max_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=False)
                        df.replace(to_replace=[min_value_api, max_value_api], value=[min_value_dbms, max_value_dbms], inplace=True)
                elif (type_ in ['DATETIME', 'TIMESTAMP']):
                    df[col] = pd.to_datetime(df[col])
                elif (type_ in ['DATE']):
                    df[col] = pd.to_datetime(df[col]).dt.date
        if (table_name is None):
            table_name = generate_test_table_name()
        df.to_sql(name=table_name, con=engine, index=False, dtype=sql_dtypes, if_exists='replace')
        return SqlAlchemyDataset(table_name, engine=engine, profiler=profiler, caching=caching)
    elif (dataset_type == 'postgresql'):
        if ((not create_engine) or (not POSTGRESQL_TYPES)):
            return None
        db_hostname = os.getenv('GE_TEST_LOCAL_DB_HOSTNAME', 'localhost')
        engine = connection_manager.get_engine(f'postgresql://postgres@{db_hostname}/test_ci')
        sql_dtypes = {}
        if (schemas and ('postgresql' in schemas) and isinstance(engine.dialect, postgresqltypes.dialect)):
            schema = schemas['postgresql']
            sql_dtypes = {col: POSTGRESQL_TYPES[dtype] for (col, dtype) in schema.items()}
            for col in schema:
                type_ = schema[col]
                if (type_ in ['INTEGER', 'SMALLINT', 'BIGINT']):
                    df[col] = pd.to_numeric(df[col], downcast='signed')
                elif (type_ in ['FLOAT', 'DOUBLE', 'DOUBLE_PRECISION']):
                    df[col] = pd.to_numeric(df[col])
                    min_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=True)
                    max_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=False)
                    for api_schema_type in ['api_np', 'api_cast']:
                        min_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=True)
                        max_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=False)
                        df.replace(to_replace=[min_value_api, max_value_api], value=[min_value_dbms, max_value_dbms], inplace=True)
                elif (type_ in ['DATETIME', 'TIMESTAMP']):
                    df[col] = pd.to_datetime(df[col])
                elif (type_ in ['DATE']):
                    df[col] = pd.to_datetime(df[col]).dt.date
        if (table_name is None):
            table_name = generate_test_table_name()
        df.to_sql(name=table_name, con=engine, index=False, dtype=sql_dtypes, if_exists='replace')
        return SqlAlchemyDataset(table_name, engine=engine, profiler=profiler, caching=caching)
    elif (dataset_type == 'mysql'):
        if ((not create_engine) or (not MYSQL_TYPES)):
            return None
        db_hostname = os.getenv('GE_TEST_LOCAL_DB_HOSTNAME', 'localhost')
        engine = create_engine(f'mysql+pymysql://root@{db_hostname}/test_ci')
        sql_dtypes = {}
        if (schemas and ('mysql' in schemas) and isinstance(engine.dialect, mysqltypes.dialect)):
            schema = schemas['mysql']
            sql_dtypes = {col: MYSQL_TYPES[dtype] for (col, dtype) in schema.items()}
            for col in schema:
                type_ = schema[col]
                if (type_ in ['INTEGER', 'SMALLINT', 'BIGINT']):
                    df[col] = pd.to_numeric(df[col], downcast='signed')
                elif (type_ in ['FLOAT', 'DOUBLE', 'DOUBLE_PRECISION']):
                    df[col] = pd.to_numeric(df[col])
                    min_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=True)
                    max_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=False)
                    for api_schema_type in ['api_np', 'api_cast']:
                        min_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=True)
                        max_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=False)
                        df.replace(to_replace=[min_value_api, max_value_api], value=[min_value_dbms, max_value_dbms], inplace=True)
                elif (type_ in ['DATETIME', 'TIMESTAMP']):
                    df[col] = pd.to_datetime(df[col])
                elif (type_ in ['DATE']):
                    df[col] = pd.to_datetime(df[col]).dt.date
        if (table_name is None):
            table_name = generate_test_table_name()
        df.to_sql(name=table_name, con=engine, index=False, dtype=sql_dtypes, if_exists='replace')
        custom_sql: str = f'SELECT * FROM {table_name}'
        return SqlAlchemyDataset(custom_sql=custom_sql, engine=engine, profiler=profiler, caching=caching)
    elif (dataset_type == 'bigquery'):
        if (not create_engine):
            return None
        engine = _create_bigquery_engine()
        if (schemas and (dataset_type in schemas)):
            schema = schemas[dataset_type]
        df.columns = df.columns.str.replace(' ', '_')
        if (table_name is None):
            table_name = generate_test_table_name()
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        custom_sql = f'SELECT * FROM {_bigquery_dataset()}.{table_name}'
        return SqlAlchemyDataset(custom_sql=custom_sql, engine=engine, profiler=profiler, caching=caching)
    elif (dataset_type == 'trino'):
        if ((not create_engine) or (not TRINO_TYPES)):
            return None
        db_hostname = os.getenv('GE_TEST_LOCAL_DB_HOSTNAME', 'localhost')
        engine = _create_trino_engine(db_hostname)
        sql_dtypes = {}
        if (schemas and ('trino' in schemas) and isinstance(engine.dialect, trinoDialect)):
            schema = schemas['trino']
            sql_dtypes = {col: TRINO_TYPES[dtype] for (col, dtype) in schema.items()}
            for col in schema:
                type_ = schema[col]
                if (type_ in ['INTEGER', 'SMALLINT', 'BIGINT']):
                    df[col] = pd.to_numeric(df[col], downcast='signed')
                elif (type_ in ['FLOAT', 'DOUBLE', 'DOUBLE_PRECISION']):
                    df[col] = pd.to_numeric(df[col])
                    min_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=True)
                    max_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=False)
                    for api_schema_type in ['api_np', 'api_cast']:
                        min_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=True)
                        max_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=False)
                        df.replace(to_replace=[min_value_api, max_value_api], value=[min_value_dbms, max_value_dbms], inplace=True)
                elif (type_ in ['DATETIME', 'TIMESTAMP']):
                    df[col] = pd.to_datetime(df[col])
                elif (type_ in ['DATE']):
                    df[col] = pd.to_datetime(df[col]).dt.date
        if (table_name is None):
            table_name = generate_test_table_name()
        df.to_sql(name=table_name, con=engine, index=False, dtype=sql_dtypes, if_exists='replace')
        return SqlAlchemyDataset(table_name, engine=engine, profiler=profiler, caching=caching)
    elif (dataset_type == 'mssql'):
        if ((not create_engine) or (not MSSQL_TYPES)):
            return None
        db_hostname = os.getenv('GE_TEST_LOCAL_DB_HOSTNAME', 'localhost')
        engine = create_engine(f'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true')
        sql_dtypes = {}
        if (schemas and (dataset_type in schemas) and isinstance(engine.dialect, mssqltypes.dialect)):
            schema = schemas[dataset_type]
            sql_dtypes = {col: MSSQL_TYPES[dtype] for (col, dtype) in schema.items()}
            for col in schema:
                type_ = schema[col]
                if (type_ in ['INTEGER', 'SMALLINT', 'BIGINT']):
                    df[col] = pd.to_numeric(df[col], downcast='signed')
                elif (type_ in ['FLOAT']):
                    df[col] = pd.to_numeric(df[col])
                    min_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=True)
                    max_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=dataset_type, negative=False)
                    for api_schema_type in ['api_np', 'api_cast']:
                        min_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=True)
                        max_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=False)
                        df.replace(to_replace=[min_value_api, max_value_api], value=[min_value_dbms, max_value_dbms], inplace=True)
                elif (type_ in ['DATETIME', 'TIMESTAMP']):
                    df[col] = pd.to_datetime(df[col])
                elif (type_ in ['DATE']):
                    df[col] = pd.to_datetime(df[col]).dt.date
        if (table_name is None):
            table_name = generate_test_table_name()
        df.to_sql(name=table_name, con=engine, index=False, dtype=sql_dtypes, if_exists='replace')
        return SqlAlchemyDataset(table_name, engine=engine, profiler=profiler, caching=caching)
    elif (dataset_type == 'SparkDFDataset'):
        import pyspark.sql.types as sparktypes
        spark_types = {'StringType': sparktypes.StringType, 'IntegerType': sparktypes.IntegerType, 'LongType': sparktypes.LongType, 'DateType': sparktypes.DateType, 'TimestampType': sparktypes.TimestampType, 'FloatType': sparktypes.FloatType, 'DoubleType': sparktypes.DoubleType, 'BooleanType': sparktypes.BooleanType, 'DataType': sparktypes.DataType, 'NullType': sparktypes.NullType}
        spark = get_or_create_spark_application(spark_config={'spark.sql.catalogImplementation': 'hive', 'spark.executor.memory': '450m'})
        data_reshaped = list(zip(*(v for (_, v) in data.items())))
        if (schemas and ('spark' in schemas)):
            schema = schemas['spark']
            try:
                spark_schema = sparktypes.StructType([sparktypes.StructField(column, spark_types[schema[column]](), True) for column in schema])
                data = copy.deepcopy(data)
                if ('ts' in data):
                    print(data)
                    print(schema)
                for col in schema:
                    type_ = schema[col]
                    if (type_ in ['IntegerType', 'LongType']):
                        vals = []
                        for val in data[col]:
                            if (val is None):
                                vals.append(val)
                            else:
                                vals.append(int(val))
                        data[col] = vals
                    elif (type_ in ['FloatType', 'DoubleType']):
                        vals = []
                        for val in data[col]:
                            if (val is None):
                                vals.append(val)
                            else:
                                vals.append(float(val))
                        data[col] = vals
                    elif (type_ in ['DateType', 'TimestampType']):
                        vals = []
                        for val in data[col]:
                            if (val is None):
                                vals.append(val)
                            else:
                                vals.append(parse(val))
                        data[col] = vals
                data_reshaped = list(zip(*(v for (_, v) in data.items())))
                spark_df = spark.createDataFrame(data_reshaped, schema=spark_schema)
            except TypeError:
                string_schema = sparktypes.StructType([sparktypes.StructField(column, sparktypes.StringType()) for column in schema])
                spark_df = spark.createDataFrame(data_reshaped, string_schema)
                for c in spark_df.columns:
                    spark_df = spark_df.withColumn(c, spark_df[c].cast(spark_types[schema[c]]()))
        elif (len(data_reshaped) == 0):
            columns = list(data.keys())
            spark_schema = sparktypes.StructType([sparktypes.StructField(column, sparktypes.StringType()) for column in columns])
            spark_df = spark.createDataFrame(data_reshaped, spark_schema)
        else:
            columns = list(data.keys())
            spark_df = spark.createDataFrame(data_reshaped, columns)
        return SparkDFDataset(spark_df, profiler=profiler, caching=caching)
    else:
        raise ValueError(f'Unknown dataset_type {str(dataset_type)}')

def get_test_validator_with_data(execution_engine, data, schemas=None, caching=True, table_name=None, sqlite_db_path=None):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Utility to create datasets for json-formatted tests.'
    df = pd.DataFrame(data)
    if (execution_engine == 'pandas'):
        if (schemas and ('pandas' in schemas)):
            schema = schemas['pandas']
            pandas_schema = {}
            for (key, value) in schema.items():
                if (value.lower() in ['timestamp', 'datetime64[ns, tz]']):
                    df[key] = pd.to_datetime(df[key], utc=True)
                    continue
                elif (value.lower() in ['datetime', 'datetime64', 'datetime64[ns]']):
                    df[key] = pd.to_datetime(df[key])
                    continue
                elif (value.lower() in ['date']):
                    df[key] = pd.to_datetime(df[key]).dt.date
                    value = 'object'
                try:
                    type_ = np.dtype(value)
                except TypeError:
                    type_ = getattr(pd.core.dtypes.dtypes, value)
                pandas_schema[key] = type_
            df = df.astype(pandas_schema)
        if (table_name is None):
            table_name = generate_test_table_name()
        return build_pandas_validator_with_data(df=df)
    elif (execution_engine in SQL_DIALECT_NAMES):
        if (not create_engine):
            return None
        if (table_name is None):
            table_name = generate_test_table_name().lower()
        result = build_sa_validator_with_data(df=df, sa_engine_name=execution_engine, schemas=schemas, caching=caching, table_name=table_name, sqlite_db_path=sqlite_db_path)
        return result
    elif (execution_engine == 'spark'):
        import pyspark.sql.types as sparktypes
        spark_types: dict = {'StringType': sparktypes.StringType, 'IntegerType': sparktypes.IntegerType, 'LongType': sparktypes.LongType, 'DateType': sparktypes.DateType, 'TimestampType': sparktypes.TimestampType, 'FloatType': sparktypes.FloatType, 'DoubleType': sparktypes.DoubleType, 'BooleanType': sparktypes.BooleanType, 'DataType': sparktypes.DataType, 'NullType': sparktypes.NullType}
        spark = get_or_create_spark_application(spark_config={'spark.sql.catalogImplementation': 'hive', 'spark.executor.memory': '450m'})
        data_reshaped = list(zip(*(v for (_, v) in data.items())))
        if (schemas and ('spark' in schemas)):
            schema = schemas['spark']
            try:
                spark_schema = sparktypes.StructType([sparktypes.StructField(column, spark_types[schema[column]](), True) for column in schema])
                data = copy.deepcopy(data)
                if ('ts' in data):
                    print(data)
                    print(schema)
                for col in schema:
                    type_ = schema[col]
                    if (type_ in ['IntegerType', 'LongType']):
                        vals = []
                        for val in data[col]:
                            if (val is None):
                                vals.append(val)
                            else:
                                vals.append(int(val))
                        data[col] = vals
                    elif (type_ in ['FloatType', 'DoubleType']):
                        vals = []
                        for val in data[col]:
                            if (val is None):
                                vals.append(val)
                            else:
                                vals.append(float(val))
                        data[col] = vals
                    elif (type_ in ['DateType', 'TimestampType']):
                        vals = []
                        for val in data[col]:
                            if (val is None):
                                vals.append(val)
                            else:
                                vals.append(parse(val))
                        data[col] = vals
                data_reshaped = list(zip(*(v for (_, v) in data.items())))
                spark_df = spark.createDataFrame(data_reshaped, schema=spark_schema)
            except TypeError:
                string_schema = sparktypes.StructType([sparktypes.StructField(column, sparktypes.StringType()) for column in schema])
                spark_df = spark.createDataFrame(data_reshaped, string_schema)
                for c in spark_df.columns:
                    spark_df = spark_df.withColumn(c, spark_df[c].cast(spark_types[schema[c]]()))
        elif (len(data_reshaped) == 0):
            columns = list(data.keys())
            spark_schema = sparktypes.StructType([sparktypes.StructField(column, sparktypes.StringType()) for column in columns])
            spark_df = spark.createDataFrame(data_reshaped, spark_schema)
        else:
            columns = list(data.keys())
            spark_df = spark.createDataFrame(data_reshaped, columns)
        if (table_name is None):
            table_name = generate_test_table_name()
        return build_spark_validator_with_data(df=spark_df, spark=spark)
    else:
        raise ValueError(f'Unknown dataset_type {str(execution_engine)}')

def build_pandas_validator_with_data(df: pd.DataFrame, batch_definition: Optional[BatchDefinition]=None) -> Validator:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    batch: Batch = Batch(data=df, batch_definition=batch_definition)
    return Validator(execution_engine=PandasExecutionEngine(), batches=[batch])

def build_sa_validator_with_data(df, sa_engine_name, schemas=None, caching=True, table_name=None, sqlite_db_path=None, batch_definition: Optional[BatchDefinition]=None):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    dialect_classes = {}
    dialect_types = {}
    try:
        dialect_classes['sqlite'] = sqlitetypes.dialect
        dialect_types['sqlite'] = SQLITE_TYPES
    except AttributeError:
        pass
    try:
        dialect_classes['postgresql'] = postgresqltypes.dialect
        dialect_types['postgresql'] = POSTGRESQL_TYPES
    except AttributeError:
        pass
    try:
        dialect_classes['mysql'] = mysqltypes.dialect
        dialect_types['mysql'] = MYSQL_TYPES
    except AttributeError:
        pass
    try:
        dialect_classes['mssql'] = mssqltypes.dialect
        dialect_types['mssql'] = MSSQL_TYPES
    except AttributeError:
        pass
    try:
        dialect_classes['bigquery'] = sqla_bigquery.BigQueryDialect
        dialect_types['bigquery'] = BIGQUERY_TYPES
    except AttributeError:
        pass
    try:
        dialect_classes['trino'] = trinoDialect
        dialect_types['trino'] = TRINO_TYPES
    except AttributeError:
        pass
    db_hostname = os.getenv('GE_TEST_LOCAL_DB_HOSTNAME', 'localhost')
    if (sa_engine_name == 'sqlite'):
        engine = create_engine(get_sqlite_connection_url(sqlite_db_path))
    elif (sa_engine_name == 'postgresql'):
        engine = connection_manager.get_engine(f'postgresql://postgres@{db_hostname}/test_ci')
    elif (sa_engine_name == 'mysql'):
        engine = create_engine(f'mysql+pymysql://root@{db_hostname}/test_ci')
    elif (sa_engine_name == 'mssql'):
        engine = create_engine(f'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true')
    elif (sa_engine_name == 'bigquery'):
        engine = _create_bigquery_engine()
    elif (sa_engine_name == 'trino'):
        engine = _create_trino_engine(db_hostname)
    else:
        engine = None
    if (sa_engine_name == 'bigquery'):
        df.columns = df.columns.str.replace(' ', '_')
    sql_dtypes = {}
    if (schemas and (sa_engine_name in schemas) and isinstance(engine.dialect, dialect_classes.get(sa_engine_name))):
        schema = schemas[sa_engine_name]
        sql_dtypes = {col: dialect_types.get(sa_engine_name)[dtype] for (col, dtype) in schema.items()}
        for col in schema:
            type_ = schema[col]
            if (type_ in ['INTEGER', 'SMALLINT', 'BIGINT']):
                df[col] = pd.to_numeric(df[col], downcast='signed')
            elif (type_ in ['FLOAT', 'DOUBLE', 'DOUBLE_PRECISION']):
                df[col] = pd.to_numeric(df[col])
                min_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=sa_engine_name, negative=True)
                max_value_dbms = get_sql_dialect_floating_point_infinity_value(schema=sa_engine_name, negative=False)
                for api_schema_type in ['api_np', 'api_cast']:
                    min_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=True)
                    max_value_api = get_sql_dialect_floating_point_infinity_value(schema=api_schema_type, negative=False)
                    df.replace(to_replace=[min_value_api, max_value_api], value=[min_value_dbms, max_value_dbms], inplace=True)
            elif (type_ in ['DATETIME', 'TIMESTAMP', 'DATE']):
                df[col] = pd.to_datetime(df[col])
            elif (type_ in ['VARCHAR', 'STRING']):
                df[col] = df[col].apply(str)
    if (table_name is None):
        table_name = generate_test_table_name()
    if (sa_engine_name in ['trino']):
        table_name = table_name.lower()
        sql_insert_method = 'multi'
    else:
        sql_insert_method = None
    df.to_sql(name=table_name, con=engine, index=False, dtype=sql_dtypes, if_exists='replace', method=sql_insert_method)
    batch_data = SqlAlchemyBatchData(execution_engine=engine, table_name=table_name)
    batch = Batch(data=batch_data, batch_definition=batch_definition)
    execution_engine = SqlAlchemyExecutionEngine(caching=caching, engine=engine)
    return Validator(execution_engine=execution_engine, batches=[batch])

def modify_locale(func):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')

    @wraps(func)
    def locale_wrapper(*args, **kwargs) -> None:
        import inspect
        __frame = inspect.currentframe()
        __file = __frame.f_code.co_filename
        __func = __frame.f_code.co_name
        for (k, v) in __frame.f_locals.items():
            if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
                continue
            print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
        old_locale = locale.setlocale(locale.LC_TIME, None)
        print(old_locale)
        try:
            new_locale = locale.setlocale(locale.LC_TIME, 'en_US.UTF-8')
            assert (new_locale == 'en_US.UTF-8')
            func(*args, **kwargs)
        except Exception:
            raise
        finally:
            locale.setlocale(locale.LC_TIME, old_locale)
    return locale_wrapper

def build_spark_validator_with_data(df: Union[(pd.DataFrame, SparkDataFrame)], spark: SparkSession, batch_definition: Optional[BatchDefinition]=None) -> Validator:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if isinstance(df, pd.DataFrame):
        df = spark.createDataFrame([tuple(((None if (isinstance(x, (float, int)) and np.isnan(x)) else x) for x in record.tolist())) for record in df.to_records(index=False)], df.columns.tolist())
    batch: Batch = Batch(data=df, batch_definition=batch_definition)
    execution_engine: SparkDFExecutionEngine = build_spark_engine(spark=spark, df=df, batch_id=batch.id)
    return Validator(execution_engine=execution_engine, batches=[batch])

def build_pandas_engine(df: pd.DataFrame) -> PandasExecutionEngine:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    batch: Batch = Batch(data=df)
    execution_engine: PandasExecutionEngine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    return execution_engine

def build_sa_engine(df: pd.DataFrame, sa: ModuleType, schema: Optional[str]=None, if_exists: str='fail', index: bool=False, dtype: Optional[dict]=None) -> SqlAlchemyExecutionEngine:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    table_name: str = 'test'
    sqlalchemy_engine: Engine = sa.create_engine('sqlite://', echo=False)
    df.to_sql(name=table_name, con=sqlalchemy_engine, schema=schema, if_exists=if_exists, index=index, dtype=dtype)
    execution_engine: SqlAlchemyExecutionEngine
    execution_engine = SqlAlchemyExecutionEngine(engine=sqlalchemy_engine)
    batch_data: SqlAlchemyBatchData = SqlAlchemyBatchData(execution_engine=execution_engine, table_name=table_name)
    batch: Batch = Batch(data=batch_data)
    execution_engine = SqlAlchemyExecutionEngine(engine=sqlalchemy_engine, batch_data_dict={batch.id: batch_data})
    return execution_engine

def build_spark_engine(spark: SparkSession, df: Union[(pd.DataFrame, SparkDataFrame)], batch_id: Optional[str]=None, batch_definition: Optional[BatchDefinition]=None) -> SparkDFExecutionEngine:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (sum((bool(x) for x in [(batch_id is not None), (batch_definition is not None)])) != 1):
        raise ValueError('Exactly one of batch_id or batch_definition must be specified.')
    if (batch_id is None):
        batch_id = batch_definition.id
    if isinstance(df, pd.DataFrame):
        df = spark.createDataFrame([tuple(((None if (isinstance(x, (float, int)) and np.isnan(x)) else x) for x in record.tolist())) for record in df.to_records(index=False)], df.columns.tolist())
    conf: List[tuple] = spark.sparkContext.getConf().getAll()
    spark_config: Dict[(str, str)] = dict(conf)
    execution_engine: SparkDFExecutionEngine = SparkDFExecutionEngine(spark_config=spark_config)
    execution_engine.load_batch_data(batch_id=batch_id, batch_data=df)
    return execution_engine

def candidate_getter_is_on_temporary_notimplemented_list(context, getter):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (context in ['sqlite']):
        return (getter in ['get_column_modes', 'get_column_stdev'])
    if (context in ['postgresql', 'mysql', 'mssql']):
        return (getter in ['get_column_modes'])
    if (context == 'spark'):
        return (getter in [])

def candidate_test_is_on_temporary_notimplemented_list(context, expectation_type):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (context in SQL_DIALECT_NAMES):
        expectations_not_implemented_v2_sql = ['expect_column_values_to_be_increasing', 'expect_column_values_to_be_decreasing', 'expect_column_values_to_match_strftime_format', 'expect_column_values_to_be_dateutil_parseable', 'expect_column_values_to_be_json_parseable', 'expect_column_values_to_match_json_schema', 'expect_column_stdev_to_be_between', 'expect_column_most_common_value_to_be_in_set', 'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than', 'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than', 'expect_column_pair_values_to_be_equal', 'expect_column_pair_values_A_to_be_greater_than_B', 'expect_column_pair_values_to_be_in_set', 'expect_select_column_values_to_be_unique_within_record', 'expect_compound_columns_to_be_unique', 'expect_multicolumn_values_to_be_unique', 'expect_column_pair_cramers_phi_value_to_be_less_than', 'expect_multicolumn_sum_to_equal']
        if (context in ['bigquery']):
            expectations_not_implemented_v2_sql.append('expect_column_kl_divergence_to_be_less_than')
            expectations_not_implemented_v2_sql.append('expect_column_chisquare_test_p_value_to_be_greater_than')
            expectations_not_implemented_v2_sql.append('expect_column_values_to_be_between')
            expectations_not_implemented_v2_sql.append('expect_column_values_to_be_in_set')
            expectations_not_implemented_v2_sql.append('expect_column_values_to_be_in_type_list')
            expectations_not_implemented_v2_sql.append('expect_column_values_to_be_of_type')
            expectations_not_implemented_v2_sql.append('expect_column_values_to_match_like_pattern_list')
            expectations_not_implemented_v2_sql.append('expect_column_values_to_not_match_like_pattern_list')
        return (expectation_type in expectations_not_implemented_v2_sql)
    if (context == 'SparkDFDataset'):
        return (expectation_type in ['expect_column_values_to_be_dateutil_parseable', 'expect_column_values_to_be_json_parseable', 'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than', 'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than', 'expect_compound_columns_to_be_unique', 'expect_column_pair_cramers_phi_value_to_be_less_than', 'expect_table_row_count_to_equal_other_table'])
    if (context == 'PandasDataset'):
        return (expectation_type in ['expect_table_row_count_to_equal_other_table'])
    return False

def candidate_test_is_on_temporary_notimplemented_list_cfe(context, expectation_type):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    candidate_test_is_on_temporary_notimplemented_list_cfe_trino = ['expect_column_distinct_values_to_contain_set', 'expect_column_max_to_be_between', 'expect_column_mean_to_be_between', 'expect_column_median_to_be_between', 'expect_column_min_to_be_between', 'expect_column_most_common_value_to_be_in_set', 'expect_column_quantile_values_to_be_between', 'expect_column_sum_to_be_between', 'expect_column_kl_divergence_to_be_less_than', 'expect_column_value_lengths_to_be_between', 'expect_column_values_to_be_between', 'expect_column_values_to_be_in_set', 'expect_column_values_to_be_in_type_list', 'expect_column_values_to_be_null', 'expect_column_values_to_be_of_type', 'expect_column_values_to_be_unique', 'expect_column_values_to_match_like_pattern', 'expect_column_values_to_match_like_pattern_list', 'expect_column_values_to_match_regex', 'expect_column_values_to_match_regex_list', 'expect_column_values_to_not_be_null', 'expect_column_values_to_not_match_like_pattern', 'expect_column_values_to_not_match_like_pattern_list', 'expect_column_values_to_not_match_regex', 'expect_column_values_to_not_match_regex_list', 'expect_column_pair_values_A_to_be_greater_than_B', 'expect_column_pair_values_to_be_equal', 'expect_column_pair_values_to_be_in_set', 'expect_compound_columns_to_be_unique', 'expect_select_column_values_to_be_unique_within_record', 'expect_table_column_count_to_be_between', 'expect_table_column_count_to_equal', 'expect_table_row_count_to_be_between', 'expect_table_row_count_to_equal']
    candidate_test_is_on_temporary_notimplemented_list_cfe_other_sql = ['expect_column_values_to_be_increasing', 'expect_column_values_to_be_decreasing', 'expect_column_values_to_match_strftime_format', 'expect_column_values_to_be_dateutil_parseable', 'expect_column_values_to_be_json_parseable', 'expect_column_values_to_match_json_schema', 'expect_column_stdev_to_be_between', 'expect_multicolumn_values_to_be_unique', 'expect_column_pair_cramers_phi_value_to_be_less_than', 'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than', 'expect_column_chisquare_test_p_value_to_be_greater_than', 'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than']
    if (context in ['trino']):
        return (expectation_type in set(candidate_test_is_on_temporary_notimplemented_list_cfe_trino).union(set(candidate_test_is_on_temporary_notimplemented_list_cfe_other_sql)))
    if (context in SQL_DIALECT_NAMES):
        expectations_not_implemented_v3_sql = ['expect_column_values_to_be_increasing', 'expect_column_values_to_be_decreasing', 'expect_column_values_to_match_strftime_format', 'expect_column_values_to_be_dateutil_parseable', 'expect_column_values_to_be_json_parseable', 'expect_column_values_to_match_json_schema', 'expect_column_stdev_to_be_between', 'expect_multicolumn_values_to_be_unique', 'expect_column_pair_cramers_phi_value_to_be_less_than', 'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than', 'expect_column_chisquare_test_p_value_to_be_greater_than', 'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than']
        if (context in ['bigquery']):
            expectations_not_implemented_v3_sql.append('expect_column_kl_divergence_to_be_less_than')
            expectations_not_implemented_v3_sql.append('expect_column_quantile_values_to_be_between')
        return (expectation_type in expectations_not_implemented_v3_sql)
    if (context == 'spark'):
        return (expectation_type in ['expect_table_row_count_to_equal_other_table', 'expect_column_values_to_be_in_set', 'expect_column_values_to_not_be_in_set', 'expect_column_values_to_not_match_regex_list', 'expect_column_values_to_match_like_pattern', 'expect_column_values_to_not_match_like_pattern', 'expect_column_values_to_match_like_pattern_list', 'expect_column_values_to_not_match_like_pattern_list', 'expect_column_values_to_be_dateutil_parseable', 'expect_multicolumn_values_to_be_unique', 'expect_column_pair_cramers_phi_value_to_be_less_than', 'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than', 'expect_column_chisquare_test_p_value_to_be_greater_than', 'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than'])
    if (context == 'pandas'):
        return (expectation_type in ['expect_table_row_count_to_equal_other_table', 'expect_column_values_to_match_like_pattern', 'expect_column_values_to_not_match_like_pattern', 'expect_column_values_to_match_like_pattern_list', 'expect_column_values_to_not_match_like_pattern_list', 'expect_multicolumn_values_to_be_unique', 'expect_column_pair_cramers_phi_value_to_be_less_than', 'expect_column_bootstrapped_ks_test_p_value_to_be_greater_than', 'expect_column_chisquare_test_p_value_to_be_greater_than', 'expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than'])
    return False

def build_test_backends_list(include_pandas=True, include_spark=False, include_sqlalchemy=True, include_sqlite=True, include_postgresql=False, include_mysql=False, include_mssql=False, include_bigquery=False, include_aws=False, include_trino=False, raise_exceptions_for_backends: bool=True) -> List[str]:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Attempts to identify supported backends by checking which imports are available.'
    test_backends = []
    if include_pandas:
        test_backends += ['pandas']
    if include_spark:
        try:
            import pyspark
            from pyspark.sql import SparkSession
        except ImportError:
            if (raise_exceptions_for_backends is True):
                raise ValueError('spark tests are requested, but pyspark is not installed')
            else:
                logger.warning('spark tests are requested, but pyspark is not installed')
        else:
            test_backends += ['spark']
    db_hostname = os.getenv('GE_TEST_LOCAL_DB_HOSTNAME', 'localhost')
    if include_sqlalchemy:
        sa: Optional[ModuleType] = import_library_module(module_name='sqlalchemy')
        if (sa is None):
            if (raise_exceptions_for_backends is True):
                raise ImportError('sqlalchemy tests are requested, but sqlalchemy in not installed')
            else:
                logger.warning('sqlalchemy tests are requested, but sqlalchemy in not installed')
            return test_backends
        if include_sqlite:
            test_backends += ['sqlite']
        if include_postgresql:
            connection_string = f'postgresql://postgres@{db_hostname}/test_ci'
            checker = LockingConnectionCheck(sa, connection_string)
            if (checker.is_valid() is True):
                test_backends += ['postgresql']
            elif (raise_exceptions_for_backends is True):
                raise ValueError(f'backend-specific tests are requested, but unable to connect to the database at {connection_string}')
            else:
                logger.warning(f'backend-specific tests are requested, but unable to connect to the database at {connection_string}')
        if include_mysql:
            try:
                engine = create_engine(f'mysql+pymysql://root@{db_hostname}/test_ci')
                conn = engine.connect()
                conn.close()
            except (ImportError, SQLAlchemyError):
                if (raise_exceptions_for_backends is True):
                    raise ImportError(f"mysql tests are requested, but unable to connect to the mysql database at 'mysql+pymysql://root@{db_hostname}/test_ci'")
                else:
                    logger.warning(f"mysql tests are requested, but unable to connect to the mysql database at 'mysql+pymysql://root@{db_hostname}/test_ci'")
            else:
                test_backends += ['mysql']
        if include_mssql:
            try:
                engine = create_engine(f'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true')
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                if (raise_exceptions_for_backends is True):
                    raise ImportError(f"mssql tests are requested, but unable to connect to the mssql database at 'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true'")
                else:
                    logger.warning(f"mssql tests are requested, but unable to connect to the mssql database at 'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true'")
            else:
                test_backends += ['mssql']
        if include_bigquery:
            try:
                engine = _create_bigquery_engine()
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if (raise_exceptions_for_backends is True):
                    raise ImportError('bigquery tests are requested, but unable to connect') from e
                else:
                    logger.warning(f'bigquery tests are requested, but unable to connect; {repr(e)}')
            else:
                test_backends += ['bigquery']
        if include_aws:
            aws_access_key_id: Optional[str] = os.getenv('AWS_ACCESS_KEY_ID')
            aws_secret_access_key: Optional[str] = os.getenv('AWS_SECRET_ACCESS_KEY')
            aws_session_token: Optional[str] = os.getenv('AWS_SESSION_TOKEN')
            aws_config_file: Optional[str] = os.getenv('AWS_CONFIG_FILE')
            if ((not aws_access_key_id) and (not aws_secret_access_key) and (not aws_session_token) and (not aws_config_file)):
                if (raise_exceptions_for_backends is True):
                    raise ImportError('AWS tests are requested, but credentials were not set up')
                else:
                    logger.warning('AWS tests are requested, but credentials were not set up')
        if include_trino:
            try:
                engine = _create_trino_engine(db_hostname)
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if (raise_exceptions_for_backends is True):
                    raise ImportError('trino tests are requested, but unable to connect') from e
                else:
                    logger.warning(f'trino tests are requested, but unable to connect; {repr(e)}')
            else:
                test_backends += ['trino']
    return test_backends

def generate_expectation_tests(expectation_type: str, test_data_cases: List[ExpectationTestDataCases], execution_engine_diagnostics: ExpectationExecutionEngineDiagnostics, raise_exceptions_for_backends: bool=False):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Determine tests to run\n\n    :param expectation_type: snake_case name of the expectation type\n    :param test_data_cases: list of ExpectationTestDataCases that has data, tests, schemas, and backends to use\n    :param execution_engine_diagnostics: ExpectationExecutionEngineDiagnostics object specifying the engines the expectation is implemented for\n    :return: list of parametrized tests with loaded validators and accessible backends\n    '
    parametrized_tests = []
    for d in test_data_cases:
        d = copy.deepcopy(d)
        dialects_to_include = {}
        engines_to_include = {}
        if d.test_backends:
            for tb in d.test_backends:
                engines_to_include[tb.backend] = True
                if (tb.backend == 'sqlalchemy'):
                    for dialect in tb.dialects:
                        dialects_to_include[dialect] = True
        else:
            engines_to_include['pandas'] = execution_engine_diagnostics.PandasExecutionEngine
            engines_to_include['spark'] = execution_engine_diagnostics.SparkDFExecutionEngine
            engines_to_include['sqlalchemy'] = execution_engine_diagnostics.SqlAlchemyExecutionEngine
            if ((engines_to_include.get('sqlalchemy') is True) and (raise_exceptions_for_backends is False)):
                dialects_to_include = {dialect: True for dialect in SQL_DIALECT_NAMES if (dialect != 'bigquery')}
        if ((engines_to_include.get('sqlalchemy') is True) and (not dialects_to_include)):
            dialects_to_include['sqlite'] = True
        backends = build_test_backends_list(include_pandas=engines_to_include.get('pandas', False), include_spark=engines_to_include.get('spark', False), include_sqlalchemy=engines_to_include.get('sqlalchemy', False), include_sqlite=dialects_to_include.get('sqlite', False), include_postgresql=dialects_to_include.get('postgresql', False), include_mysql=dialects_to_include.get('mysql', False), include_mssql=dialects_to_include.get('mssql', False), include_bigquery=dialects_to_include.get('bigquery', False), include_trino=dialects_to_include.get('trino', False), raise_exceptions_for_backends=raise_exceptions_for_backends)
        for c in backends:
            datasets = []
            try:
                if isinstance(d['data'], list):
                    sqlite_db_path = generate_sqlite_db_path()
                    for dataset in d['data']:
                        datasets.append(get_test_validator_with_data(c, dataset['data'], dataset.get('schemas'), table_name=dataset.get('dataset_name'), sqlite_db_path=sqlite_db_path))
                    validator_with_data = datasets[0]
                else:
                    validator_with_data = get_test_validator_with_data(c, d['data'], d['schemas'])
            except Exception as e:
                print('\n\n[[ Problem calling get_test_validator_with_data ]]')
                print(f'expectation_type -> {expectation_type}')
                print(f'''c -> {c}
e -> {e}''')
                print(f"d['data'] -> {d.get('data')}")
                print(f"d['schemas'] -> {d.get('schemas')}")
                print('DataFrame from data without any casting/conversion ->')
                print(pd.DataFrame(d.get('data')))
                print()
                if (('data_alt' in d) and (d['data_alt'] is not None)):
                    print('There is alternate data to try!!')
                    try:
                        if isinstance(d['data_alt'], list):
                            sqlite_db_path = generate_sqlite_db_path()
                            for dataset in d['data_alt']:
                                datasets.append(get_test_validator_with_data(c, dataset['data_alt'], dataset.get('schemas'), table_name=dataset.get('dataset_name'), sqlite_db_path=sqlite_db_path))
                            validator_with_data = datasets[0]
                        else:
                            validator_with_data = get_test_validator_with_data(c, d['data_alt'], d['schemas'])
                    except Exception as e2:
                        print('\n[[ STILL Problem calling get_test_validator_with_data ]]')
                        print(f'expectation_type -> {expectation_type}')
                        print(f'''c -> {c}
e2 -> {e2}''')
                        print(f"d['data_alt'] -> {d.get('data_alt')}")
                        print('DataFrame from data_alt without any casting/conversion ->')
                        print(pd.DataFrame(d.get('data_alt')))
                        print()
                        continue
                    else:
                        print('\n[[ The alternate data worked!! ]]\n')
                else:
                    continue
            except Exception as e:
                continue
            for test in d['tests']:
                if (not should_we_generate_this_test(backend=c, expectation_test_case=test)):
                    continue
                if (('allow_cross_type_comparisons' in test['input']) and validator_with_data and isinstance(validator_with_data.execution_engine.active_batch_data, SqlAlchemyBatchData)):
                    continue
                parametrized_tests.append({'expectation_type': expectation_type, 'validator_with_data': validator_with_data, 'test': test, 'backend': c})
    return parametrized_tests

def should_we_generate_this_test(backend: str, expectation_test_case: ExpectationTestCase):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (backend in expectation_test_case.suppress_test_for):
        return False
    if (('sqlalchemy' in expectation_test_case.suppress_test_for) and (backend in SQL_DIALECT_NAMES)):
        return False
    if ((expectation_test_case.only_for != None) and expectation_test_case.only_for):
        if (not (backend in expectation_test_case.only_for)):
            if (('sqlalchemy' in expectation_test_case.only_for) and (backend in SQL_DIALECT_NAMES)):
                return True
            elif ('pandas' == backend):
                (major, minor, *_) = pd.__version__.split('.')
                if (('pandas_022' in expectation_test_case.only_for) or ('pandas_023' in expectation_test_case.only_for)):
                    if ((major == '0') and (minor in ['22', '23'])):
                        return True
                elif ('pandas>=024' in expectation_test_case.only_for):
                    if (((major == '0') and (int(minor) >= 24)) or (int(major) >= 1)):
                        return True
            return False
    return True

def sort_unexpected_values(test_value_list, result_value_list):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if (isinstance(test_value_list, list) & (len(test_value_list) >= 1)):
        if isinstance(test_value_list[0], dict):
            test_value_list = sorted(test_value_list, key=(lambda x: tuple((x[k] for k in list(test_value_list[0].keys())))))
            result_value_list = sorted(result_value_list, key=(lambda x: tuple((x[k] for k in list(test_value_list[0].keys())))))
        elif (type(test_value_list[0].__lt__(test_value_list[0])) != type(NotImplemented)):
            test_value_list = sorted(test_value_list, key=(lambda x: str(x)))
            result_value_list = sorted(result_value_list, key=(lambda x: str(x)))
    return (test_value_list, result_value_list)

def evaluate_json_test(data_asset, expectation_type, test) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "\n    This method will evaluate the result of a test build using the Great Expectations json test format.\n\n    NOTE: Tests can be suppressed for certain data types if the test contains the Key 'suppress_test_for' with a list\n        of DataAsset types to suppress, such as ['SQLAlchemy', 'Pandas'].\n\n    :param data_asset: (DataAsset) A great expectations DataAsset\n    :param expectation_type: (string) the name of the expectation to be run using the test input\n    :param test: (dict) a dictionary containing information for the test to be run. The dictionary must include:\n        - title: (string) the name of the test\n        - exact_match_out: (boolean) If true, match the 'out' dictionary exactly against the result of the expectation\n        - in: (dict or list) a dictionary of keyword arguments to use to evaluate the expectation or a list of positional arguments\n        - out: (dict) the dictionary keys against which to make assertions. Unless exact_match_out is true, keys must            come from the following list:\n              - success\n              - observed_value\n              - unexpected_index_list\n              - unexpected_list\n              - details\n              - traceback_substring (if present, the string value will be expected as a substring of the exception_traceback)\n    :return: None. asserts correctness of results.\n    "
    data_asset.set_default_expectation_argument('result_format', 'COMPLETE')
    data_asset.set_default_expectation_argument('include_config', False)
    if ('title' not in test):
        raise ValueError("Invalid test configuration detected: 'title' is required.")
    if ('exact_match_out' not in test):
        raise ValueError("Invalid test configuration detected: 'exact_match_out' is required.")
    if ('input' not in test):
        if ('in' in test):
            test['input'] = test['in']
        else:
            raise ValueError("Invalid test configuration detected: 'input' is required.")
    if ('output' not in test):
        if ('out' in test):
            test['output'] = test['out']
        else:
            raise ValueError("Invalid test configuration detected: 'output' is required.")
    if isinstance(test['input'], list):
        result = getattr(data_asset, expectation_type)(*test['input'])
    else:
        result = getattr(data_asset, expectation_type)(**test['input'])
    check_json_test_result(test=test, result=result, data_asset=data_asset)

def evaluate_json_test_cfe(validator, expectation_type, test, raise_exception=True):
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    "\n    This method will evaluate the result of a test build using the Great Expectations json test format.\n\n    NOTE: Tests can be suppressed for certain data types if the test contains the Key 'suppress_test_for' with a list\n        of DataAsset types to suppress, such as ['SQLAlchemy', 'Pandas'].\n\n    :param expectation_type: (string) the name of the expectation to be run using the test input\n    :param test: (dict) a dictionary containing information for the test to be run. The dictionary must include:\n        - title: (string) the name of the test\n        - exact_match_out: (boolean) If true, match the 'out' dictionary exactly against the result of the expectation\n        - in: (dict or list) a dictionary of keyword arguments to use to evaluate the expectation or a list of positional arguments\n        - out: (dict) the dictionary keys against which to make assertions. Unless exact_match_out is true, keys must            come from the following list:\n              - success\n              - observed_value\n              - unexpected_index_list\n              - unexpected_list\n              - details\n              - traceback_substring (if present, the string value will be expected as a substring of the exception_traceback)\n    :param raise_exception: (bool) If False, capture any failed AssertionError from the call to check_json_test_result and return with validation_result\n    :return: Tuple(ExpectationValidationResult, error_message, stack_trace). asserts correctness of results.\n    "
    expectation_suite = ExpectationSuite('json_test_suite', data_context=validator._data_context)
    validator._initialize_expectations(expectation_suite=expectation_suite)
    if ('title' not in test):
        raise ValueError("Invalid test configuration detected: 'title' is required.")
    if ('exact_match_out' not in test):
        raise ValueError("Invalid test configuration detected: 'exact_match_out' is required.")
    if ('input' not in test):
        if ('in' in test):
            test['input'] = test['in']
        else:
            raise ValueError("Invalid test configuration detected: 'input' is required.")
    if ('output' not in test):
        if ('out' in test):
            test['output'] = test['out']
        else:
            raise ValueError("Invalid test configuration detected: 'output' is required.")
    kwargs = copy.deepcopy(test['input'])
    error_message = None
    stack_trace = None
    try:
        if isinstance(test['input'], list):
            result = getattr(validator, expectation_type)(*kwargs)
        else:
            runtime_kwargs = {'result_format': 'COMPLETE', 'include_config': False}
            runtime_kwargs.update(kwargs)
            result = getattr(validator, expectation_type)(**runtime_kwargs)
    except (MetricProviderError, MetricResolutionError) as e:
        if raise_exception:
            raise
        error_message = str(e)
        stack_trace = (traceback.format_exc(),)
        result = None
    else:
        try:
            check_json_test_result(test=test, result=result, data_asset=validator.execution_engine.active_batch_data)
        except Exception as e:
            if raise_exception:
                raise
            error_message = str(e)
            stack_trace = (traceback.format_exc(),)
    return (result, error_message, stack_trace)

def check_json_test_result(test, result, data_asset=None) -> None:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    if ('unexpected_list' in result['result']):
        if (('result' in test['output']) and ('unexpected_list' in test['output']['result'])):
            (test['output']['result']['unexpected_list'], result['result']['unexpected_list']) = sort_unexpected_values(test['output']['result']['unexpected_list'], result['result']['unexpected_list'])
        elif ('unexpected_list' in test['output']):
            (test['output']['unexpected_list'], result['result']['unexpected_list']) = sort_unexpected_values(test['output']['unexpected_list'], result['result']['unexpected_list'])
    if ('partial_unexpected_list' in result['result']):
        if (('result' in test['output']) and ('partial_unexpected_list' in test['output']['result'])):
            (test['output']['result']['partial_unexpected_list'], result['result']['partial_unexpected_list']) = sort_unexpected_values(test['output']['result']['partial_unexpected_list'], result['result']['partial_unexpected_list'])
        elif ('partial_unexpected_list' in test['output']):
            (test['output']['partial_unexpected_list'], result['result']['partial_unexpected_list']) = sort_unexpected_values(test['output']['partial_unexpected_list'], result['result']['partial_unexpected_list'])
    if (test['exact_match_out'] is True):
        assert (result == expectationValidationResultSchema.load(test['output']))
    else:
        result = result.to_json_dict()
        for (key, value) in test['output'].items():
            if (key == 'success'):
                assert (result['success'] == value)
            elif (key == 'observed_value'):
                if ('tolerance' in test):
                    if isinstance(value, dict):
                        assert (set(result['result']['observed_value'].keys()) == set(value.keys()))
                        for (k, v) in value.items():
                            assert np.allclose(result['result']['observed_value'][k], v, rtol=test['tolerance'])
                    else:
                        assert np.allclose(result['result']['observed_value'], value, rtol=test['tolerance'])
                else:
                    assert (result['result']['observed_value'] == value)
            elif (key == 'observed_value_list'):
                assert (result['result']['observed_value'] in value)
            elif (key == 'unexpected_index_list'):
                if isinstance(data_asset, (SqlAlchemyDataset, SparkDFDataset)):
                    pass
                elif isinstance(data_asset, (SqlAlchemyBatchData, SparkDFBatchData)):
                    pass
                else:
                    assert (result['result']['unexpected_index_list'] == value)
            elif (key == 'unexpected_list'):
                assert (result['result']['unexpected_list'] == value), ((('expected ' + str(value)) + ' but got ') + str(result['result']['unexpected_list']))
            elif (key == 'partial_unexpected_list'):
                assert (result['result']['partial_unexpected_list'] == value), ((('expected ' + str(value)) + ' but got ') + str(result['result']['partial_unexpected_list']))
            elif (key == 'details'):
                assert (result['result']['details'] == value)
            elif (key == 'value_counts'):
                for val_count in value:
                    assert (val_count in result['result']['details']['value_counts'])
            elif key.startswith('observed_cdf'):
                if ('x_-1' in key):
                    if key.endswith('gt'):
                        assert (result['result']['details']['observed_cdf']['x'][(- 1)] > value)
                    else:
                        assert (result['result']['details']['observed_cdf']['x'][(- 1)] == value)
                elif ('x_0' in key):
                    if key.endswith('lt'):
                        assert (result['result']['details']['observed_cdf']['x'][0] < value)
                    else:
                        assert (result['result']['details']['observed_cdf']['x'][0] == value)
                else:
                    raise ValueError(f"Invalid test specification: unknown key {key} in 'out'")
            elif (key == 'traceback_substring'):
                assert result['exception_info']['raised_exception']
                assert (value in result['exception_info']['exception_traceback']), ((('expected to find ' + value) + ' in ') + result['exception_info']['exception_traceback'])
            elif (key == 'expected_partition'):
                assert np.allclose(result['result']['details']['expected_partition']['bins'], value['bins'])
                assert np.allclose(result['result']['details']['expected_partition']['weights'], value['weights'])
                if ('tail_weights' in result['result']['details']['expected_partition']):
                    assert np.allclose(result['result']['details']['expected_partition']['tail_weights'], value['tail_weights'])
            elif (key == 'observed_partition'):
                assert np.allclose(result['result']['details']['observed_partition']['bins'], value['bins'])
                assert np.allclose(result['result']['details']['observed_partition']['weights'], value['weights'])
                if ('tail_weights' in result['result']['details']['observed_partition']):
                    assert np.allclose(result['result']['details']['observed_partition']['tail_weights'], value['tail_weights'])
            else:
                raise ValueError(f"Invalid test specification: unknown key {key} in 'out'")

def generate_test_table_name(default_table_name_prefix: str='test_data_') -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    table_name: str = (default_table_name_prefix + ''.join([random.choice((string.ascii_letters + string.digits)) for _ in range(8)]))
    return table_name

def _create_bigquery_engine() -> Engine:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    gcp_project = os.getenv('GE_TEST_GCP_PROJECT')
    if (not gcp_project):
        raise ValueError('Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery expectation tests')
    return create_engine(f'bigquery://{gcp_project}/{_bigquery_dataset()}')

def _bigquery_dataset() -> str:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    dataset = os.getenv('GE_TEST_BIGQUERY_DATASET')
    if (not dataset):
        raise ValueError('Environment Variable GE_TEST_BIGQUERY_DATASET is required to run BigQuery expectation tests')
    return dataset

def _create_trino_engine(hostname: str='localhost', schema_name: str='schema') -> Engine:
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    engine = create_engine(f'trino://test@{hostname}:8088/memory/{schema_name}')
    from sqlalchemy import text
    from trino.exceptions import TrinoUserError
    with engine.begin() as conn:
        try:
            res = conn.execute(text(f'create schema {schema_name}'))
        except TrinoUserError:
            pass
    return engine

def generate_sqlite_db_path():
    import inspect
    __frame = inspect.currentframe()
    __file = __frame.f_code.co_filename
    __func = __frame.f_code.co_name
    for (k, v) in __frame.f_locals.items():
        if any(((var in k) for var in ('self', 'cls', '__frame', '__file', '__func'))):
            continue
        print(f'<INTROSPECT> {__file}:{__func}:{k} - {v.__class__.__name__}')
    'Creates a temporary directory and absolute path to an ephemeral sqlite_db within that temp directory.\n\n    Used to support testing of multi-table expectations without creating temp directories at import.\n\n    Returns:\n        str: An absolute path to the ephemeral db within the created temporary directory.\n    '
    tmp_dir = str(tempfile.mkdtemp())
    abspath = os.path.abspath(os.path.join(tmp_dir, (('sqlite_db' + ''.join([random.choice((string.ascii_letters + string.digits)) for _ in range(8)])) + '.db')))
    return abspath
