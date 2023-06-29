from __future__ import annotations

import copy
import locale
import logging
import os
import platform
import random
import re
import string
import time
import traceback
import warnings
from decimal import Decimal
from functools import partial, wraps
from logging import Logger
from types import ModuleType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import numpy as np
import pandas as pd
from dateutil.parser import parse

import great_expectations.compatibility.bigquery as BigQueryDialect
from great_expectations.compatibility import aws, pyspark, snowflake, sqlalchemy, trino
from great_expectations.compatibility.pandas_compatibility import (
    execute_pandas_to_datetime,
)
from great_expectations.compatibility.sqlalchemy import (
    sqlalchemy as sa,
)
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core import (
    ExpectationConfigurationSchema,
    ExpectationSuite,
    ExpectationSuiteSchema,
    ExpectationSuiteValidationResultSchema,
    ExpectationValidationResultSchema,
    IDDict,
)
from great_expectations.core.batch import Batch, BatchDefinition, BatchRequest
from great_expectations.core.util import (
    get_or_create_spark_application,
    get_sql_dialect_floating_point_infinity_value,
)
from great_expectations.dataset import PandasDataset
from great_expectations.datasource import Datasource
from great_expectations.datasource.data_connector import ConfiguredAssetSqlDataConnector
from great_expectations.exceptions.exceptions import (
    ExecutionEngineError,
    InvalidExpectationConfigurationError,
    MetricProviderError,
    MetricResolutionError,
)
from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.profile import ColumnsExistProfiler
from great_expectations.self_check.sqlalchemy_connection_manager import (
    LockingConnectionCheck,
)
from great_expectations.util import (
    build_in_memory_runtime_context,
    import_library_module,
)
from great_expectations.validator.validator import Validator

SQLAlchemyError = sqlalchemy.SQLAlchemyError

if TYPE_CHECKING:
    from great_expectations.core.expectation_diagnostics.expectation_test_data_cases import (
        ExpectationTestCase,
        ExpectationTestDataCases,
    )
    from great_expectations.core.expectation_diagnostics.supporting_types import (
        ExpectationExecutionEngineDiagnostics,
    )
    from great_expectations.data_context import AbstractDataContext

expectationValidationResultSchema = ExpectationValidationResultSchema()
expectationSuiteValidationResultSchema = ExpectationSuiteValidationResultSchema()
expectationConfigurationSchema = ExpectationConfigurationSchema()
expectationSuiteSchema = ExpectationSuiteSchema()

# mysql and mssql allow table names to be a maximum of 128 characters
# for postgres it is 63.
MAX_TABLE_NAME_LENGTH: int = 63

logger = logging.getLogger(__name__)

if sqlalchemy.sqlite:
    SQLITE_TYPES = {
        "VARCHAR": sqlalchemy.sqlite.VARCHAR,
        "CHAR": sqlalchemy.sqlite.CHAR,
        "INTEGER": sqlalchemy.sqlite.INTEGER,
        "SMALLINT": sqlalchemy.sqlite.SMALLINT,
        "DATETIME": sqlalchemy.sqlite.DATETIME(truncate_microseconds=True),
        "DATE": sqlalchemy.sqlite.DATE,
        "FLOAT": sqlalchemy.sqlite.FLOAT,
        "BOOLEAN": sqlalchemy.sqlite.BOOLEAN,
        "TIMESTAMP": sqlalchemy.sqlite.TIMESTAMP,
    }
else:
    SQLITE_TYPES = {}


from great_expectations.compatibility.bigquery import (
    BIGQUERY_TYPES,
    GEOGRAPHY,
)

if GEOGRAPHY:
    BIGQUERY_TYPES["GEOGRAPHY"] = GEOGRAPHY

try:
    import sqlalchemy.dialects.postgresql as postgresqltypes  # noqa: TID251

    # noinspection PyPep8Naming
    from sqlalchemy.dialects.postgresql import dialect as pgDialect  # noqa: TID251

    POSTGRESQL_TYPES = {
        "TEXT": postgresqltypes.TEXT,
        "CHAR": postgresqltypes.CHAR,
        "INTEGER": postgresqltypes.INTEGER,
        "SMALLINT": postgresqltypes.SMALLINT,
        "BIGINT": postgresqltypes.BIGINT,
        "TIMESTAMP": postgresqltypes.TIMESTAMP,
        "DATE": postgresqltypes.DATE,
        "DOUBLE_PRECISION": postgresqltypes.DOUBLE_PRECISION,
        "BOOLEAN": postgresqltypes.BOOLEAN,
        "NUMERIC": postgresqltypes.NUMERIC,
    }
except (ImportError, KeyError):
    postgresqltypes = None
    pgDialect = None
    POSTGRESQL_TYPES = {}

try:
    import sqlalchemy.dialects.mysql as mysqltypes  # noqa: TID251

    # noinspection PyPep8Naming
    from sqlalchemy.dialects.mysql import dialect as mysqlDialect  # noqa: TID251

    MYSQL_TYPES = {
        "TEXT": mysqltypes.TEXT,
        "CHAR": mysqltypes.CHAR,
        "INTEGER": mysqltypes.INTEGER,
        "SMALLINT": mysqltypes.SMALLINT,
        "BIGINT": mysqltypes.BIGINT,
        "DATETIME": mysqltypes.DATETIME,
        "TIMESTAMP": mysqltypes.TIMESTAMP,
        "DATE": mysqltypes.DATE,
        "FLOAT": mysqltypes.FLOAT,
        "DOUBLE": mysqltypes.DOUBLE,
        "BOOLEAN": mysqltypes.BOOLEAN,
        "TINYINT": mysqltypes.TINYINT,
    }
except (ImportError, KeyError):
    mysqltypes = None
    mysqlDialect = None
    MYSQL_TYPES = {}

try:
    # SQLAlchemy does not export the "INT" type for the MS SQL Server dialect; however "INT" is supported by the engine.
    # Since SQLAlchemy exports the "INTEGER" type for the MS SQL Server dialect, alias "INT" to the "INTEGER" type.
    import sqlalchemy.dialects.mssql as mssqltypes  # noqa: TID251

    # noinspection PyPep8Naming
    from sqlalchemy.dialects.mssql import dialect as mssqlDialect  # noqa: TID251

    try:
        getattr(mssqltypes, "INT")
    except AttributeError:
        mssqltypes.INT = mssqltypes.INTEGER

    # noinspection PyUnresolvedReferences
    MSSQL_TYPES = {
        "BIGINT": mssqltypes.BIGINT,
        "BINARY": mssqltypes.BINARY,
        "BIT": mssqltypes.BIT,
        "CHAR": mssqltypes.CHAR,
        "DATE": mssqltypes.DATE,
        "DATETIME": mssqltypes.DATETIME,
        "DATETIME2": mssqltypes.DATETIME2,
        "DATETIMEOFFSET": mssqltypes.DATETIMEOFFSET,
        "DECIMAL": mssqltypes.DECIMAL,
        "FLOAT": mssqltypes.FLOAT,
        "IMAGE": mssqltypes.IMAGE,
        "INT": mssqltypes.INT,
        "INTEGER": mssqltypes.INTEGER,
        "MONEY": mssqltypes.MONEY,
        "NCHAR": mssqltypes.NCHAR,
        "NTEXT": mssqltypes.NTEXT,
        "NUMERIC": mssqltypes.NUMERIC,
        "NVARCHAR": mssqltypes.NVARCHAR,
        "REAL": mssqltypes.REAL,
        "SMALLDATETIME": mssqltypes.SMALLDATETIME,
        "SMALLINT": mssqltypes.SMALLINT,
        "SMALLMONEY": mssqltypes.SMALLMONEY,
        "SQL_VARIANT": mssqltypes.SQL_VARIANT,
        "TEXT": mssqltypes.TEXT,
        "TIME": mssqltypes.TIME,
        "TIMESTAMP": mssqltypes.TIMESTAMP,
        "TINYINT": mssqltypes.TINYINT,
        "UNIQUEIDENTIFIER": mssqltypes.UNIQUEIDENTIFIER,
        "VARBINARY": mssqltypes.VARBINARY,
        "VARCHAR": mssqltypes.VARCHAR,
    }
except (ImportError, KeyError):
    mssqltypes = None
    mssqlDialect = None
    MSSQL_TYPES = {}


try:
    import clickhouse_sqlalchemy.types.common as clickhousetypes
    from clickhouse_sqlalchemy.drivers.base import (
        ClickHouseDialect as clickhouseDialect,
    )

    CLICKHOUSE_TYPES = {
        "INT256": clickhousetypes.Int256,
        "INT128": clickhousetypes.Int128,
        "INT64": clickhousetypes.Int64,
        "INT32": clickhousetypes.Int32,
        "INT16": clickhousetypes.Int16,
        "INT8": clickhousetypes.Int8,
        "UINT256": clickhousetypes.UInt256,
        "UINT128": clickhousetypes.UInt128,
        "UINT64": clickhousetypes.UInt64,
        "UINT32": clickhousetypes.UInt32,
        "UINT16": clickhousetypes.UInt16,
        "UINT8": clickhousetypes.UInt8,
        "DATE": clickhousetypes.Date,
        "DATETIME": clickhousetypes.DateTime,
        "DATETIME64": clickhousetypes.DateTime64,
        "FLOAT64": clickhousetypes.Float64,
        "FLOAT32": clickhousetypes.Float32,
        "DECIMAL": clickhousetypes.Decimal,
        "STRING": clickhousetypes.String,
        "BOOL": clickhousetypes.Boolean,
        "BOOLEAN": clickhousetypes.Boolean,
        "UUID": clickhousetypes.UUID,
        "FIXEDSTRING": clickhousetypes.String,
        "ENUM8": clickhousetypes.Enum8,
        "ENUM16": clickhousetypes.Enum16,
        "ARRAY": clickhousetypes.Array,
        "NULLABLE": clickhousetypes.Nullable,
        "LOWCARDINALITY": clickhousetypes.LowCardinality,
        "TUPLE": clickhousetypes.Tuple,
        "MAP": clickhousetypes.Map,
    }
except (ImportError, KeyError):
    clickhouse = None
    clickhousetypes = None
    clickhouseDialect = None
    CLICKHOUSE_TYPES = {}


TRINO_TYPES: Dict[str, Any] = (
    {
        "BOOLEAN": trino.trinotypes._type_map["boolean"],
        "TINYINT": trino.trinotypes._type_map["tinyint"],
        "SMALLINT": trino.trinotypes._type_map["smallint"],
        "INT": trino.trinotypes._type_map["int"],
        "INTEGER": trino.trinotypes._type_map["integer"],
        "BIGINT": trino.trinotypes._type_map["bigint"],
        "REAL": trino.trinotypes._type_map["real"],
        "DOUBLE": trino.trinotypes._type_map["double"],
        "DECIMAL": trino.trinotypes._type_map["decimal"],
        "VARCHAR": trino.trinotypes._type_map["varchar"],
        "CHAR": trino.trinotypes._type_map["char"],
        "VARBINARY": trino.trinotypes._type_map["varbinary"],
        "JSON": trino.trinotypes._type_map["json"],
        "DATE": trino.trinotypes._type_map["date"],
        "TIME": trino.trinotypes._type_map["time"],
        "TIMESTAMP": trino.trinotypes._type_map["timestamp"],
    }
    if trino.trinotypes
    else {}
)

REDSHIFT_TYPES: Dict[str, Any] = (
    {
        "BIGINT": aws.redshiftdialect.BIGINT,
        "BOOLEAN": aws.redshiftdialect.BOOLEAN,
        "CHAR": aws.redshiftdialect.CHAR,
        "DATE": aws.redshiftdialect.DATE,
        "DECIMAL": aws.redshiftdialect.DECIMAL,
        "DOUBLE_PRECISION": aws.redshiftdialect.DOUBLE_PRECISION,
        "FOREIGN_KEY_RE": aws.redshiftdialect.FOREIGN_KEY_RE,
        "GEOMETRY": aws.redshiftdialect.GEOMETRY,
        "INTEGER": aws.redshiftdialect.INTEGER,
        "PRIMARY_KEY_RE": aws.redshiftdialect.PRIMARY_KEY_RE,
        "REAL": aws.redshiftdialect.REAL,
        "SMALLINT": aws.redshiftdialect.SMALLINT,
        "TIMESTAMP": aws.redshiftdialect.TIMESTAMP,
        "TIMESTAMPTZ": aws.redshiftdialect.TIMESTAMPTZ,
        "TIMETZ": aws.redshiftdialect.TIMETZ,
        "VARCHAR": aws.redshiftdialect.VARCHAR,
    }
    if aws.redshiftdialect
    else {}
)

SNOWFLAKE_TYPES: Dict[str, Any]
if (
    snowflake.snowflakesqlalchemy
    and snowflake.snowflakedialect
    and snowflake.snowflaketypes
):
    # Sometimes "snowflake-sqlalchemy" fails to self-register in certain environments, so we do it explicitly.
    # (see https://stackoverflow.com/questions/53284762/nosuchmoduleerror-cant-load-plugin-sqlalchemy-dialectssnowflake)
    sqlalchemy.dialects.registry.register(
        "snowflake", "snowflake.sqlalchemy", "dialect"
    )

    SNOWFLAKE_TYPES = {
        "ARRAY": snowflake.snowflaketypes.ARRAY,
        "BYTEINT": snowflake.snowflaketypes.BYTEINT,
        "CHARACTER": snowflake.snowflaketypes.CHARACTER,
        "DEC": snowflake.snowflaketypes.DEC,
        "BOOLEAN": snowflake.snowflakedialect.BOOLEAN,
        "DOUBLE": snowflake.snowflaketypes.DOUBLE,
        "FIXED": snowflake.snowflaketypes.FIXED,
        "NUMBER": snowflake.snowflaketypes.NUMBER,
        "INTEGER": snowflake.snowflakedialect.INTEGER,
        "OBJECT": snowflake.snowflaketypes.OBJECT,
        "STRING": snowflake.snowflaketypes.STRING,
        "TEXT": snowflake.snowflaketypes.TEXT,
        "TIMESTAMP_LTZ": snowflake.snowflaketypes.TIMESTAMP_LTZ,
        "TIMESTAMP_NTZ": snowflake.snowflaketypes.TIMESTAMP_NTZ,
        "TIMESTAMP_TZ": snowflake.snowflaketypes.TIMESTAMP_TZ,
        "TINYINT": snowflake.snowflaketypes.TINYINT,
        "VARBINARY": snowflake.snowflaketypes.VARBINARY,
        "VARIANT": snowflake.snowflaketypes.VARIANT,
    }
else:
    SNOWFLAKE_TYPES = {}

ATHENA_TYPES: Dict[str, Any] = (
    # athenatypes is just `from sqlalchemy import types`
    # https://github.com/laughingman7743/PyAthena/blob/master/pyathena/sqlalchemy_athena.py#L692
    #   - the _get_column_type method of AthenaDialect does some mapping via conditional statements
    # https://github.com/laughingman7743/PyAthena/blob/master/pyathena/sqlalchemy_athena.py#L105
    #   - The AthenaTypeCompiler has some methods named `visit_<TYPE>`
    {
        "BOOLEAN": aws.athenatypes.BOOLEAN,
        "FLOAT": aws.athenatypes.FLOAT,
        "DOUBLE": aws.athenatypes.FLOAT,
        "REAL": aws.athenatypes.FLOAT,
        "TINYINT": aws.athenatypes.INTEGER,
        "SMALLINT": aws.athenatypes.INTEGER,
        "INTEGER": aws.athenatypes.INTEGER,
        "INT": aws.athenatypes.INTEGER,
        "BIGINT": aws.athenatypes.BIGINT,
        "DECIMAL": aws.athenatypes.DECIMAL,
        "CHAR": aws.athenatypes.CHAR,
        "VARCHAR": aws.athenatypes.VARCHAR,
        "STRING": aws.athenatypes.String,
        "DATE": aws.athenatypes.DATE,
        "TIMESTAMP": aws.athenatypes.TIMESTAMP,
        "BINARY": aws.athenatypes.BINARY,
        "VARBINARY": aws.athenatypes.BINARY,
        "ARRAY": aws.athenatypes.String,
        "MAP": aws.athenatypes.String,
        "STRUCT": aws.athenatypes.String,
        "ROW": aws.athenatypes.String,
        "JSON": aws.athenatypes.String,
    }
    if aws.pyathena and aws.athenatypes
    else {}
)

# # Others from great_expectations/dataset/sqlalchemy_dataset.py
# try:
#     import sqlalchemy_dremio.pyodbc
#
#     sqlalchemy.dialects.registry.register(
#         "dremio", "sqlalchemy_dremio.pyodbc", "dialect"
#     )
# except ImportError:
#     sqlalchemy_dremio = None
#
# try:
#     import teradatasqlalchemy.dialect
#     import teradatasqlalchemy.types as teradatatypes
# except ImportError:
#     teradatasqlalchemy = None

try:
    from great_expectations.dataset import SparkDFDataset
except ImportError:
    SparkDFDataset = None  # type: ignore[misc,assignment] # could be None
    logger.debug(
        "Unable to load spark dataset; install optional spark dependency for support."
    )

import tempfile

# from tests.rule_based_profiler.conftest import ATOL, RTOL
RTOL: float = 1.0e-7
ATOL: float = 5.0e-2

RX_FLOAT = re.compile(r".*\d\.\d+.*")

SQL_DIALECT_NAMES = (
    "sqlite",
    "postgresql",
    "mysql",
    "mssql",
    "bigquery",
    "trino",
    "redshift",
    "clickhouse"
    # "athena",
    "snowflake",
)

BACKEND_TO_ENGINE_NAME_DICT = {
    "pandas": "pandas",
    "spark": "spark",
}

BACKEND_TO_ENGINE_NAME_DICT.update({name: "sqlalchemy" for name in SQL_DIALECT_NAMES})


def get_sqlite_connection_url(sqlite_db_path):
    url = "sqlite://"
    if sqlite_db_path is not None:
        extra_slash = ""
        if platform.system() != "Windows":
            extra_slash = "/"
        url = f"{url}/{extra_slash}{sqlite_db_path}"
    return url


def get_dataset(  # noqa: C901, PLR0912, PLR0913, PLR0915
    dataset_type,
    data,
    schemas=None,
    profiler=ColumnsExistProfiler,
    caching=True,
    table_name=None,
    sqlite_db_path=None,
):
    """Utility to create datasets for json-formatted tests"""
    df = pd.DataFrame(data)
    if dataset_type == "PandasDataset":
        if schemas and "pandas" in schemas:
            schema = schemas["pandas"]
            pandas_schema = {}
            for key, value in schema.items():
                # Note, these are just names used in our internal schemas to build datasets *for internal tests*
                # Further, some changes in pandas internal about how datetimes are created means to support pandas
                # pre- 0.25, we need to explicitly specify when we want timezone.

                # We will use timestamp for timezone-aware (UTC only) dates in our tests
                if value.lower() in ["timestamp", "datetime64[ns, tz]"]:
                    df[key] = pd.to_datetime(df[key], utc=True)
                    continue
                elif value.lower() in ["datetime", "datetime64", "datetime64[ns]"]:
                    df[key] = pd.to_datetime(df[key])
                    continue
                elif value.lower() in ["date"]:
                    df[key] = pd.to_datetime(df[key]).dt.date
                    value = "object"  # noqa: PLW2901
                try:
                    type_ = np.dtype(value)
                except TypeError:
                    # noinspection PyUnresolvedReferences
                    type_ = getattr(pd, value)()
                pandas_schema[key] = type_
            # pandas_schema = {key: np.dtype(value) for (key, value) in schemas["pandas"].items()}
            df = df.astype(pandas_schema)
        return PandasDataset(df, profiler=profiler, caching=caching)

    elif dataset_type == "SparkDFDataset":
        spark_types = {
            "StringType": pyspark.types.StringType,
            "IntegerType": pyspark.types.IntegerType,
            "LongType": pyspark.types.LongType,
            "DateType": pyspark.types.DateType,
            "TimestampType": pyspark.types.TimestampType,
            "FloatType": pyspark.types.FloatType,
            "DoubleType": pyspark.types.DoubleType,
            "BooleanType": pyspark.types.BooleanType,
            "DataType": pyspark.types.DataType,
            "NullType": pyspark.types.NullType,
        }
        spark = get_or_create_spark_application(
            spark_config={
                "spark.sql.catalogImplementation": "hive",
                "spark.executor.memory": "450m",
                # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
            }
        )
        # We need to allow null values in some column types that do not support them natively, so we skip
        # use of df in this case.
        data_reshaped = list(
            zip(*(v for _, v in data.items()))
        )  # create a list of rows
        if schemas and "spark" in schemas:
            schema = schemas["spark"]
            # sometimes first method causes Spark to throw a TypeError
            try:
                spark_schema = pyspark.types.StructType(
                    [
                        pyspark.types.StructField(
                            column, spark_types[schema[column]](), True
                        )
                        for column in schema
                    ]
                )
                # We create these every time, which is painful for testing
                # However nuance around null treatment as well as the desire
                # for real datetime support in tests makes this necessary
                data = copy.deepcopy(data)
                if "ts" in data:
                    print(data)
                    print(schema)
                for col in schema:
                    type_ = schema[col]
                    if type_ in ["IntegerType", "LongType"]:
                        # Ints cannot be None...but None can be valid in Spark (as Null)
                        vals = []
                        for val in data[col]:
                            if val is None:
                                vals.append(val)
                            else:
                                vals.append(int(val))
                        data[col] = vals
                    elif type_ in ["FloatType", "DoubleType"]:
                        vals = []
                        for val in data[col]:
                            if val is None:
                                vals.append(val)
                            else:
                                vals.append(float(val))
                        data[col] = vals
                    elif type_ in ["DateType", "TimestampType"]:
                        vals = []
                        for val in data[col]:
                            if val is None:
                                vals.append(val)
                            else:
                                vals.append(parse(val))
                        data[col] = vals
                # Do this again, now that we have done type conversion using the provided schema
                data_reshaped = list(
                    zip(*(v for _, v in data.items()))
                )  # create a list of rows
                spark_df = spark.createDataFrame(data_reshaped, schema=spark_schema)
            except TypeError:
                string_schema = pyspark.types.StructType(
                    [
                        pyspark.types.StructField(column, pyspark.types.StringType())
                        for column in schema
                    ]
                )
                spark_df = spark.createDataFrame(data_reshaped, string_schema)
                for c in spark_df.columns:
                    spark_df = spark_df.withColumn(
                        c, spark_df[c].cast(spark_types[schema[c]]())
                    )
        elif len(data_reshaped) == 0:
            # if we have an empty dataset and no schema, need to assign an arbitrary type
            columns = list(data.keys())
            spark_schema = pyspark.types.StructType(
                [
                    pyspark.types.StructField(column, pyspark.types.StringType())
                    for column in columns
                ]
            )
            spark_df = spark.createDataFrame(data_reshaped, spark_schema)
        else:
            # if no schema provided, uses Spark's schema inference
            columns = list(data.keys())
            spark_df = spark.createDataFrame(data_reshaped, columns)
        return SparkDFDataset(spark_df, profiler=profiler, caching=caching)
    else:
        warnings.warn(f"Unknown dataset_type {str(dataset_type)}")


def get_test_validator_with_data(  # noqa: PLR0913
    execution_engine: str,
    data: dict,
    table_name: str | None = None,
    schemas: dict | None = None,
    caching: bool = True,
    sqlite_db_path: str | None = None,
    extra_debug_info: str = "",
    debug_logger: logging.Logger | None = None,
    context: AbstractDataContext | None = None,
    pk_column: bool = False,
):
    """Utility to create datasets for json-formatted tests."""

    # if pk_column is defined in our test, then we add a index column to our test set
    if pk_column:
        first_column: List[Any] = list(data.values())[0]
        data["pk_index"] = list(range(len(first_column)))

    df = pd.DataFrame(data)
    if execution_engine == "pandas":
        return _get_test_validator_with_data_pandas(
            df=df,
            schemas=schemas,
            table_name=table_name,
            context=context,
            pk_column=pk_column,
        )
    elif execution_engine in SQL_DIALECT_NAMES:
        return _get_test_validator_with_data_sqlalchemy(
            df=df,
            execution_engine=execution_engine,
            schemas=schemas,
            caching=caching,
            table_name=table_name,
            sqlite_db_path=sqlite_db_path,
            extra_debug_info=extra_debug_info,
            debug_logger=debug_logger,
            context=context,
            pk_column=pk_column,
        )
    elif execution_engine == "spark":
        return _get_test_validator_with_data_spark(
            data=data,
            schemas=schemas,
            context=context,
            pk_column=pk_column,
        )
    else:
        raise ValueError(f"Unknown dataset_type {str(execution_engine)}")


def _get_test_validator_with_data_pandas(
    df: pd.DataFrame,
    schemas: dict | None,
    table_name: str | None,
    context: AbstractDataContext | None,
    pk_column: bool,
) -> Validator:
    if schemas and "pandas" in schemas:
        schema = schemas["pandas"]
        if pk_column:
            schema["pk_index"] = "int"
        pandas_schema = {}
        for key, value in schema.items():
            # Note, these are just names used in our internal schemas to build datasets *for internal tests*
            # Further, some changes in pandas internal about how datetimes are created means to support pandas
            # pre- 0.25, we need to explicitly specify when we want timezone.

            # We will use timestamp for timezone-aware (UTC only) dates in our tests
            if value.lower() in ["timestamp", "datetime64[ns, tz]"]:
                df[key] = execute_pandas_to_datetime(df[key], utc=True)
                continue
            elif value.lower() in ["datetime", "datetime64", "datetime64[ns]"]:
                df[key] = execute_pandas_to_datetime(df[key])
                continue
            elif value.lower() in ["date"]:
                df[key] = execute_pandas_to_datetime(df[key]).dt.date
                value = "object"  # noqa: PLW2901
            try:
                type_ = np.dtype(value)
            except TypeError:
                # noinspection PyUnresolvedReferences
                type_ = getattr(pd, value)()
            pandas_schema[key] = type_
        # pandas_schema = {key: np.dtype(value) for (key, value) in schemas["pandas"].items()}
        df = df.astype(pandas_schema)

    if table_name is None:
        # noinspection PyUnusedLocal
        table_name = generate_test_table_name()

    batch_definition = BatchDefinition(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_asset",
        batch_identifiers=IDDict({}),
        batch_spec_passthrough=None,
    )

    return build_pandas_validator_with_data(
        df=df, batch_definition=batch_definition, context=context
    )


def _get_test_validator_with_data_sqlalchemy(  # noqa: PLR0913
    df: pd.DataFrame,
    execution_engine: str,
    schemas: dict | None,
    caching: bool,
    table_name: str | None,
    sqlite_db_path: str | None,
    extra_debug_info: str,
    debug_logger: logging.Logger | None,
    context: AbstractDataContext | None,
    pk_column: bool,
) -> Validator | None:
    if not sa:
        return None

    if table_name is None:
        raise ExecutionEngineError(
            "Initializing a Validator for SqlAlchemyExecutionEngine in tests requires `table_name` to be defined. Please check your configuration"
        )
    return build_sa_validator_with_data(
        df=df,
        sa_engine_name=execution_engine,
        schemas=schemas,
        caching=caching,
        table_name=table_name,
        sqlite_db_path=sqlite_db_path,
        extra_debug_info=extra_debug_info,
        debug_logger=debug_logger,
        batch_definition=None,
        context=context,
        pk_column=pk_column,
    )


def _get_test_validator_with_data_spark(  # noqa: C901, PLR0912, PLR0915
    data: dict,
    schemas: dict | None,
    context: AbstractDataContext | None,
    pk_column: bool,
) -> Validator:
    spark_types: Dict[str, Callable] = {
        "StringType": pyspark.types.StringType,
        "IntegerType": pyspark.types.IntegerType,
        "LongType": pyspark.types.LongType,
        "DateType": pyspark.types.DateType,
        "TimestampType": pyspark.types.TimestampType,
        "FloatType": pyspark.types.FloatType,
        "DoubleType": pyspark.types.DoubleType,
        "BooleanType": pyspark.types.BooleanType,
        "DataType": pyspark.types.DataType,
        "NullType": pyspark.types.NullType,
        # When inferring schema from decimal.Decimal objects, pyspark uses DecimalType(38, 18).
        "DecimalType": partial(pyspark.types.DecimalType, 38, 18),
    }

    spark = get_or_create_spark_application(
        spark_config={
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "450m",
            # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
        }
    )
    # We need to allow null values in some column types that do not support them natively, so we skip
    # use of df in this case.
    data_reshaped = list(zip(*(v for _, v in data.items())))  # create a list of rows
    if schemas and "spark" in schemas:
        schema = schemas["spark"]
        if pk_column:
            schema["pk_index"] = "IntegerType"
        # sometimes first method causes Spark to throw a TypeError
        try:
            spark_schema = pyspark.types.StructType(
                [
                    pyspark.types.StructField(
                        column, spark_types[schema[column]](), True
                    )
                    for column in schema
                ]
            )
            # We create these every time, which is painful for testing
            # However nuance around null treatment as well as the desire
            # for real datetime support in tests makes this necessary
            data = copy.deepcopy(data)
            if "ts" in data:
                print(data)
                print(schema)
            for col in schema:
                type_ = schema[col]
                # Ints cannot be None...but None can be valid in Spark (as Null)
                vals: List[Union[str, int, float, None, Decimal]] = []
                if type_ in ["IntegerType", "LongType"]:
                    for val in data[col]:
                        if val is None:
                            vals.append(val)
                        else:
                            vals.append(int(val))
                    data[col] = vals
                elif type_ in ["FloatType", "DoubleType"]:
                    for val in data[col]:
                        if val is None:
                            vals.append(val)
                        else:
                            vals.append(float(val))
                    data[col] = vals
                elif type_ in ["DecimalType"]:
                    for val in data[col]:
                        if val is None:
                            vals.append(val)
                        else:
                            vals.append(Decimal(val))
                    data[col] = vals
                elif type_ in ["DateType", "TimestampType"]:
                    for val in data[col]:
                        if val is None:
                            vals.append(val)
                        else:
                            vals.append(parse(val))  # type: ignore[arg-type]
                    data[col] = vals
            # Do this again, now that we have done type conversion using the provided schema
            data_reshaped = list(
                zip(*(v for _, v in data.items()))
            )  # create a list of rows
            spark_df = spark.createDataFrame(data_reshaped, schema=spark_schema)
        except TypeError:
            string_schema = pyspark.types.StructType(
                [
                    pyspark.types.StructField(column, pyspark.types.StringType())
                    for column in schema
                ]
            )
            spark_df = spark.createDataFrame(data_reshaped, string_schema)
            for c in spark_df.columns:
                spark_df = spark_df.withColumn(
                    c, spark_df[c].cast(spark_types[schema[c]]())
                )
    elif len(data_reshaped) == 0:
        # if we have an empty dataset and no schema, need to assign an arbitrary type
        columns = list(data.keys())
        spark_schema = pyspark.types.StructType(
            [
                pyspark.types.StructField(column, pyspark.types.StringType())
                for column in columns
            ]
        )
        spark_df = spark.createDataFrame(data_reshaped, spark_schema)
    else:
        # if no schema provided, uses Spark's schema inference
        columns = list(data.keys())
        spark_df = spark.createDataFrame(data_reshaped, columns)

    batch_definition = BatchDefinition(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="my_asset",
        batch_identifiers=IDDict({}),
        batch_spec_passthrough=None,
    )
    return build_spark_validator_with_data(
        df=spark_df,
        spark=spark,
        batch_definition=batch_definition,
        context=context,
    )


def build_pandas_validator_with_data(
    df: pd.DataFrame,
    batch_definition: Optional[BatchDefinition] = None,
    context: Optional[AbstractDataContext] = None,
) -> Validator:
    batch = Batch(data=df, batch_definition=batch_definition)

    if context is None:
        context = build_in_memory_runtime_context(include_spark=False)

    return Validator(
        execution_engine=PandasExecutionEngine(),
        batches=[
            batch,
        ],
        data_context=context,
    )


def build_sa_validator_with_data(  # noqa: C901, PLR0912, PLR0913, PLR0915
    df,
    sa_engine_name,
    table_name,
    schemas=None,
    caching=True,
    sqlite_db_path=None,
    extra_debug_info="",
    batch_definition: Optional[BatchDefinition] = None,
    debug_logger: Optional[logging.Logger] = None,
    context: Optional[AbstractDataContext] = None,
    pk_column: bool = False,
):
    _debug = lambda x: x  # noqa: E731
    if debug_logger:
        _debug = lambda x: debug_logger.debug(f"(build_sa_validator_with_data) {x}")  # type: ignore[union-attr] # noqa: E731

    dialect_classes: Dict[str, Type] = {}
    dialect_types = {}
    try:
        dialect_classes["sqlite"] = sqlalchemy.sqlite.dialect
        dialect_types["sqlite"] = SQLITE_TYPES
    except AttributeError:
        pass

    try:
        dialect_classes["postgresql"] = postgresqltypes.dialect
        dialect_types["postgresql"] = POSTGRESQL_TYPES
    except AttributeError:
        pass

    try:
        dialect_classes["mysql"] = mysqltypes.dialect
        dialect_types["mysql"] = MYSQL_TYPES
    except AttributeError:
        pass

    try:
        dialect_classes["mssql"] = mssqltypes.dialect
        dialect_types["mssql"] = MSSQL_TYPES
    except AttributeError:
        pass

    try:
        dialect_classes["bigquery"] = BigQueryDialect  # type: ignore[assignment]
        dialect_types["bigquery"] = BIGQUERY_TYPES
    except AttributeError:
        pass

    try:
        dialect_classes["clickhouse"] = clickhouseDialect
        dialect_types["clickhouse"] = CLICKHOUSE_TYPES
    except AttributeError:
        pass

    if aws.redshiftdialect:
        dialect_classes["redshift"] = aws.redshiftdialect.RedshiftDialect
        dialect_types["redshift"] = REDSHIFT_TYPES

    if aws.sqlalchemy_athena:
        dialect_classes["athena"] = aws.sqlalchemy_athena.AthenaDialect
        dialect_types["athena"] = ATHENA_TYPES

    if snowflake.snowflakedialect:
        dialect_classes["snowflake"] = snowflake.snowflakedialect.dialect
        dialect_types["snowflake"] = SNOWFLAKE_TYPES

    if trino.trinodialect:
        dialect_classes["trino"] = trino.trinodialect.TrinoDialect
        dialect_types["trino"] = TRINO_TYPES

    if trino.trinodialect:
        dialect_classes["trino"] = trino.trinodialect.TrinoDialect
        dialect_types["trino"] = TRINO_TYPES

    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    if sa_engine_name == "sqlite":
        connection_string = get_sqlite_connection_url(sqlite_db_path)
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "postgresql":
        connection_string = f"postgresql://postgres@{db_hostname}/test_ci"
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "mysql":
        connection_string = f"mysql+pymysql://root@{db_hostname}/test_ci"
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "mssql":
        connection_string = f"mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true"
        engine = sa.create_engine(
            connection_string,
            # echo=True,
        )
    elif sa_engine_name == "bigquery":
        connection_string = _get_bigquery_connection_string()
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "clickhouse":
        connection_string = _get_clickhouse_connection_string()
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "trino":
        connection_string = _get_trino_connection_string()
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "redshift":
        connection_string = _get_redshift_connection_string()
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "athena":
        connection_string = _get_athena_connection_string()
        engine = sa.create_engine(connection_string)
    elif sa_engine_name == "snowflake":
        connection_string = _get_snowflake_connection_string()
        engine = sa.create_engine(connection_string)
    else:
        connection_string = None
        engine = None

    # If "autocommit" is not desired to be on by default, then use the following pattern when explicit "autocommit"
    # is desired (e.g., for temporary tables, "autocommit" is off by default, so the override option may be useful).
    # execution_engine.execute_query(sa.text(sql_query_string).execution_options(autocommit=True))

    # Add the data to the database as a new table

    if sa_engine_name == "bigquery":
        df.columns = df.columns.str.replace(" ", "_")

    sql_dtypes = {}
    # noinspection PyTypeHints
    if (
        schemas
        and sa_engine_name in schemas
        and isinstance(engine.dialect, dialect_classes[sa_engine_name])
    ):
        schema = schemas[sa_engine_name]
        if pk_column:
            schema["pk_index"] = "INTEGER"

        sql_dtypes = {
            col: dialect_types[sa_engine_name][dtype] for (col, dtype) in schema.items()
        }
        for col in schema:
            type_ = schema[col]
            if type_ in ["INTEGER", "SMALLINT", "BIGINT", "NUMBER"]:
                df[col] = pd.to_numeric(df[col], downcast="signed")
            elif type_ in ["FLOAT", "DOUBLE", "DOUBLE_PRECISION"]:
                df[col] = pd.to_numeric(df[col])
                min_value_dbms = get_sql_dialect_floating_point_infinity_value(
                    schema=sa_engine_name, negative=True
                )
                max_value_dbms = get_sql_dialect_floating_point_infinity_value(
                    schema=sa_engine_name, negative=False
                )
                for api_schema_type in ["api_np", "api_cast"]:
                    min_value_api = get_sql_dialect_floating_point_infinity_value(
                        schema=api_schema_type, negative=True
                    )
                    max_value_api = get_sql_dialect_floating_point_infinity_value(
                        schema=api_schema_type, negative=False
                    )
                    df.replace(
                        to_replace=[min_value_api, max_value_api],
                        value=[min_value_dbms, max_value_dbms],
                        inplace=True,
                    )
            elif type_ in [
                "DATETIME",
                "TIMESTAMP",
                "DATE",
                "TIMESTAMP_NTZ",  # the following 3 types are snowflake-specific
                "TIMESTAMP_LTZ",
                "TIMESTAMP_TZ",
            ]:
                df[col] = execute_pandas_to_datetime(df[col])
            elif type_ in ["VARCHAR", "STRING"]:
                df[col] = df[col].apply(str)

    if sa_engine_name in [
        "trino",
    ]:
        table_name = table_name.lower()
        sql_insert_method = "multi"
    else:
        sql_insert_method = None

    execution_engine = SqlAlchemyExecutionEngine(caching=caching, engine=engine)
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine, table_name=table_name
    )
    with execution_engine.get_connection() as connection:
        _debug("Calling df.to_sql")
        _start = time.time()
        add_dataframe_to_db(
            df=df,
            name=table_name,
            con=connection,
            index=False,
            dtype=sql_dtypes,
            if_exists="replace",
            method=sql_insert_method,
        )
        _end = time.time()
        _debug(
            f"Took {_end - _start} seconds to df.to_sql for {sa_engine_name} {extra_debug_info}"
        )

    if context is None:
        context = build_in_memory_runtime_context()

    assert (
        context is not None
    ), 'Instance of any child of "AbstractDataContext" class is required.'

    context.datasources["my_test_datasource"] = Datasource(
        name="my_test_datasource",
        # Configuration for "execution_engine" here is largely placeholder to comply with "Datasource" constructor.
        execution_engine={
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": connection_string,
        },
        data_connectors={
            "my_sql_data_connector": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "my_asset": {
                        "table_name": "animal_names",
                    },
                },
            },
        },
    )
    # Updating "execution_engine" to insure peculiarities, incorporated herein, propagate to "ExecutionEngine" itself.
    context.datasources["my_test_datasource"]._execution_engine = execution_engine  # type: ignore[union-attr]
    my_data_connector: ConfiguredAssetSqlDataConnector = (
        ConfiguredAssetSqlDataConnector(
            name="my_sql_data_connector",
            datasource_name="my_test_datasource",
            execution_engine=execution_engine,
            assets={
                "my_asset": {
                    "table_name": "animals_table",
                },
            },
        )
    )

    if batch_definition is None:
        batch_definition = (
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="my_test_datasource",
                    data_connector_name="my_sql_data_connector",
                    data_asset_name="my_asset",
                )
            )
        )[0]

    batch = Batch(data=batch_data, batch_definition=batch_definition)  # type: ignore[arg-type] # got SqlAlchemyBatchData

    return Validator(
        execution_engine=execution_engine,
        data_context=context,
        batches=[
            batch,
        ],
    )


def modify_locale(func):
    @wraps(func)
    def locale_wrapper(*args, **kwargs) -> None:
        old_locale = locale.setlocale(locale.LC_TIME, None)
        print(old_locale)
        # old_locale = locale.getlocale(locale.LC_TIME) Why not getlocale? not sure
        try:
            new_locale = locale.setlocale(locale.LC_TIME, "en_US.UTF-8")
            assert new_locale == "en_US.UTF-8"
            func(*args, **kwargs)
        except Exception:
            raise
        finally:
            locale.setlocale(locale.LC_TIME, old_locale)

    return locale_wrapper


def build_spark_validator_with_data(
    df: Union[pd.DataFrame, pyspark.DataFrame],
    spark: pyspark.SparkSession,
    batch_definition: Optional[BatchDefinition] = None,
    context: Optional[AbstractDataContext] = None,
) -> Validator:
    if isinstance(df, pd.DataFrame):
        df = spark.createDataFrame(
            [
                tuple(
                    None if isinstance(x, (float, int)) and np.isnan(x) else x
                    for x in record.tolist()
                )
                for record in df.to_records(index=False)
            ],
            df.columns.tolist(),
        )

    batch = Batch(data=df, batch_definition=batch_definition)  # type: ignore[arg-type] # got DataFrame
    execution_engine: SparkDFExecutionEngine = build_spark_engine(
        spark=spark,
        df=df,
        batch_id=batch.id,
    )

    if context is None:
        context = build_in_memory_runtime_context(include_pandas=False)

    return Validator(
        execution_engine=execution_engine,
        batches=[
            batch,
        ],
        data_context=context,
    )


def build_pandas_engine(
    df: pd.DataFrame,
) -> PandasExecutionEngine:
    batch = Batch(data=df)
    execution_engine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    return execution_engine


def build_sa_execution_engine(  # noqa: PLR0913
    df: pd.DataFrame,
    sa: ModuleType,
    schema: Optional[str] = None,
    batch_id: Optional[str] = None,
    if_exists: str = "replace",
    index: bool = False,
    dtype: Optional[dict] = None,
) -> SqlAlchemyExecutionEngine:
    table_name: str = "test"

    # noinspection PyUnresolvedReferences
    sqlalchemy_engine: sqlalchemy.Engine = sa.create_engine("sqlite://", echo=False)
    add_dataframe_to_db(
        df=df,
        name=table_name,
        con=sqlalchemy_engine,
        schema=schema,
        if_exists=if_exists,
        index=index,
        dtype=dtype,
    )

    execution_engine: SqlAlchemyExecutionEngine = SqlAlchemyExecutionEngine(
        engine=sqlalchemy_engine
    )
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine, table_name=table_name
    )
    batch = Batch(data=batch_data)  # type: ignore[arg-type] # got SqlAlchemyBatchData

    if batch_id is None:
        batch_id = batch.id

    execution_engine = SqlAlchemyExecutionEngine(
        engine=sqlalchemy_engine, batch_data_dict={batch_id: batch_data}
    )

    return execution_engine


# Builds a Spark Execution Engine
def build_spark_engine(
    spark: pyspark.SparkSession,
    df: Union[pd.DataFrame, pyspark.DataFrame],
    schema: Optional[pyspark.types.StructType] = None,
    batch_id: Optional[str] = None,
    batch_definition: Optional[BatchDefinition] = None,
) -> SparkDFExecutionEngine:
    if (
        sum(
            bool(x)
            for x in [
                batch_id is not None,
                batch_definition is not None,
            ]
        )
        != 1
    ):
        raise ValueError(
            "Exactly one of batch_id or batch_definition must be specified."
        )

    if batch_id is None:
        batch_id = cast(BatchDefinition, batch_definition).id

    if isinstance(df, pd.DataFrame):
        if schema is None:
            data: Union[pd.DataFrame, List[tuple]] = [
                tuple(
                    None if isinstance(x, (float, int)) and np.isnan(x) else x
                    for x in record.tolist()
                )
                for record in df.to_records(index=False)
            ]
            schema = df.columns.tolist()
        else:
            data = df

        df = spark.createDataFrame(data=data, schema=schema)

    conf: Iterable[Tuple[str, str]] = spark.sparkContext.getConf().getAll()
    spark_config: Dict[str, Any] = dict(conf)
    execution_engine = SparkDFExecutionEngine(
        spark_config=spark_config,
        batch_data_dict={
            batch_id: df,
        },
        force_reuse_spark_context=True,
    )
    return execution_engine


def candidate_getter_is_on_temporary_notimplemented_list(context, getter):
    if context in ["sqlite"]:
        return getter in ["get_column_modes", "get_column_stdev"]
    if context in ["postgresql", "mysql", "mssql"]:
        return getter in ["get_column_modes"]
    if context == "spark":
        return getter in []


def candidate_test_is_on_temporary_notimplemented_list_v2_api(
    context, expectation_type
):
    if context in SQL_DIALECT_NAMES:
        expectations_not_implemented_v2_sql = [
            "expect_column_values_to_be_increasing",
            "expect_column_values_to_be_decreasing",
            "expect_column_values_to_match_strftime_format",
            "expect_column_values_to_be_dateutil_parseable",
            "expect_column_values_to_be_json_parseable",
            "expect_column_values_to_match_json_schema",
            "expect_column_stdev_to_be_between",
            "expect_column_most_common_value_to_be_in_set",
            "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
            "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
            "expect_column_pair_values_to_be_equal",
            "expect_column_pair_values_A_to_be_greater_than_B",
            "expect_select_column_values_to_be_unique_within_record",
            "expect_compound_columns_to_be_unique",
            "expect_multicolumn_values_to_be_unique",
            "expect_column_pair_cramers_phi_value_to_be_less_than",
            "expect_multicolumn_sum_to_equal",
            "expect_column_value_z_scores_to_be_less_than",
        ]
        if context in ["bigquery"]:
            ###
            # NOTE: 202201 - Will: Expectations below are temporarily not being tested
            # with BigQuery in V2 API
            ###
            expectations_not_implemented_v2_sql.append(
                "expect_column_kl_divergence_to_be_less_than"
            )  # TODO: unique to bigquery  -- https://github.com/great-expectations/great_expectations/issues/3261
            expectations_not_implemented_v2_sql.append(
                "expect_column_chisquare_test_p_value_to_be_greater_than"
            )  # TODO: unique to bigquery  -- https://github.com/great-expectations/great_expectations/issues/3261
            expectations_not_implemented_v2_sql.append(
                "expect_column_values_to_be_between"
            )  # TODO: error unique to bigquery -- https://github.com/great-expectations/great_expectations/issues/3261
            expectations_not_implemented_v2_sql.append(
                "expect_column_values_to_be_in_set"
            )  # TODO: error unique to bigquery -- https://github.com/great-expectations/great_expectations/issues/3261
            expectations_not_implemented_v2_sql.append(
                "expect_column_values_to_be_in_type_list"
            )  # TODO: error unique to bigquery -- https://github.com/great-expectations/great_expectations/issues/3261
            expectations_not_implemented_v2_sql.append(
                "expect_column_values_to_be_of_type"
            )  # TODO: error unique to bigquery -- https://github.com/great-expectations/great_expectations/issues/3261
            expectations_not_implemented_v2_sql.append(
                "expect_column_values_to_match_like_pattern_list"
            )  # TODO: error unique to bigquery -- https://github.com/great-expectations/great_expectations/issues/3261
            expectations_not_implemented_v2_sql.append(
                "expect_column_values_to_not_match_like_pattern_list"
            )  # TODO: error unique to bigquery -- https://github.com/great-expectations/great_expectations/issues/3261
        return expectation_type in expectations_not_implemented_v2_sql

    if context == "SparkDFDataset":
        return expectation_type in [
            "expect_column_values_to_be_dateutil_parseable",
            "expect_column_values_to_be_json_parseable",
            "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
            "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
            "expect_compound_columns_to_be_unique",
            "expect_column_pair_cramers_phi_value_to_be_less_than",
            "expect_table_row_count_to_equal_other_table",
            "expect_column_value_z_scores_to_be_less_than",
        ]
    if context == "PandasDataset":
        return expectation_type in [
            "expect_table_row_count_to_equal_other_table",
            "expect_column_value_z_scores_to_be_less_than",
        ]
    return False


def candidate_test_is_on_temporary_notimplemented_list_v3_api(
    context, expectation_type
):
    candidate_test_is_on_temporary_notimplemented_list_v3_api_trino = [
        "expect_column_distinct_values_to_contain_set",
        "expect_column_max_to_be_between",
        "expect_column_mean_to_be_between",
        "expect_column_median_to_be_between",
        "expect_column_min_to_be_between",
        "expect_column_most_common_value_to_be_in_set",
        "expect_column_quantile_values_to_be_between",
        "expect_column_sum_to_be_between",
        "expect_column_kl_divergence_to_be_less_than",
        "expect_column_value_lengths_to_be_between",
        "expect_column_values_to_be_between",
        "expect_column_values_to_be_in_set",
        "expect_column_values_to_be_in_type_list",
        "expect_column_values_to_be_null",
        "expect_column_values_to_be_of_type",
        "expect_column_values_to_be_unique",
        "expect_column_values_to_match_like_pattern",
        "expect_column_values_to_match_like_pattern_list",
        "expect_column_values_to_match_regex",
        "expect_column_values_to_match_regex_list",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_not_match_like_pattern",
        "expect_column_values_to_not_match_like_pattern_list",
        "expect_column_values_to_not_match_regex",
        "expect_column_values_to_not_match_regex_list",
        "expect_column_pair_values_A_to_be_greater_than_B",
        "expect_column_pair_values_to_be_equal",
        "expect_column_pair_values_to_be_in_set",
        "expect_compound_columns_to_be_unique",
        "expect_select_column_values_to_be_unique_within_record",
        "expect_table_column_count_to_be_between",
        "expect_table_column_count_to_equal",
        "expect_table_row_count_to_be_between",
        "expect_table_row_count_to_equal",
    ]
    candidate_test_is_on_temporary_notimplemented_list_v3_api_other_sql = [
        "expect_column_values_to_be_increasing",
        "expect_column_values_to_be_decreasing",
        "expect_column_values_to_match_strftime_format",
        "expect_column_values_to_be_dateutil_parseable",
        "expect_column_values_to_be_json_parseable",
        "expect_column_values_to_match_json_schema",
        "expect_column_stdev_to_be_between",
        # "expect_column_unique_value_count_to_be_between",
        # "expect_column_proportion_of_unique_values_to_be_between",
        # "expect_column_most_common_value_to_be_in_set",
        # "expect_column_max_to_be_between",
        # "expect_column_min_to_be_between",
        # "expect_column_sum_to_be_between",
        # "expect_column_pair_values_A_to_be_greater_than_B",
        # "expect_column_pair_values_to_be_equal",
        # "expect_column_pair_values_to_be_in_set",
        # "expect_multicolumn_sum_to_equal",
        # "expect_compound_columns_to_be_unique",
        "expect_multicolumn_values_to_be_unique",
        # "expect_select_column_values_to_be_unique_within_record",
        "expect_column_pair_cramers_phi_value_to_be_less_than",
        "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
        "expect_column_chisquare_test_p_value_to_be_greater_than",
        "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
    ]
    if context in ["trino"]:
        return expectation_type in set(
            candidate_test_is_on_temporary_notimplemented_list_v3_api_trino
        ).union(
            set(candidate_test_is_on_temporary_notimplemented_list_v3_api_other_sql)
        )
    if context in SQL_DIALECT_NAMES:
        expectations_not_implemented_v3_sql = [
            "expect_column_values_to_be_increasing",
            "expect_column_values_to_be_decreasing",
            "expect_column_values_to_match_strftime_format",
            "expect_column_values_to_be_dateutil_parseable",
            "expect_column_values_to_be_json_parseable",
            "expect_column_values_to_match_json_schema",
            "expect_multicolumn_values_to_be_unique",
            "expect_column_pair_cramers_phi_value_to_be_less_than",
            "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
            "expect_column_chisquare_test_p_value_to_be_greater_than",
            "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
        ]
        if context in ["bigquery"]:
            ###
            # NOTE: 20210729 - jdimatteo: Below are temporarily not being tested
            # with BigQuery. For each disabled test below, please include a link to
            # a github issue tracking adding the test with BigQuery.
            ###
            expectations_not_implemented_v3_sql.append(
                "expect_column_kl_divergence_to_be_less_than"  # TODO: will collect for over 60 minutes, and will not completes
            )
            expectations_not_implemented_v3_sql.append(
                "expect_column_quantile_values_to_be_between"  # TODO: will run but will add about 1hr to pipeline.
            )
        return expectation_type in expectations_not_implemented_v3_sql

    if context == "spark":
        return expectation_type in [
            "expect_table_row_count_to_equal_other_table",
            "expect_column_values_to_be_in_set",
            "expect_column_values_to_not_be_in_set",
            "expect_column_values_to_not_match_regex_list",
            "expect_column_values_to_match_like_pattern",
            "expect_column_values_to_not_match_like_pattern",
            "expect_column_values_to_match_like_pattern_list",
            "expect_column_values_to_not_match_like_pattern_list",
            "expect_column_values_to_be_dateutil_parseable",
            "expect_multicolumn_values_to_be_unique",
            "expect_column_pair_cramers_phi_value_to_be_less_than",
            "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
            "expect_column_chisquare_test_p_value_to_be_greater_than",
            "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
        ]
    if context == "pandas":
        return expectation_type in [
            "expect_table_row_count_to_equal_other_table",
            "expect_column_values_to_match_like_pattern",
            "expect_column_values_to_not_match_like_pattern",
            "expect_column_values_to_match_like_pattern_list",
            "expect_column_values_to_not_match_like_pattern_list",
            "expect_multicolumn_values_to_be_unique",
            "expect_column_pair_cramers_phi_value_to_be_less_than",
            "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than",
            "expect_column_chisquare_test_p_value_to_be_greater_than",
            "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than",
        ]

    return False


def build_test_backends_list(  # noqa: C901, PLR0912, PLR0913, PLR0915
    include_pandas=True,
    include_spark=False,
    include_sqlalchemy=True,
    include_sqlite=True,
    include_postgresql=False,
    include_mysql=False,
    include_mssql=False,
    include_bigquery=False,
    include_aws=False,
    include_clickhouse=False,
    include_trino=False,
    include_azure=False,
    include_redshift=False,
    include_athena=False,
    include_snowflake=False,
    raise_exceptions_for_backends: bool = True,
) -> List[str]:
    """Attempts to identify supported backends by checking which imports are available."""

    test_backends = []

    if include_pandas:
        test_backends += ["pandas"]

    if include_spark:
        from great_expectations.compatibility import pyspark

        if not pyspark.SparkSession:  # type: ignore[truthy-function]
            if raise_exceptions_for_backends is True:
                raise ValueError(
                    "spark tests are requested, but pyspark is not installed"
                )
            else:
                logger.warning(
                    "spark tests are requested, but pyspark is not installed"
                )
        else:
            test_backends += ["spark"]

    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    if include_sqlalchemy:
        sa: Optional[ModuleType] = import_library_module(module_name="sqlalchemy")
        if sa is None:
            if raise_exceptions_for_backends is True:
                raise ImportError(
                    "sqlalchemy tests are requested, but sqlalchemy in not installed"
                )
            else:
                logger.warning(
                    "sqlalchemy tests are requested, but sqlalchemy in not installed"
                )
            return test_backends

        if include_sqlite:
            test_backends += ["sqlite"]

        if include_postgresql:
            ###
            # NOTE: 20190918 - JPC: Since I've had to relearn this a few times, a note here.
            # SQLALCHEMY coerces postgres DOUBLE_PRECISION to float, which loses precision
            # round trip compared to NUMERIC, which stays as a python DECIMAL

            # Be sure to ensure that tests (and users!) understand that subtlety,
            # which can be important for distributional expectations, for example.
            ###
            connection_string = f"postgresql://postgres@{db_hostname}/test_ci"
            checker = LockingConnectionCheck(sa, connection_string)
            if checker.is_valid() is True:
                test_backends += ["postgresql"]
            else:
                if raise_exceptions_for_backends is True:  # noqa: PLR5501
                    raise ValueError(
                        f"backend-specific tests are requested, but unable to connect to the database at "
                        f"{connection_string}"
                    )
                else:
                    logger.warning(
                        f"backend-specific tests are requested, but unable to connect to the database at "
                        f"{connection_string}"
                    )

        if include_mysql:
            try:
                engine = sa.create_engine(f"mysql+pymysql://root@{db_hostname}/test_ci")
                conn = engine.connect()
                conn.close()
            except (ImportError, SQLAlchemyError):
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "mysql tests are requested, but unable to connect to the mysql database at "
                        f"'mysql+pymysql://root@{db_hostname}/test_ci'"
                    )
                else:
                    logger.warning(
                        "mysql tests are requested, but unable to connect to the mysql database at "
                        f"'mysql+pymysql://root@{db_hostname}/test_ci'"
                    )
            else:
                test_backends += ["mysql"]

        if include_mssql:
            # noinspection PyUnresolvedReferences
            try:
                engine = sa.create_engine(
                    f"mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?"
                    "driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true",
                    # echo=True,
                )
                conn = engine.connect()
                conn.close()
            except (ImportError, sa.exc.SQLAlchemyError):
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "mssql tests are requested, but unable to connect to the mssql database at "
                        f"'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?"
                        "driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true'",
                    )
                else:
                    logger.warning(
                        "mssql tests are requested, but unable to connect to the mssql database at "
                        f"'mssql+pyodbc://sa:ReallyStrongPwd1234%^&*@{db_hostname}:1433/test_ci?"
                        "driver=ODBC Driver 17 for SQL Server&charset=utf8&autocommit=true'",
                    )
            else:
                test_backends += ["mssql"]

        if include_bigquery:
            # noinspection PyUnresolvedReferences
            try:
                engine = _create_bigquery_engine()
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "bigquery tests are requested, but unable to connect"
                    ) from e
                else:
                    logger.warning(
                        f"bigquery tests are requested, but unable to connect; {repr(e)}"
                    )
            else:
                test_backends += ["bigquery"]

        if include_redshift or include_athena:
            include_aws = True

        if include_aws:
            # TODO need to come up with a better way to do this check.
            # currently this checks the 3 default ENV variables that boto3 looks for
            aws_access_key_id: Optional[str] = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_access_key: Optional[str] = os.getenv("AWS_SECRET_ACCESS_KEY")
            aws_session_token: Optional[str] = os.getenv("AWS_SESSION_TOKEN")
            aws_config_file: Optional[str] = os.getenv("AWS_CONFIG_FILE")
            if (
                not aws_access_key_id
                and not aws_secret_access_key
                and not aws_session_token
                and not aws_config_file
            ):
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "AWS tests are requested, but credentials were not set up"
                    )
                else:
                    logger.warning(
                        "AWS tests are requested, but credentials were not set up"
                    )

        if include_clickhouse:
            # noinspection PyUnresolvedReferences
            try:
                engine = _create_clickhouse_engine(db_hostname)
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "clickhouse tests are requested, but unable to connect"
                    ) from e
                else:
                    logger.warning(
                        f"clickhouse tests are requested, but unable to connect; {repr(e)}"
                    )
            else:
                test_backends += ["clickhouse"]

        if include_trino:
            # noinspection PyUnresolvedReferences
            try:
                engine = _create_trino_engine(db_hostname)
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "trino tests are requested, but unable to connect"
                    ) from e
                else:
                    logger.warning(
                        f"trino tests are requested, but unable to connect; {repr(e)}"
                    )
            else:
                test_backends += ["trino"]

        if include_azure:
            azure_credential: Optional[str] = os.getenv("AZURE_CREDENTIAL")
            azure_access_key: Optional[str] = os.getenv("AZURE_ACCESS_KEY")
            if not azure_access_key and not azure_credential:
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "Azure tests are requested, but credentials were not set up"
                    )
                else:
                    logger.warning(
                        "Azure tests are requested, but credentials were not set up"
                    )
            test_backends += ["azure"]

        if include_redshift:
            # noinspection PyUnresolvedReferences
            try:
                engine = _create_redshift_engine()
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "redshift tests are requested, but unable to connect"
                    ) from e
                else:
                    logger.warning(
                        f"redshift tests are requested, but unable to connect; {repr(e)}"
                    )
            else:
                test_backends += ["redshift"]

        if include_athena:
            # noinspection PyUnresolvedReferences
            try:
                engine = _create_athena_engine()
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "athena tests are requested, but unable to connect"
                    ) from e
                else:
                    logger.warning(
                        f"athena tests are requested, but unable to connect; {repr(e)}"
                    )
            else:
                test_backends += ["athena"]

        if include_snowflake:
            # noinspection PyUnresolvedReferences
            try:
                engine = _create_snowflake_engine()
                conn = engine.connect()
                conn.close()
            except (ImportError, ValueError, sa.exc.SQLAlchemyError) as e:
                if raise_exceptions_for_backends is True:
                    raise ImportError(
                        "snowflake tests are requested, but unable to connect"
                    ) from e
                else:
                    logger.warning(
                        f"snowflake tests are requested, but unable to connect; {repr(e)}"
                    )
            else:
                test_backends += ["snowflake"]

    return test_backends


def generate_expectation_tests(  # noqa: C901, PLR0912, PLR0913, PLR0915
    expectation_type: str,
    test_data_cases: List[ExpectationTestDataCases],
    execution_engine_diagnostics: ExpectationExecutionEngineDiagnostics,
    raise_exceptions_for_backends: bool = False,
    ignore_suppress: bool = False,
    ignore_only_for: bool = False,
    debug_logger: Optional[logging.Logger] = None,
    only_consider_these_backends: Optional[List[str]] = None,
    context: Optional[AbstractDataContext] = None,
):
    """Determine tests to run

    :param expectation_type: snake_case name of the expectation type
    :param test_data_cases: list of ExpectationTestDataCases that has data, tests, schemas, and backends to use
    :param execution_engine_diagnostics: ExpectationExecutionEngineDiagnostics object specifying the engines the expectation is implemented for
    :param raise_exceptions_for_backends: bool object that when True will raise an Exception if a backend fails to connect
    :param ignore_suppress: bool object that when True will ignore the suppress_test_for list on Expectation sample tests
    :param ignore_only_for: bool object that when True will ignore the only_for list on Expectation sample tests
    :param debug_logger: optional logging.Logger object to use for sending debug messages to
    :param only_consider_these_backends: optional list of backends to consider
    :param context Instance of any child of "AbstractDataContext" class
    :return: list of parametrized tests with loaded validators and accessible backends
    """
    _debug = lambda x: x  # noqa: E731
    _error = lambda x: x  # noqa: E731
    if debug_logger:
        _debug = lambda x: debug_logger.debug(f"(generate_expectation_tests) {x}")  # type: ignore[union-attr]  # noqa: E731
        _error = lambda x: debug_logger.error(f"(generate_expectation_tests) {x}")  # type: ignore[union-attr]  # noqa: E731

    dialects_to_include = {}
    engines_to_include = {}
    engines_implemented = []

    if execution_engine_diagnostics.PandasExecutionEngine:
        engines_implemented.append("pandas")
    if execution_engine_diagnostics.SparkDFExecutionEngine:
        engines_implemented.append("spark")
    if execution_engine_diagnostics.SqlAlchemyExecutionEngine:
        engines_implemented.append("sqlalchemy")
    _debug(
        f"Implemented engines for {expectation_type}: {', '.join(engines_implemented)}"
    )

    if only_consider_these_backends:
        _debug(f"only_consider_these_backends -> {only_consider_these_backends}")
        for backend in only_consider_these_backends:
            if backend in BACKEND_TO_ENGINE_NAME_DICT:
                _engine = BACKEND_TO_ENGINE_NAME_DICT[backend]
                if _engine == "sqlalchemy" and "sqlalchemy" in engines_implemented:
                    engines_to_include[_engine] = True
                    dialects_to_include[backend] = True
                elif _engine == "pandas" and "pandas" in engines_implemented:
                    engines_to_include[_engine] = True
                elif _engine == "spark" and "spark" in engines_implemented:
                    engines_to_include[_engine] = True
    else:
        engines_to_include[
            "pandas"
        ] = execution_engine_diagnostics.PandasExecutionEngine
        engines_to_include[
            "spark"
        ] = execution_engine_diagnostics.SparkDFExecutionEngine
        engines_to_include[
            "sqlalchemy"
        ] = execution_engine_diagnostics.SqlAlchemyExecutionEngine
        if (
            engines_to_include.get("sqlalchemy") is True
            and raise_exceptions_for_backends is False
        ):
            dialects_to_include = {dialect: True for dialect in SQL_DIALECT_NAMES}

    _debug(
        f"Attempting engines ({engines_to_include}) and dialects ({dialects_to_include})"
    )

    backends = build_test_backends_list(
        include_pandas=engines_to_include.get("pandas", False),
        include_spark=engines_to_include.get("spark", False),
        include_sqlalchemy=engines_to_include.get("sqlalchemy", False),
        include_sqlite=dialects_to_include.get("sqlite", False),
        include_postgresql=dialects_to_include.get("postgresql", False),
        include_mysql=dialects_to_include.get("mysql", False),
        include_mssql=dialects_to_include.get("mssql", False),
        include_bigquery=dialects_to_include.get("bigquery", False),
        include_clickhouse=dialects_to_include.get("clickhouse", False),
        include_trino=dialects_to_include.get("trino", False),
        include_redshift=dialects_to_include.get("redshift", False),
        include_athena=dialects_to_include.get("athena", False),
        include_snowflake=dialects_to_include.get("snowflake", False),
        raise_exceptions_for_backends=raise_exceptions_for_backends,
    )

    _debug(f"Successfully connecting backends -> {backends}")

    if not backends:
        _debug("No suitable backends to connect to")
        return []

    parametrized_tests = []
    num_test_data_cases = len(test_data_cases)
    for i, d in enumerate(test_data_cases, 1):
        _debug(f"test_data_case {i}/{num_test_data_cases}")
        d = copy.deepcopy(d)  # noqa: PLW2901
        titles = []
        only_fors = []
        suppress_test_fors = []
        for _test_case in d.tests:
            titles.append(_test_case.title)
            only_fors.append(_test_case.only_for)
            suppress_test_fors.append(_test_case.suppress_test_for)
        _debug(f"titles -> {titles}")
        _debug(
            f"only_fors -> {only_fors}  suppress_test_fors -> {suppress_test_fors}  only_consider_these_backends -> {only_consider_these_backends}"
        )
        for c in backends:
            _debug(f"Getting validators with data: {c}")

            tests_suppressed_for_backend = [
                c in sup or ("sqlalchemy" in sup and c in SQL_DIALECT_NAMES)
                if sup
                else False
                for sup in suppress_test_fors
            ]
            only_fors_ok = []
            for i, only_for in enumerate(only_fors):  # noqa: PLW2901
                if not only_for:
                    only_fors_ok.append(True)
                    continue
                if c in only_for or (
                    "sqlalchemy" in only_for and c in SQL_DIALECT_NAMES
                ):
                    only_fors_ok.append(True)
                else:
                    only_fors_ok.append(False)
            if tests_suppressed_for_backend and all(tests_suppressed_for_backend):
                _debug(
                    f"All {len(tests_suppressed_for_backend)} tests are SUPPRESSED for {c}"
                )
                continue
            if not any(only_fors_ok):
                _debug(f"No tests are allowed for {c}")
                _debug(
                    f"c -> {c}  only_fors -> {only_fors}  only_fors_ok -> {only_fors_ok}"
                )
                continue

            datasets = []

            # noinspection PyBroadException,PyExceptClausesOrder
            try:
                if isinstance(d["data"], list):
                    sqlite_db_path = generate_sqlite_db_path()
                    sub_index: int = 1  # additional index needed when dataset is a list
                    for dataset in d["data"]:
                        dataset_name = generate_dataset_name_from_expectation_name(
                            dataset=dataset,
                            expectation_type=expectation_type,
                            index=i,
                            sub_index=sub_index,
                        )
                        sub_index += 1
                        datasets.append(
                            get_test_validator_with_data(
                                execution_engine=c,
                                data=dataset["data"],
                                schemas=dataset.get("schemas"),
                                table_name=dataset_name,
                                sqlite_db_path=sqlite_db_path,
                                extra_debug_info=expectation_type,
                                debug_logger=debug_logger,
                                context=context,
                            )
                        )
                    validator_with_data = datasets[0]
                else:
                    dataset_name = generate_dataset_name_from_expectation_name(
                        dataset=d,  # type: ignore[arg-type] # should be dict but got ExpectationTestDataCases
                        expectation_type=expectation_type,
                        index=i,
                    )
                    dataset_name = d.get(
                        "dataset_name", f"{expectation_type}_dataset_{i}"
                    )
                    validator_with_data = get_test_validator_with_data(
                        execution_engine=c,
                        data=d["data"],
                        schemas=d["schemas"],
                        table_name=dataset_name,
                        extra_debug_info=expectation_type,
                        debug_logger=debug_logger,
                        context=context,
                    )
            except Exception as e:
                # # Adding these print statements for build_gallery.py's console output
                # print("\n\n[[ Problem calling get_test_validator_with_data ]]")
                # print(f"expectation_type -> {expectation_type}")
                # print(f"c -> {c}\ne -> {e}")
                # print(f"d['data'] -> {d.get('data')}")
                # print(f"d['schemas'] -> {d.get('schemas')}")
                # print("DataFrame from data without any casting/conversion ->")
                # print(pd.DataFrame(d.get("data")))
                # print()

                if "data_alt" in d and d["data_alt"] is not None:
                    # print("There is alternate data to try!!")
                    # noinspection PyBroadException
                    try:
                        if isinstance(d["data_alt"], list):
                            sqlite_db_path = generate_sqlite_db_path()
                            for dataset in d["data_alt"]:
                                datasets.append(
                                    get_test_validator_with_data(
                                        execution_engine=c,
                                        data=dataset["data_alt"],
                                        schemas=dataset.get("schemas"),
                                        table_name=dataset.get("dataset_name"),
                                        sqlite_db_path=sqlite_db_path,
                                        extra_debug_info=expectation_type,
                                        debug_logger=debug_logger,
                                        context=context,
                                    )
                                )
                            validator_with_data = datasets[0]
                        else:
                            validator_with_data = get_test_validator_with_data(
                                execution_engine=c,
                                data=d["data_alt"],
                                schemas=d["schemas"],
                                table_name=d["dataset_name"],
                                extra_debug_info=expectation_type,
                                debug_logger=debug_logger,
                                context=context,
                            )
                    except Exception:
                        # print(
                        #     "\n[[ STILL Problem calling get_test_validator_with_data ]]"
                        # )
                        # print(f"expectation_type -> {expectation_type}")
                        # print(f"c -> {c}\ne2 -> {e2}")
                        # print(f"d['data_alt'] -> {d.get('data_alt')}")
                        # print(
                        #     "DataFrame from data_alt without any casting/conversion ->"
                        # )
                        # print(pd.DataFrame(d.get("data_alt")))
                        # print()
                        _error(
                            f"PROBLEM with get_test_validator_with_data in backend {c} for {expectation_type} from data AND data_alt {repr(e)[:300]}"
                        )
                        parametrized_tests.append(
                            {
                                "expectation_type": expectation_type,
                                "validator_with_data": None,
                                "error": repr(e)[:300],
                                "test": None,
                                "backend": c,
                            }
                        )
                        continue
                    else:
                        # print("\n[[ The alternate data worked!! ]]\n")
                        _debug(
                            f"Needed to use data_alt for backend {c}, but it worked for {expectation_type}"
                        )
                else:
                    _error(
                        f"PROBLEM with get_test_validator_with_data in backend {c} for {expectation_type} from data (no data_alt to try) {repr(e)[:300]}"
                    )
                    parametrized_tests.append(
                        {
                            "expectation_type": expectation_type,
                            "validator_with_data": None,
                            "error": repr(e)[:300],
                            "test": None,
                            "backend": c,
                        }
                    )
                    continue

            for test in d["tests"]:
                if not should_we_generate_this_test(
                    backend=c,
                    expectation_test_case=test,
                    ignore_suppress=ignore_suppress,
                    ignore_only_for=ignore_only_for,
                    extra_debug_info=expectation_type,
                    debug_logger=debug_logger,
                ):
                    continue

                # Known condition: SqlAlchemy does not support allow_cross_type_comparisons
                if (
                    "allow_cross_type_comparisons" in test["input"]
                    and validator_with_data
                    and isinstance(
                        validator_with_data.execution_engine.batch_manager.active_batch_data,
                        SqlAlchemyBatchData,
                    )
                ):
                    continue

                parametrized_tests.append(
                    {
                        "expectation_type": expectation_type,
                        "validator_with_data": validator_with_data,
                        "test": test,
                        "backend": c,
                    }
                )

    return parametrized_tests


def should_we_generate_this_test(  # noqa: PLR0911, PLR0913, PLR0912
    backend: str,
    expectation_test_case: ExpectationTestCase,
    ignore_suppress: bool = False,
    ignore_only_for: bool = False,
    extra_debug_info: str = "",
    debug_logger: Optional[logging.Logger] = None,
):
    _debug = lambda x: x  # noqa: E731
    if debug_logger:
        _debug = lambda x: debug_logger.debug(f"(should_we_generate_this_test) {x}")  # type: ignore[union-attr] # noqa: E731

    # backend will only ever be pandas, spark, or a specific SQL dialect, but sometimes
    # suppress_test_for or only_for may include "sqlalchemy"
    #
    # There is one Expectation (expect_column_values_to_be_of_type) that has some tests that
    # are only for specific versions of pandas
    #   - only_for can be any of: pandas, pandas_022, pandas_023, pandas>=024
    #   - See: https://github.com/great-expectations/great_expectations/blob/7766bb5caa4e0e5b22fa3b3a5e1f2ac18922fdeb/tests/test_definitions/test_expectations_cfe.py#L176-L185
    if backend in expectation_test_case.suppress_test_for:
        if ignore_suppress:
            _debug(
                f"Should be suppressing {expectation_test_case.title} for {backend}, but ignore_suppress is True | {extra_debug_info}"
            )
            return True
        else:
            _debug(
                f"Backend {backend} is suppressed for test {expectation_test_case.title}: | {extra_debug_info}"
            )
            return False
    if (
        "sqlalchemy" in expectation_test_case.suppress_test_for
        and backend in SQL_DIALECT_NAMES
    ):
        if ignore_suppress:
            _debug(
                f"Should be suppressing {expectation_test_case.title} for sqlalchemy (including {backend}), but ignore_suppress is True | {extra_debug_info}"
            )
            return True
        else:
            _debug(
                f"All sqlalchemy (including {backend}) is suppressed for test: {expectation_test_case.title} | {extra_debug_info}"
            )
            return False
    if expectation_test_case.only_for is not None and expectation_test_case.only_for:
        if backend not in expectation_test_case.only_for:
            if (
                "sqlalchemy" in expectation_test_case.only_for
                and backend in SQL_DIALECT_NAMES
            ):
                return True
            elif "pandas" == backend:
                major, minor, *_ = pd.__version__.split(".")
                if (
                    "pandas_022" in expectation_test_case.only_for
                    or "pandas_023" in expectation_test_case.only_for
                ):
                    if major == "0" and minor in ["22", "23"]:
                        return True
                elif "pandas>=024" in expectation_test_case.only_for:
                    if (major == "0" and int(minor) >= 24) or int(  # noqa: PLR2004
                        major
                    ) >= 1:
                        return True

            if ignore_only_for:
                _debug(
                    f"Should normally not run test {expectation_test_case.title} for {backend}, but ignore_only_for is True | {extra_debug_info}"
                )
                return True
            else:
                _debug(
                    f"Only {expectation_test_case.only_for} allowed (not {backend}) for test: {expectation_test_case.title} | {extra_debug_info}"
                )
                return False

    return True


def sort_unexpected_values(test_value_list, result_value_list):
    # check if value can be sorted; if so, sort so arbitrary ordering of results does not cause failure
    if (isinstance(test_value_list, list)) & (len(test_value_list) >= 1):
        # __lt__ is not implemented for python dictionaries making sorting trickier
        # in our case, we will sort on the values for each key sequentially
        if isinstance(test_value_list[0], dict):
            test_value_list = sorted(
                test_value_list,
                key=lambda x: tuple(x[k] for k in list(test_value_list[0].keys())),
            )
            result_value_list = sorted(
                result_value_list,
                key=lambda x: tuple(x[k] for k in list(test_value_list[0].keys())),
            )
        # if python built-in class has __lt__ then sorting can always work this way
        elif type(test_value_list[0].__lt__(test_value_list[0])) != type(
            NotImplemented
        ):
            test_value_list = sorted(test_value_list, key=lambda x: str(x))
            result_value_list = sorted(result_value_list, key=lambda x: str(x))

    return test_value_list, result_value_list


def evaluate_json_test_v2_api(data_asset, expectation_type, test) -> None:
    """
    This method will evaluate the result of a test build using the Great Expectations json test format.

    NOTE: Tests can be suppressed for certain data types if the test contains the Key 'suppress_test_for' with a list
        of DataAsset types to suppress, such as ['SQLAlchemy', 'Pandas'].

    :param data_asset: (DataAsset) A great expectations DataAsset
    :param expectation_type: (string) the name of the expectation to be run using the test input
    :param test: (dict) a dictionary containing information for the test to be run. The dictionary must include:
        - title: (string) the name of the test
        - exact_match_out: (boolean) If true, match the 'out' dictionary exactly against the result of the expectation
        - in: (dict or list) a dictionary of keyword arguments to use to evaluate the expectation or a list of positional arguments
        - out: (dict) the dictionary keys against which to make assertions. Unless exact_match_out is true, keys must\
            come from the following list:
              - success
              - observed_value
              - unexpected_index_list
              - unexpected_list
              - details
              - traceback_substring (if present, the string value will be expected as a substring of the exception_traceback)
    :return: None. asserts correctness of results.
    """

    data_asset.set_default_expectation_argument("result_format", "COMPLETE")
    data_asset.set_default_expectation_argument("include_config", False)

    if "title" not in test:
        raise ValueError("Invalid test configuration detected: 'title' is required.")

    if "exact_match_out" not in test:
        raise ValueError(
            "Invalid test configuration detected: 'exact_match_out' is required."
        )

    if "input" not in test:
        if "in" in test:
            test["input"] = test["in"]
        else:
            raise ValueError(
                "Invalid test configuration detected: 'input' is required."
            )

    if "output" not in test:
        if "out" in test:
            test["output"] = test["out"]
        else:
            raise ValueError(
                "Invalid test configuration detected: 'output' is required."
            )

    # Support tests with positional arguments
    if isinstance(test["input"], list):
        result = getattr(data_asset, expectation_type)(*test["input"])
    # As well as keyword arguments
    else:
        result = getattr(data_asset, expectation_type)(**test["input"])

    check_json_test_result(test=test, result=result, data_asset=data_asset)


def evaluate_json_test_v3_api(  # noqa: PLR0912, PLR0913
    validator: Validator,
    expectation_type: str,
    test: Dict[str, Any],
    raise_exception: bool = True,
    debug_logger: Optional[Logger] = None,
    pk_column: bool = False,
):
    """
    This method will evaluate the result of a test build using the Great Expectations json test format.

    NOTE: Tests can be suppressed for certain data types if the test contains the Key 'suppress_test_for' with a list
        of DataAsset types to suppress, such as ['SQLAlchemy', 'Pandas'].

    :param validator: (Validator) reference to "Validator" (key object that resolves Metrics and validates Expectations)
    :param expectation_type: (string) the name of the expectation to be run using the test input
    :param test: (dict) a dictionary containing information for the test to be run. The dictionary must include:
        - title: (string) the name of the test
        - exact_match_out: (boolean) If true, match the 'out' dictionary exactly against the result of the expectation
        - in: (dict or list) a dictionary of keyword arguments to use to evaluate the expectation or a list of positional arguments
        - out: (dict) the dictionary keys against which to make assertions. Unless exact_match_out is true, keys must\
            come from the following list:
              - success
              - observed_value
              - unexpected_index_list
              - unexpected_list
              - details
              - traceback_substring (if present, the string value will be expected as a substring of the exception_traceback)
    :param raise_exception: (bool) If False, capture any failed AssertionError from the call to check_json_test_result and return with validation_result
    :param debug_logger: logger instance or None
    :param pk_column: If True, then the primary-key column has been defined in the json test data.
    :return: Tuple(ExpectationValidationResult, error_message, stack_trace). asserts correctness of results.
    """
    if debug_logger is not None:
        _debug = lambda x: debug_logger.debug(  # noqa: E731
            f"(evaluate_json_test_v3_api) {x}"
        )
    else:
        _debug = lambda x: x  # noqa: E731

    expectation_suite = ExpectationSuite(
        "json_test_suite", data_context=validator._data_context
    )
    # noinspection PyProtectedMember
    validator._initialize_expectations(expectation_suite=expectation_suite)
    # validator.set_default_expectation_argument("result_format", "COMPLETE")
    # validator.set_default_expectation_argument("include_config", False)

    if "title" not in test:
        raise ValueError("Invalid test configuration detected: 'title' is required.")

    if "exact_match_out" not in test:
        raise ValueError(
            "Invalid test configuration detected: 'exact_match_out' is required."
        )

    if "input" not in test:
        if "in" in test:
            test["input"] = test["in"]
        else:
            raise ValueError(
                "Invalid test configuration detected: 'input' is required."
            )

    if "output" not in test:
        if "out" in test:
            test["output"] = test["out"]
        else:
            raise ValueError(
                "Invalid test configuration detected: 'output' is required."
            )

    kwargs = copy.deepcopy(test["input"])
    error_message = None
    stack_trace = None

    try:
        if isinstance(test["input"], list):
            result = getattr(validator, expectation_type)(*kwargs)
        # As well as keyword arguments
        else:
            if pk_column:
                runtime_kwargs = {
                    "result_format": {
                        "result_format": "COMPLETE",
                        "unexpected_index_column_names": ["pk_index"],
                    },
                    "include_config": False,
                }
            else:
                runtime_kwargs = {
                    "result_format": {
                        "result_format": "COMPLETE",
                    },
                    "include_config": False,
                }
            runtime_kwargs.update(kwargs)
            result = getattr(validator, expectation_type)(**runtime_kwargs)
    except (
        MetricProviderError,
        MetricResolutionError,
        InvalidExpectationConfigurationError,
    ) as e:
        if raise_exception:
            raise
        error_message = str(e)
        stack_trace = (traceback.format_exc(),)
        result = None
    else:
        try:
            check_json_test_result(
                test=test,
                result=result,
                data_asset=validator.execution_engine.batch_manager.active_batch_data,
                pk_column=pk_column,
            )
        except Exception as e:
            _debug(
                f"RESULT: {result['result']}  |  CONFIG: {result['expectation_config']}"
            )
            if raise_exception:
                raise
            error_message = str(e)
            stack_trace = (traceback.format_exc(),)

    return (result, error_message, stack_trace)


def check_json_test_result(  # noqa: C901, PLR0912, PLR0915
    test, result, data_asset=None, pk_column=False
) -> None:
    # check for id_pk results in cases where pk_column is true and unexpected_index_list already exists
    # this will work for testing since result_format is COMPLETE
    if pk_column:
        if not result["success"]:
            if "unexpected_index_list" in result["result"]:
                assert "unexpected_index_query" in result["result"]

    if "unexpected_list" in result["result"]:
        if ("result" in test["output"]) and (
            "unexpected_list" in test["output"]["result"]
        ):
            (
                test["output"]["result"]["unexpected_list"],
                result["result"]["unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["result"]["unexpected_list"],
                result["result"]["unexpected_list"],
            )
        elif "unexpected_list" in test["output"]:
            (
                test["output"]["unexpected_list"],
                result["result"]["unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["unexpected_list"],
                result["result"]["unexpected_list"],
            )

    if "partial_unexpected_list" in result["result"]:
        if ("result" in test["output"]) and (
            "partial_unexpected_list" in test["output"]["result"]
        ):
            (
                test["output"]["result"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["result"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            )
        elif "partial_unexpected_list" in test["output"]:
            (
                test["output"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            ) = sort_unexpected_values(
                test["output"]["partial_unexpected_list"],
                result["result"]["partial_unexpected_list"],
            )

    # Determine if np.allclose(..) might be needed for float comparison
    try_allclose = False
    if "observed_value" in test["output"]:
        if RX_FLOAT.match(repr(test["output"]["observed_value"])):
            try_allclose = True

    # Check results
    if test["exact_match_out"] is True:
        if "result" in result and "observed_value" in result["result"]:
            if isinstance(result["result"]["observed_value"], (np.floating, float)):
                assert np.allclose(
                    result["result"]["observed_value"],
                    expectationValidationResultSchema.load(test["output"])["result"][
                        "observed_value"
                    ],
                    rtol=RTOL,
                    atol=ATOL,
                ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['result']['observed_value']} not np.allclose to {expectationValidationResultSchema.load(test['output'])['result']['observed_value']}"
            else:
                assert result == expectationValidationResultSchema.load(
                    test["output"]
                ), f"{result} != {expectationValidationResultSchema.load(test['output'])}"
        else:
            assert result == expectationValidationResultSchema.load(
                test["output"]
            ), f"{result} != {expectationValidationResultSchema.load(test['output'])}"
    else:
        # Convert result to json since our tests are reading from json so cannot easily contain richer types (e.g. NaN)
        # NOTE - 20191031 - JPC - we may eventually want to change these tests as we update our view on how
        # representations, serializations, and objects should interact and how much of that is shown to the user.
        result = result.to_json_dict()
        for key, value in test["output"].items():
            if key == "success":
                if isinstance(value, (np.floating, float)):
                    try:
                        assert np.allclose(
                            result["success"],
                            value,
                            rtol=RTOL,
                            atol=ATOL,
                        ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['success']} not np.allclose to {value}"
                    except TypeError:
                        assert (
                            result["success"] == value
                        ), f"{result['success']} != {value}"
                else:
                    assert result["success"] == value, f"{result['success']} != {value}"

            elif key == "observed_value":
                if "tolerance" in test:
                    if isinstance(value, dict):
                        assert set(result["result"]["observed_value"].keys()) == set(
                            value.keys()
                        ), f"{set(result['result']['observed_value'].keys())} != {set(value.keys())}"
                        for k, v in value.items():
                            assert np.allclose(
                                result["result"]["observed_value"][k],
                                v,
                                rtol=test["tolerance"],
                            )
                    else:
                        assert np.allclose(
                            result["result"]["observed_value"],
                            value,
                            rtol=test["tolerance"],
                        )
                else:
                    if isinstance(value, dict) and "values" in value:  # noqa: PLR5501
                        try:
                            assert np.allclose(
                                result["result"]["observed_value"]["values"],
                                value["values"],
                                rtol=RTOL,
                                atol=ATOL,
                            ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['result']['observed_value']['values']} not np.allclose to {value['values']}"
                        except TypeError as e:
                            print(e)
                            assert (
                                result["result"]["observed_value"] == value
                            ), f"{result['result']['observed_value']} != {value}"
                    elif try_allclose:
                        assert np.allclose(
                            result["result"]["observed_value"],
                            value,  # type: ignore[arg-type]
                            rtol=RTOL,
                            atol=ATOL,
                        ), f"(RTOL={RTOL}, ATOL={ATOL}) {result['result']['observed_value']} not np.allclose to {value}"
                    else:
                        assert (
                            result["result"]["observed_value"] == value
                        ), f"{result['result']['observed_value']} != {value}"

            # NOTE: This is a key used ONLY for testing cases where an expectation is legitimately allowed to return
            # any of multiple possible observed_values. expect_column_values_to_be_of_type is one such expectation.
            elif key == "observed_value_list":
                assert result["result"]["observed_value"] in value

            elif key == "unexpected_index_list":
                unexpected_list = result["result"].get("unexpected_index_list")
                if pk_column and unexpected_list:
                    # Note that consistent ordering of unexpected_list is not a guarantee by ID/PK
                    assert (
                        sorted(unexpected_list, key=lambda d: d["pk_index"]) == value
                    ), f"{unexpected_list} != {value}"

            elif key == "unexpected_list":
                try:
                    assert result["result"]["unexpected_list"] == value, (
                        "expected "
                        + str(value)
                        + " but got "
                        + str(result["result"]["unexpected_list"])
                    )
                except AssertionError:
                    if result["result"]["unexpected_list"]:
                        if type(result["result"]["unexpected_list"][0]) == list:
                            unexpected_list_tup = [
                                tuple(x) for x in result["result"]["unexpected_list"]
                            ]
                            assert (
                                unexpected_list_tup == value
                            ), f"{unexpected_list_tup} != {value}"
                        else:
                            raise
                    else:
                        raise

            elif key == "partial_unexpected_list":
                assert result["result"]["partial_unexpected_list"] == value, (
                    "expected "
                    + str(value)
                    + " but got "
                    + str(result["result"]["partial_unexpected_list"])
                )

            elif key == "unexpected_count":
                pass

            elif key == "details":
                assert result["result"]["details"] == value

            elif key == "value_counts":
                for val_count in value:
                    assert val_count in result["result"]["details"]["value_counts"]

            elif key.startswith("observed_cdf"):
                if "x_-1" in key:
                    if key.endswith("gt"):
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][-1] > value
                        )
                    else:
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][-1]
                            == value
                        )
                elif "x_0" in key:
                    if key.endswith("lt"):
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][0] < value
                        )
                    else:
                        assert (
                            result["result"]["details"]["observed_cdf"]["x"][0] == value
                        )
                else:
                    raise ValueError(
                        f"Invalid test specification: unknown key {key} in 'out'"
                    )

            elif key == "traceback_substring":
                assert result["exception_info"][
                    "raised_exception"
                ], f"{result['exception_info']['raised_exception']}"
                assert value in result["exception_info"]["exception_traceback"], (
                    "expected to find "
                    + value
                    + " in "
                    + result["exception_info"]["exception_traceback"]
                )

            elif key == "expected_partition":
                assert np.allclose(
                    result["result"]["details"]["expected_partition"]["bins"],
                    value["bins"],
                )
                assert np.allclose(
                    result["result"]["details"]["expected_partition"]["weights"],
                    value["weights"],
                )
                if "tail_weights" in result["result"]["details"]["expected_partition"]:
                    assert np.allclose(
                        result["result"]["details"]["expected_partition"][
                            "tail_weights"
                        ],
                        value["tail_weights"],
                    )

            elif key == "observed_partition":
                assert np.allclose(
                    result["result"]["details"]["observed_partition"]["bins"],
                    value["bins"],
                )
                assert np.allclose(
                    result["result"]["details"]["observed_partition"]["weights"],
                    value["weights"],
                )
                if "tail_weights" in result["result"]["details"]["observed_partition"]:
                    assert np.allclose(
                        result["result"]["details"]["observed_partition"][
                            "tail_weights"
                        ],
                        value["tail_weights"],
                    )

            else:
                raise ValueError(
                    f"Invalid test specification: unknown key {key} in 'out'"
                )


def generate_test_table_name(
    default_table_name_prefix: str = "test_data_",
) -> str:
    table_name: str = default_table_name_prefix + "".join(
        [random.choice(string.ascii_letters + string.digits) for _ in range(8)]
    )
    return table_name


def generate_dataset_name_from_expectation_name(
    dataset: dict, expectation_type: str, index: int, sub_index: int | None = None
) -> str:
    """Method to generate dataset_name for tests. Will either use the name defined in the test
    configuration ("dataset_name"), or generate one using the Expectation name and index. In cases where
    the dataset is a list, then an additional index will be used.

    Args:
        dataset (dict): definition of data and (possibly) dataset-name
        expectation_type (str): Expectation that the test_data is being generated for.
        index (int): index used to number the dataset, so that we can insert a pre-defined list of tables into our db.
        sub_index (Optional int): In cases where dataset is a list, the additional index is used.

    Returns: dataset_name
    """

    dataset_name: str
    if not sub_index:
        dataset_name = dataset.get(
            "dataset_name", f"{expectation_type}_dataset_{index}"
        )
    else:
        dataset_name = dataset.get(
            "dataset_name", f"{expectation_type}_dataset_{index}_{sub_index}"
        )

    dataset_name = _check_if_valid_dataset_name(dataset_name)
    return dataset_name


def _check_if_valid_dataset_name(dataset_name: str) -> str:
    """Check that dataset_name (ie. table name) is valid before adding data to table.

    A valid dataset_name must:

        1. Contain only alphanumeric characters and `_`
        2. Not be longer than 63 characters (which is the limit for postgres)
        3. Begin with letter

    Args:
        dataset_name (str): dataset_name passed in by user or generated by generate_dataset_name_from_expectation_name()

    Returns: dataset_name

    """
    if not re.match(r"^[A-Za-z0-9_]+$", dataset_name):
        raise ExecutionEngineError(
            f"dataset_name: {dataset_name} is not valid, because it contains non-alphanumeric and _ characters."
            f"Please check your configuration."
        )

    if len(dataset_name) >= MAX_TABLE_NAME_LENGTH:
        # starting from the end, so that we always get the index and sub_index
        new_dataset_name = dataset_name[-MAX_TABLE_NAME_LENGTH:]
        logger.info(
            f"dataset_name: '{dataset_name}' was truncated to '{new_dataset_name}' to keep within length limits."
        )
        dataset_name = new_dataset_name

    while not re.match(r"^[A-Za-z]+$", dataset_name[0]):
        dataset_name = dataset_name[1:]

    return dataset_name


def _create_bigquery_engine() -> sqlalchemy.Engine:
    return sa.create_engine(_get_bigquery_connection_string())


def _get_bigquery_connection_string() -> str:
    gcp_project = os.getenv("GE_TEST_GCP_PROJECT")
    if not gcp_project:
        raise ValueError(
            "Environment Variable GE_TEST_GCP_PROJECT is required to run BigQuery expectation tests"
        )

    return f"bigquery://{gcp_project}/{_bigquery_dataset()}"


def _bigquery_dataset() -> str:
    dataset = os.getenv("GE_TEST_BIGQUERY_DATASET")
    if not dataset:
        raise ValueError(
            "Environment Variable GE_TEST_BIGQUERY_DATASET is required to run BigQuery expectation tests"
        )
    return dataset


def _get_clickhouse_connection_string(
    hostname: str = "localhost", schema_name: str = "test"
) -> str:
    return f"clickhouse+native://{hostname}:9000/{schema_name}"


def _create_clickhouse_engine(
    hostname: str = "localhost", schema_name: str = "schema"
) -> sqlalchemy.Engine:
    engine = sa.create_engine(
        _get_clickhouse_connection_string(hostname=hostname, schema_name=schema_name)
    )
    from clickhouse_sqlalchemy.exceptions import DatabaseException
    from sqlalchemy import text  # noqa: TID251

    with engine.begin() as conn:
        try:
            schemas = conn.execute(
                text(f"show schemas from memory like {repr(schema_name)}")
            ).fetchall()
            if (schema_name,) not in schemas:
                conn.execute(text(f"create schema {schema_name}"))
        except DatabaseException:
            pass

    return engine


def _create_trino_engine(
    hostname: str = "localhost", schema_name: str = "schema"
) -> sqlalchemy.Engine:
    engine = sa.create_engine(
        _get_trino_connection_string(hostname=hostname, schema_name=schema_name)
    )

    with engine.begin() as conn:
        try:
            schemas = conn.execute(
                sa.text(f"show schemas from memory like {repr(schema_name)}")
            ).fetchall()
            if (schema_name,) not in schemas:
                conn.execute(sa.text(f"create schema {schema_name}"))
        except trino.trinoexceptions.TrinoUserError:
            pass

    return engine
    # trino_user = os.getenv("GE_TEST_TRINO_USER")
    # if not trino_user:
    #     raise ValueError(
    #         "Environment Variable GE_TEST_TRINO_USER is required to run trino expectation tests."
    #     )

    # trino_password = os.getenv("GE_TEST_TRINO_PASSWORD")
    # if not trino_password:
    #     raise ValueError(
    #         "Environment Variable GE_TEST_TRINO_PASSWORD is required to run trino expectation tests."
    #     )

    # trino_account = os.getenv("GE_TEST_TRINO_ACCOUNT")
    # if not trino_account:
    #     raise ValueError(
    #         "Environment Variable GE_TEST_TRINO_ACCOUNT is required to run trino expectation tests."
    #     )

    # trino_cluster = os.getenv("GE_TEST_TRINO_CLUSTER")
    # if not trino_cluster:
    #     raise ValueError(
    #         "Environment Variable GE_TEST_TRINO_CLUSTER is required to run trino expectation tests."
    #     )

    # return create_engine(
    #     f"trino://{trino_user}:{trino_password}@{trino_account}-{trino_cluster}.trino.galaxy.starburst.io:443/test_suite/test_ci"
    # )


def _get_trino_connection_string(
    hostname: str = "localhost", schema_name: str = "schema"
) -> str:
    return f"trino://test@{hostname}:8088/memory/{schema_name}"


def _create_redshift_engine() -> sqlalchemy.Engine:
    return sa.create_engine(_get_redshift_connection_string())


def _get_redshift_connection_string() -> str:
    """
    Copied get_redshift_connection_url func from tests/test_utils.py
    """
    host = os.environ.get("REDSHIFT_HOST")
    port = os.environ.get("REDSHIFT_PORT")
    user = os.environ.get("REDSHIFT_USERNAME")
    pswd = os.environ.get("REDSHIFT_PASSWORD")
    db = os.environ.get("REDSHIFT_DATABASE")
    ssl = os.environ.get("REDSHIFT_SSLMODE")

    if not host:
        raise ValueError(
            "Environment Variable REDSHIFT_HOST is required to run integration tests against Redshift"
        )
    if not port:
        raise ValueError(
            "Environment Variable REDSHIFT_PORT is required to run integration tests against Redshift"
        )
    if not user:
        raise ValueError(
            "Environment Variable REDSHIFT_USERNAME is required to run integration tests against Redshift"
        )
    if not pswd:
        raise ValueError(
            "Environment Variable REDSHIFT_PASSWORD is required to run integration tests against Redshift"
        )
    if not db:
        raise ValueError(
            "Environment Variable REDSHIFT_DATABASE is required to run integration tests against Redshift"
        )
    if not ssl:
        raise ValueError(
            "Environment Variable REDSHIFT_SSLMODE is required to run integration tests against Redshift"
        )

    url = f"redshift+psycopg2://{user}:{pswd}@{host}:{port}/{db}?sslmode={ssl}"

    return url


def _create_athena_engine(
    db_name_env_var: str = "ATHENA_DB_NAME",
) -> sqlalchemy.Engine:
    return sa.create_engine(
        _get_athena_connection_string(db_name_env_var=db_name_env_var)
    )


def _get_athena_connection_string(db_name_env_var: str = "ATHENA_DB_NAME") -> str:
    """
    Copied get_awsathena_connection_url and get_awsathena_db_name funcs from
    tests/test_utils.py
    """
    ATHENA_DB_NAME: Optional[str] = os.getenv(db_name_env_var)
    ATHENA_STAGING_S3: Optional[str] = os.getenv("ATHENA_STAGING_S3")

    if not ATHENA_DB_NAME:
        raise ValueError(
            f"Environment Variable {db_name_env_var} is required to run integration tests against AWS Athena"
        )

    if not ATHENA_STAGING_S3:
        raise ValueError(
            "Environment Variable ATHENA_STAGING_S3 is required to run integration tests against AWS Athena"
        )

    url = f"awsathena+rest://@athena.us-east-1.amazonaws.com/{ATHENA_DB_NAME}?s3_staging_dir={ATHENA_STAGING_S3}"

    return url


def _create_snowflake_engine() -> sqlalchemy.Engine:
    return sa.create_engine(_get_snowflake_connection_string())


def _get_snowflake_connection_string() -> str:
    """
    Copied get_snowflake_connection_url func from tests/test_utils.py
    """
    sfUser = os.environ.get("SNOWFLAKE_USER")
    sfPswd = os.environ.get("SNOWFLAKE_PW")
    sfAccount = os.environ.get("SNOWFLAKE_ACCOUNT")
    sfDatabase = os.environ.get("SNOWFLAKE_DATABASE")
    sfSchema = os.environ.get("SNOWFLAKE_SCHEMA")
    sfWarehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
    sfRole = os.environ.get("SNOWFLAKE_ROLE") or "PUBLIC"

    url = f"snowflake://{sfUser}:{sfPswd}@{sfAccount}/{sfDatabase}/{sfSchema}?warehouse={sfWarehouse}&role={sfRole}"

    return url


def generate_sqlite_db_path():
    """Creates a temporary directory and absolute path to an ephemeral sqlite_db within that temp directory.

    Used to support testing of multi-table expectations without creating temp directories at import.

    Returns:
        str: An absolute path to the ephemeral db within the created temporary directory.
    """
    tmp_dir = str(tempfile.mkdtemp())
    abspath = os.path.abspath(  # noqa: PTH100
        os.path.join(  # noqa: PTH118
            tmp_dir,
            "sqlite_db"
            + "".join(
                [random.choice(string.ascii_letters + string.digits) for _ in range(8)]
            )
            + ".db",
        )
    )
    return abspath
