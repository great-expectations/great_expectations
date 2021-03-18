import copy
import logging
import random
import string
from typing import Dict, List, Union

import numpy as np
import pandas as pd
from dateutil.parser import parse

from great_expectations.exceptions import GreatExpectationsError
from great_expectations.execution_engine.sqlalchemy_batch_data import SqlAlchemyBatchData
from great_expectations.render.types import RenderedStringTemplateContent
from great_expectations.core import (
    ExpectationConfigurationSchema,
    ExpectationSuiteSchema,
    ExpectationSuiteValidationResultSchema,
    ExpectationValidationResultSchema,
)
from great_expectations.core.batch import Batch
from great_expectations.core.util import get_or_create_spark_application
from great_expectations.execution_engine import (
    SparkDFExecutionEngine, SqlAlchemyExecutionEngine, PandasExecutionEngine,
)
from great_expectations.profile import ColumnsExistProfiler
from great_expectations.validator.validator import Validator

expectationValidationResultSchema = ExpectationValidationResultSchema()
expectationSuiteValidationResultSchema = ExpectationSuiteValidationResultSchema()
expectationConfigurationSchema = ExpectationConfigurationSchema()
expectationSuiteSchema = ExpectationSuiteSchema()

logger = logging.getLogger(__name__)

try:
    from sqlalchemy import create_engine
except ImportError:
    create_engine = None

try:
    from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
except ImportError:
    SparkSession = None
    SparkDataFrame = type(None)

try:
    import sqlalchemy.dialects.sqlite as sqlitetypes

    SQLITE_TYPES = {
        "VARCHAR": sqlitetypes.VARCHAR,
        "CHAR": sqlitetypes.CHAR,
        "INTEGER": sqlitetypes.INTEGER,
        "SMALLINT": sqlitetypes.SMALLINT,
        "DATETIME": sqlitetypes.DATETIME(truncate_microseconds=True),
        "DATE": sqlitetypes.DATE,
        "FLOAT": sqlitetypes.FLOAT,
        "BOOLEAN": sqlitetypes.BOOLEAN,
        "TIMESTAMP": sqlitetypes.TIMESTAMP,
    }
except ImportError:
    sqlitetypes = None
    SQLITE_TYPES = {}

try:
    import sqlalchemy.dialects.postgresql as postgresqltypes

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
except ImportError:
    postgresqltypes = None
    POSTGRESQL_TYPES = {}

try:
    import sqlalchemy.dialects.mysql as mysqltypes

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
except ImportError:
    mysqltypes = None
    MYSQL_TYPES = {}

try:
    import sqlalchemy.dialects.mssql as mssqltypes

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
except ImportError:
    mssqltypes = None
    MSSQL_TYPES = {}


def render_evaluation_parameter_string(render_func):
    def inner_func(*args, **kwargs):
        rendered_string_template = render_func(*args, **kwargs)
        current_expectation_params = list()
        app_template_str = (
            "\n - $eval_param = $eval_param_value (at time of validation)."
        )
        configuration = kwargs.get("configuration", None)
        kwargs_dict = configuration.kwargs
        for key, value in kwargs_dict.items():
            if isinstance(value, dict) and "$PARAMETER" in value.keys():
                current_expectation_params.append(value["$PARAMETER"])

        # if expectation configuration has no eval params, then don't look for the values in runtime_configuration
        if len(current_expectation_params) > 0:
            runtime_configuration = kwargs.get("runtime_configuration", None)
            if runtime_configuration:
                eval_params = runtime_configuration.get("evaluation_parameters", {})
                styling = runtime_configuration.get("styling")
                for key, val in eval_params.items():
                    # this needs to be more complicated?
                    # the possibility that it is a substring?
                    for param in current_expectation_params:
                        # "key in param" condition allows for eval param values to be rendered if arithmetic is present
                        if key == param or key in param:
                            app_params = dict()
                            app_params["eval_param"] = key
                            app_params["eval_param_value"] = val
                            to_append = RenderedStringTemplateContent(
                                **{
                                    "content_block_type": "string_template",
                                    "string_template": {
                                        "template": app_template_str,
                                        "params": app_params,
                                        "styling": styling,
                                    },
                                }
                            )
                            rendered_string_template.append(to_append)
            else:
                raise GreatExpectationsError(
                    f"""GE was not able to render the value of evaluation parameters.
                        Expectation {render_func} had evaluation parameters set, but they were not passed in."""
                )
        return rendered_string_template

    return inner_func


legacy_method_parameters = {
    "expect_column_bootstrapped_ks_test_p_value_to_be_greater_than": (
        "column",
        "partition_object",
        "p",
        "bootstrap_samples",
        "bootstrap_sample_size",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_chisquare_test_p_value_to_be_greater_than": (
        "column",
        "partition_object",
        "p",
        "tail_weight_holdout",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_distinct_values_to_be_in_set": (
        "column",
        "value_set",
        "parse_strings_as_datetimes",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_distinct_values_to_contain_set": (
        "column",
        "value_set",
        "parse_strings_as_datetimes",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_distinct_values_to_equal_set": (
        "column",
        "value_set",
        "parse_strings_as_datetimes",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_kl_divergence_to_be_less_than": (
        "column",
        "partition_object",
        "threshold",
        "tail_weight_holdout",
        "internal_weight_holdout",
        "bucketize_data",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_max_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "parse_strings_as_datetimes",
        "output_strftime_format",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_mean_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_median_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_min_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "parse_strings_as_datetimes",
        "output_strftime_format",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_most_common_value_to_be_in_set": (
        "column",
        "value_set",
        "ties_okay",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_pair_cramers_phi_value_to_be_less_than": (
        "column_A",
        "column_B",
        "bins_A",
        "bins_B",
        "n_bins_A",
        "n_bins_B",
        "threshold",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_pair_values_A_to_be_greater_than_B": (
        "column_A",
        "column_B",
        "or_equal",
        "parse_strings_as_datetimes",
        "allow_cross_type_comparisons",
        "ignore_row_if",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_pair_values_to_be_equal": (
        "column_A",
        "column_B",
        "ignore_row_if",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_pair_values_to_be_in_set": (
        "column_A",
        "column_B",
        "value_pairs_set",
        "ignore_row_if",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_parameterized_distribution_ks_test_p_value_to_be_greater_than": (
        "column",
        "distribution",
        "p_value",
        "params",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_proportion_of_unique_values_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_quantile_values_to_be_between": (
        "column",
        "quantile_ranges",
        "allow_relative_error",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_stdev_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_sum_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_to_exist": (
        "column",
        "column_index",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_unique_value_count_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_value_lengths_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "mostly",
        "row_condition",
        "condition_parser",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_value_lengths_to_equal": (
        "column",
        "value",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_between": (
        "column",
        "min_value",
        "max_value",
        "strict_min",
        "strict_max",
        "allow_cross_type_comparisons",
        "parse_strings_as_datetimes",
        "output_strftime_format",
        "mostly",
        "row_condition",
        "condition_parser",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_dateutil_parseable": (
        "column",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_decreasing": (
        "column",
        "strictly",
        "parse_strings_as_datetimes",
        "mostly",
        "row_condition",
        "condition_parser",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_in_set": (
        "column",
        "value_set",
        "mostly",
        "parse_strings_as_datetimes",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_in_type_list": (
        "column",
        "type_list",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_increasing": (
        "column",
        "strictly",
        "parse_strings_as_datetimes",
        "mostly",
        "row_condition",
        "condition_parser",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_json_parseable": (
        "column",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_null": (
        "column",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_of_type": (
        "column",
        "type_",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_be_unique": (
        "column",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_match_json_schema": (
        "column",
        "json_schema",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_match_regex": (
        "column",
        "regex",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_match_regex_list": (
        "column",
        "regex_list",
        "match_on",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_match_strftime_format": (
        "column",
        "strftime_format",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_not_be_in_set": (
        "column",
        "value_set",
        "mostly",
        "parse_strings_as_datetimes",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_not_be_null": (
        "column",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_not_match_regex": (
        "column",
        "regex",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_column_values_to_not_match_regex_list": (
        "column",
        "regex_list",
        "mostly",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_compound_columns_to_be_unique": (
        "column_list",
        "ignore_row_if",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_multicolumn_sum_to_equal": (
        "column_list",
        "sum_total",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_multicolumn_values_to_be_unique": (
        "column_list",
        "ignore_row_if",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_select_column_values_to_be_unique_within_record": (
        "column_list",
        "ignore_row_if",
        "result_format",
        "row_condition",
        "condition_parser",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_table_column_count_to_be_between": (
        "min_value",
        "max_value",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_table_column_count_to_equal": (
        "value",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_table_columns_to_match_ordered_list": (
        "column_list",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_table_columns_to_match_set": (
        "column_set",
        "exact_match",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_table_row_count_to_be_between": (
        "min_value",
        "max_value",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
    "expect_table_row_count_to_equal": (
        "value",
        "result_format",
        "include_config",
        "catch_exceptions",
        "meta",
    ),
}


def get_test_validator_with_data(
    execution_engine,
    data,
    schemas=None,
    profiler=ColumnsExistProfiler,
    caching=True,
    table_name=None,
    sqlite_db_path=None,
):
    """Utility to create datasets for json-formatted tests."""
    df = pd.DataFrame(data)
    if execution_engine == "pandas":
        if schemas and "pandas" in schemas:
            schema = schemas["pandas"]
            pandas_schema = {}
            for (key, value) in schema.items():
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
                try:
                    type_ = np.dtype(value)
                except TypeError:
                    type_ = getattr(pd.core.dtypes.dtypes, value)
                    # If this raises AttributeError it's okay: it means someone built a bad test
                pandas_schema[key] = type_
            # pandas_schema = {key: np.dtype(value) for (key, value) in schemas["pandas"].items()}
            df = df.astype(pandas_schema)

        if table_name is None:
            table_name = "test_data_" + "".join(
                [random.choice(string.ascii_letters + string.digits) for _ in range(8)]
            )

        return _build_pandas_validator_with_data(df=df)

    elif execution_engine in ["sqlite", "postgresql", "mysql", "mssql"]:
        if not create_engine:
            return None
        return _build_sa_validator_with_data(
            df=df,
            sa_engine_name=execution_engine,
            schemas=schemas,
            caching=caching,
            table_name=table_name,
            sqlite_db_path=sqlite_db_path,
        )

    elif execution_engine == "spark":
        import pyspark.sql.types as sparktypes

        SPARK_TYPES = {
            "StringType": sparktypes.StringType,
            "IntegerType": sparktypes.IntegerType,
            "LongType": sparktypes.LongType,
            "DateType": sparktypes.DateType,
            "TimestampType": sparktypes.TimestampType,
            "FloatType": sparktypes.FloatType,
            "DoubleType": sparktypes.DoubleType,
            "BooleanType": sparktypes.BooleanType,
            "DataType": sparktypes.DataType,
            "NullType": sparktypes.NullType,
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
            zip(*[v for _, v in data.items()])
        )  # create a list of rows
        if schemas and "spark" in schemas:
            schema = schemas["spark"]
            # sometimes first method causes Spark to throw a TypeError
            try:
                spark_schema = sparktypes.StructType(
                    [
                        sparktypes.StructField(
                            column, SPARK_TYPES[schema[column]](), True
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
                    zip(*[v for _, v in data.items()])
                )  # create a list of rows
                spark_df = spark.createDataFrame(data_reshaped, schema=spark_schema)
            except TypeError:
                string_schema = sparktypes.StructType(
                    [
                        sparktypes.StructField(column, sparktypes.StringType())
                        for column in schema
                    ]
                )
                spark_df = spark.createDataFrame(data_reshaped, string_schema)
                for c in spark_df.columns:
                    spark_df = spark_df.withColumn(
                        c, spark_df[c].cast(SPARK_TYPES[schema[c]]())
                    )
        elif len(data_reshaped) == 0:
            # if we have an empty dataset and no schema, need to assign an arbitrary type
            columns = list(data.keys())
            spark_schema = sparktypes.StructType(
                [
                    sparktypes.StructField(column, sparktypes.StringType())
                    for column in columns
                ]
            )
            spark_df = spark.createDataFrame(data_reshaped, spark_schema)
        else:
            # if no schema provided, uses Spark's schema inference
            columns = list(data.keys())
            spark_df = spark.createDataFrame(data_reshaped, columns)

        if table_name is None:
            # noinspection PyUnusedLocal
            table_name = "test_data_" + "".join(
                [random.choice(string.ascii_letters + string.digits) for _ in range(8)]
            )

        return build_spark_validator_with_data(df=spark_df, spark=spark)

    else:
        raise ValueError("Unknown dataset_type " + str(execution_engine))


def build_spark_validator_with_data(df: Union[pd.DataFrame, SparkDataFrame], spark: SparkSession) -> Validator:
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
    batch: Batch = Batch(data=df)
    execution_engine: SparkDFExecutionEngine = build_spark_engine(spark=spark, df=df, batch_id=batch.id)
    return Validator(execution_engine=execution_engine, batches=(batch,))


# Builds a Spark Execution Engine
def build_spark_engine(spark: SparkSession, df: Union[pd.DataFrame, SparkDataFrame], batch_id: str) -> SparkDFExecutionEngine:
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
    conf: List[tuple] = spark.sparkContext.getConf().getAll()
    spark_config: Dict[str, str] = dict(conf)
    execution_engine: SparkDFExecutionEngine = SparkDFExecutionEngine(
        spark_config=spark_config
    )
    execution_engine.load_batch_data(batch_id=batch_id, batch_data=df)
    return execution_engine


def build_sa_engine(df: pd.DataFrame) -> SqlAlchemyExecutionEngine:
    import sqlalchemy as sa

    table_name: str = "test"

    sqlalchemy_engine: sa.engine.Engine = sa.create_engine("sqlite://", echo=False)
    df.to_sql(table_name, sqlalchemy_engine)

    execution_engine: SqlAlchemyExecutionEngine

    execution_engine = SqlAlchemyExecutionEngine(engine=sqlalchemy_engine)
    batch_data: SqlAlchemyBatchData = SqlAlchemyBatchData(
        execution_engine=execution_engine, table_name=table_name
    )
    batch: Batch = Batch(data=batch_data)

    execution_engine = SqlAlchemyExecutionEngine(
        engine=sqlalchemy_engine, batch_data_dict={batch.id: batch_data}
    )

    return execution_engine


def build_pandas_engine(df: pd.DataFrame) -> PandasExecutionEngine:
    batch: Batch = Batch(data=df)
    execution_engine: PandasExecutionEngine = PandasExecutionEngine(batch_data_dict={batch.id: batch.data})
    return execution_engine


def build_pandas_validator_with_data(df: pd.DataFrame) -> Validator:
    batch: Batch = Batch(data=df)
    return Validator(execution_engine=PandasExecutionEngine(), batches=(batch,))
