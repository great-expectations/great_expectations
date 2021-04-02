import logging
import os
import random
import string

import pandas as pd
import pytest

import great_expectations as ge
from great_expectations.core.batch import Batch, BatchRequest, RuntimeBatchRequest
from great_expectations.core.util import get_or_create_spark_application
from great_expectations.data_context.util import file_relative_path
from great_expectations.execution_engine import SqlAlchemyExecutionEngine
from great_expectations.execution_engine.sqlalchemy_batch_data import (
    SqlAlchemyBatchData,
)
from great_expectations.profile.base import (
    OrderedProfilerCardinality,
    profiler_semantic_types,
)
from great_expectations.profile.user_configurable_profiler import (
    UserConfigurableProfiler,
)
from great_expectations.self_check.util import (
    connection_manager,
    get_sql_dialect_floating_point_infinity_value,
)
from great_expectations.util import is_library_loadable
from great_expectations.validator.validator import Validator
from tests.profile.conftest import get_set_of_columns_and_expectations_from_suite

logger = logging.getLogger(__name__)

try:
    import sqlalchemy as sqlalchemy
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
    sqlalchemy = None
    postgresqltypes = None
    POSTGRESQL_TYPES = {}

logger = logging.getLogger(__name__)


def get_pandas_runtime_validator(context, df):
    batch_request = RuntimeBatchRequest(
        datasource_name="my_pandas_runtime_datasource",
        data_connector_name="my_data_connector",
        data_asset_name="IN_MEMORY_DATA_ASSET",
        runtime_parameters={"batch_data": df},
        batch_identifiers={
            "an_example_key": "a",
            "another_example_key": "b",
        },
    )

    expectation_suite = context.create_expectation_suite(
        "my_suite", overwrite_existing=True
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite=expectation_suite
    )

    return validator


def get_spark_runtime_validator(context, df):
    spark = get_or_create_spark_application(
        spark_config={
            "spark.sql.catalogImplementation": "hive",
            "spark.executor.memory": "450m",
            # "spark.driver.allowMultipleContexts": "true",  # This directive does not appear to have any effect.
        }
    )
    df = spark.createDataFrame(df)
    batch_request = RuntimeBatchRequest(
        datasource_name="my_spark_datasource",
        data_connector_name="my_data_connector",
        data_asset_name="IN_MEMORY_DATA_ASSET",
        runtime_parameters={"batch_data": df},
        batch_identifiers={
            "an_example_key": "a",
            "another_example_key": "b",
        },
    )

    expectation_suite = context.create_expectation_suite(
        "my_suite", overwrite_existing=True
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite=expectation_suite
    )

    return validator


def get_sqlalchemy_runtime_validator_postgresql(
    df, schemas=None, caching=True, table_name=None
):
    sa_engine_name = "postgresql"
    db_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
    try:
        engine = connection_manager.get_engine(
            f"postgresql://postgres@{db_hostname}/test_ci"
        )
    except sqlalchemy.exc.OperationalError:
        return None

    sql_dtypes = {}

    if (
        schemas
        and sa_engine_name in schemas
        and isinstance(engine.dialect, postgresqltypes.dialect)
    ):
        schema = schemas[sa_engine_name]
        sql_dtypes = {col: POSTGRESQL_TYPES[dtype] for (col, dtype) in schema.items()}

        for col in schema:
            type_ = schema[col]
            if type_ in ["INTEGER", "SMALLINT", "BIGINT"]:
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
            elif type_ in ["DATETIME", "TIMESTAMP"]:
                df[col] = pd.to_datetime(df[col])

    if table_name is None:
        table_name = "test_data_" + "".join(
            [random.choice(string.ascii_letters + string.digits) for _ in range(8)]
        )
    df.to_sql(
        name=table_name,
        con=engine,
        index=False,
        dtype=sql_dtypes,
        if_exists="replace",
    )
    batch_data = SqlAlchemyBatchData(execution_engine=engine, table_name=table_name)
    batch = Batch(data=batch_data)
    execution_engine = SqlAlchemyExecutionEngine(caching=caching, engine=engine)
    batch_data = SqlAlchemyBatchData(
        execution_engine=execution_engine, table_name=table_name
    )
    batch = Batch(data=batch_data)

    return Validator(execution_engine=execution_engine, batches=(batch,))


@pytest.fixture
def titanic_validator(titanic_data_context_modular_api):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected
    """
    df = ge.read_csv(file_relative_path(__file__, "../test_sets/Titanic.csv"))

    return get_pandas_runtime_validator(titanic_data_context_modular_api, df)


@pytest.fixture
def taxi_validator_pandas(titanic_data_context_modular_api):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected
    """

    df = ge.read_csv(
        file_relative_path(__file__, "../test_sets/yellow_tripdata_sample_2019-01.csv"),
        parse_dates=["pickup_datetime", "dropoff_datetime"],
    )

    return get_pandas_runtime_validator(titanic_data_context_modular_api, df)


@pytest.fixture
def taxi_validator_spark(spark_session, titanic_data_context_modular_api):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected
    """
    df = ge.read_csv(
        file_relative_path(__file__, "../test_sets/yellow_tripdata_sample_2019-01.csv"),
        parse_dates=["pickup_datetime", "dropoff_datetime"],
    )
    return get_spark_runtime_validator(titanic_data_context_modular_api, df)


@pytest.fixture
def taxi_validator_sqlalchemy(sa, titanic_data_context_modular_api):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected
    """
    df = ge.read_csv(
        file_relative_path(__file__, "../test_sets/yellow_tripdata_sample_2019-01.csv"),
        parse_dates=["pickup_datetime", "dropoff_datetime"],
    )
    return get_sqlalchemy_runtime_validator_postgresql(df)


@pytest.fixture()
def nulls_validator(titanic_data_context_modular_api):
    df = pd.DataFrame(
        {
            "mostly_null": [i if i % 3 == 0 else None for i in range(0, 1000)],
            "mostly_not_null": [None if i % 3 == 0 else i for i in range(0, 1000)],
        }
    )

    validator = get_pandas_runtime_validator(titanic_data_context_modular_api, df)

    return validator


@pytest.fixture()
def cardinality_validator(titanic_data_context_modular_api):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected
    """
    df = pd.DataFrame(
        {
            # TODO: Uncomment assertions that use col_none when proportion_of_unique_values bug is fixed for columns
            #  that are all NULL/None
            # "col_none": [None for i in range(0, 1000)],
            "col_one": [0 for i in range(0, 1000)],
            "col_two": [i % 2 for i in range(0, 1000)],
            "col_very_few": [i % 10 for i in range(0, 1000)],
            "col_few": [i % 50 for i in range(0, 1000)],
            "col_many": [i % 100 for i in range(0, 1000)],
            "col_very_many": [i % 500 for i in range(0, 1000)],
            "col_unique": [i for i in range(0, 1000)],
        }
    )
    return get_pandas_runtime_validator(titanic_data_context_modular_api, df)


@pytest.fixture
def taxi_data_ignored_columns():
    return [
        "pickup_location_id",
        "dropoff_location_id",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "congestion_surcharge",
    ]


@pytest.fixture
def taxi_data_semantic_types():
    return {
        "datetime": ["pickup_datetime", "dropoff_datetime"],
        "numeric": ["total_amount", "passenger_count"],
        "value_set": [
            "payment_type",
            "rate_code_id",
            "store_and_fwd_flag",
            "passenger_count",
        ],
        "boolean": ["store_and_fwd_flag"],
    }


def test_profiler_init_no_config(
    cardinality_validator,
):
    """
    What does this test do and why?
    Confirms that profiler can initialize with no config.
    """
    profiler = UserConfigurableProfiler(cardinality_validator)
    assert profiler.primary_or_compound_key == []
    assert profiler.ignored_columns == []
    assert profiler.value_set_threshold == "MANY"
    assert not profiler.table_expectations_only
    assert profiler.excluded_expectations == []


def test_profiler_init_full_config_no_semantic_types(cardinality_validator):
    """
    What does this test do and why?
    Confirms that profiler initializes properly with a full config, without a semantic_types dict
    """

    profiler = UserConfigurableProfiler(
        cardinality_validator,
        primary_or_compound_key=["col_unique"],
        ignored_columns=["col_one"],
        value_set_threshold="UNIQUE",
        table_expectations_only=False,
        excluded_expectations=["expect_column_values_to_not_be_null"],
    )
    assert profiler.primary_or_compound_key == ["col_unique"]
    assert profiler.ignored_columns == [
        "col_one",
    ]
    assert profiler.value_set_threshold == "UNIQUE"
    assert not profiler.table_expectations_only
    assert profiler.excluded_expectations == ["expect_column_values_to_not_be_null"]

    assert "col_one" not in profiler.column_info


def test_init_with_semantic_types(cardinality_validator):
    """
    What does this test do and why?
    Confirms that profiler initializes properly with a full config and a semantic_types dict
    """

    semantic_types = {
        "numeric": ["col_few", "col_many", "col_very_many"],
        "value_set": ["col_two", "col_very_few"],
    }
    profiler = UserConfigurableProfiler(
        cardinality_validator,
        semantic_types_dict=semantic_types,
        primary_or_compound_key=["col_unique"],
        ignored_columns=["col_one"],
        value_set_threshold="unique",
        table_expectations_only=False,
        excluded_expectations=["expect_column_values_to_not_be_null"],
    )

    assert "col_one" not in profiler.column_info

    # assert profiler.column_info.get("col_none") == {
    #     "cardinality": "NONE",
    #     "type": "NUMERIC",
    #     "semantic_types": [],
    # }
    assert profiler.column_info.get("col_two") == {
        "cardinality": "TWO",
        "type": "INT",
        "semantic_types": ["VALUE_SET"],
    }
    assert profiler.column_info.get("col_very_few") == {
        "cardinality": "VERY_FEW",
        "type": "INT",
        "semantic_types": ["VALUE_SET"],
    }
    assert profiler.column_info.get("col_few") == {
        "cardinality": "FEW",
        "type": "INT",
        "semantic_types": ["NUMERIC"],
    }
    assert profiler.column_info.get("col_many") == {
        "cardinality": "MANY",
        "type": "INT",
        "semantic_types": ["NUMERIC"],
    }
    assert profiler.column_info.get("col_very_many") == {
        "cardinality": "VERY_MANY",
        "type": "INT",
        "semantic_types": ["NUMERIC"],
    }
    assert profiler.column_info.get("col_unique") == {
        "cardinality": "UNIQUE",
        "type": "INT",
        "semantic_types": [],
    }


def test__validate_config(cardinality_validator):
    """
    What does this test do and why?
    Tests the validate config function on the profiler
    """

    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_validator, ignored_columns="col_name")
    assert e.typename == "AssertionError"

    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(cardinality_validator, table_expectations_only="True")
    assert e.typename == "AssertionError"


def test__validate_semantic_types_dict(cardinality_validator):
    """
    What does this test do and why?
    Tests that _validate_semantic_types_dict function errors when not formatted correctly
    """

    bad_semantic_types_dict_type = {"value_set": "col_few"}
    with pytest.raises(AssertionError) as e:
        UserConfigurableProfiler(
            cardinality_validator, semantic_types_dict=bad_semantic_types_dict_type
        )
    assert e.value.args[0] == (
        "Entries in semantic type dict must be lists of column names e.g. "
        "{'semantic_types': {'numeric': ['number_of_transactions']}}"
    )

    bad_semantic_types_incorrect_type = {"incorrect_type": ["col_few"]}
    with pytest.raises(ValueError) as e:
        UserConfigurableProfiler(
            cardinality_validator, semantic_types_dict=bad_semantic_types_incorrect_type
        )
    assert e.value.args[0] == (
        f"incorrect_type is not a recognized semantic_type. Please only include one of "
        f"{profiler_semantic_types}"
    )

    # Error if column is specified for both semantic_types and ignored
    working_semantic_type = {"numeric": ["col_few"]}
    with pytest.raises(ValueError) as e:
        UserConfigurableProfiler(
            cardinality_validator,
            semantic_types_dict=working_semantic_type,
            ignored_columns=["col_few"],
        )
    assert e.value.args[0] == (
        f"Column col_few is specified in both the semantic_types_dict and the list of ignored columns. Please remove "
        f"one of these entries to proceed."
    )


def test_build_suite_no_config(titanic_validator, possible_expectations_set):
    """
    What does this test do and why?
    Tests that the build_suite function works as expected with no config
    """
    profiler = UserConfigurableProfiler(titanic_validator)
    suite = profiler.build_suite()
    expectations_from_suite = {i.expectation_type for i in suite.expectations}

    assert expectations_from_suite.issubset(possible_expectations_set)
    assert len(suite.expectations) == 48


def test_all_table_columns_populates(taxi_validator_pandas):
    taxi_profiler = UserConfigurableProfiler(taxi_validator_pandas)

    assert taxi_profiler.all_table_columns == [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
    ]


def test_profiler_works_with_batch_object(cardinality_validator):
    profiler = UserConfigurableProfiler(cardinality_validator.active_batch)
    assert profiler.primary_or_compound_key == []
    assert profiler.ignored_columns == []
    assert profiler.value_set_threshold == "MANY"
    assert not profiler.table_expectations_only
    assert profiler.excluded_expectations == []

    assert profiler.all_table_columns == [
        "col_one",
        "col_two",
        "col_very_few",
        "col_few",
        "col_many",
        "col_very_many",
        "col_unique",
    ]


def test_build_suite_with_config_and_no_semantic_types_dict(
    titanic_validator, possible_expectations_set
):
    """
    What does this test do and why?
    Tests that the build_suite function works as expected with a config and without a semantic_types dict
    """
    profiler = UserConfigurableProfiler(
        titanic_validator,
        ignored_columns=["Survived", "Unnamed: 0"],
        excluded_expectations=["expect_column_mean_to_be_between"],
        primary_or_compound_key=["Name"],
        table_expectations_only=False,
        value_set_threshold="very_few",
    )
    suite = profiler.build_suite()
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    columns_expected_in_suite = {"Name", "PClass", "Age", "Sex", "SexCode"}
    assert columns_with_expectations == columns_expected_in_suite
    assert expectations_from_suite.issubset(possible_expectations_set)
    assert "expect_column_mean_to_be_between" not in expectations_from_suite
    assert len(suite.expectations) == 29


def test_build_suite_with_semantic_types_dict(
    cardinality_validator,
    possible_expectations_set,
):
    """
    What does this test do and why?
    Tests that the build_suite function works as expected with a semantic_types dict
    """

    semantic_types = {
        "numeric": ["col_few", "col_many", "col_very_many"],
        "value_set": ["col_two", "col_very_few"],
    }

    profiler = UserConfigurableProfiler(
        cardinality_validator,
        semantic_types_dict=semantic_types,
        primary_or_compound_key=["col_unique"],
        ignored_columns=["col_one"],
        value_set_threshold="unique",
        table_expectations_only=False,
        excluded_expectations=["expect_column_values_to_not_be_null"],
    )
    suite = profiler.build_suite()
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    assert "column_one" not in columns_with_expectations
    assert "expect_column_values_to_not_be_null" not in expectations_from_suite
    assert expectations_from_suite.issubset(possible_expectations_set)
    assert len(suite.expectations) == 32

    value_set_expectations = [
        i
        for i in suite.expectations
        if i.expectation_type == "expect_column_values_to_be_in_set"
    ]
    value_set_columns = {i.kwargs.get("column") for i in value_set_expectations}

    assert len(value_set_columns) == 2
    assert value_set_columns == {"col_two", "col_very_few"}


def test_build_suite_when_suite_already_exists(cardinality_validator):
    """
    What does this test do and why?
    Confirms that creating a new suite on an existing profiler wipes the previous suite
    """
    profiler = UserConfigurableProfiler(
        cardinality_validator,
        table_expectations_only=True,
        excluded_expectations=["expect_table_row_count_to_be_between"],
    )

    suite = profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(suite)
    assert len(suite.expectations) == 1
    assert "expect_table_columns_to_match_ordered_list" in expectations

    profiler.excluded_expectations = ["expect_table_columns_to_match_ordered_list"]
    suite = profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(suite)
    assert len(suite.expectations) == 1
    assert "expect_table_row_count_to_be_between" in expectations


def test_primary_or_compound_key_not_found_in_columns(cardinality_validator):
    """
    What does this test do and why?
    Confirms that an error is raised if a primary_or_compound key is specified with a column not found in the validator
    """
    # regular case, should pass
    working_profiler = UserConfigurableProfiler(
        cardinality_validator, primary_or_compound_key=["col_unique"]
    )
    assert working_profiler.primary_or_compound_key == ["col_unique"]

    # key includes a non-existent column, should fail
    with pytest.raises(ValueError) as e:
        bad_key_profiler = UserConfigurableProfiler(
            cardinality_validator,
            primary_or_compound_key=["col_unique", "col_that_does_not_exist"],
        )
    assert e.value.args[0] == (
        f"Column col_that_does_not_exist not found. Please ensure that this column is in the Validator if "
        f"you would like to use it as a primary_or_compound_key."
    )

    # key includes a column that exists, but is in ignored_columns, should pass
    ignored_column_profiler = UserConfigurableProfiler(
        cardinality_validator,
        primary_or_compound_key=["col_unique", "col_one"],
        ignored_columns=["col_none", "col_one"],
    )
    assert ignored_column_profiler.primary_or_compound_key == ["col_unique", "col_one"]


def test_config_with_not_null_only(
    titanic_data_context_modular_api, nulls_validator, possible_expectations_set
):
    """
    What does this test do and why?
    Confirms that the not_null_only key in config works as expected.
    """

    excluded_expectations = [i for i in possible_expectations_set if "null" not in i]

    validator = nulls_validator

    profiler_without_not_null_only = UserConfigurableProfiler(
        validator, excluded_expectations, not_null_only=False
    )
    suite_without_not_null_only = profiler_without_not_null_only.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(
        suite_without_not_null_only
    )
    assert expectations == {
        "expect_column_values_to_be_null",
        "expect_column_values_to_not_be_null",
    }

    profiler_with_not_null_only = UserConfigurableProfiler(
        validator, excluded_expectations, not_null_only=True
    )
    not_null_only_suite = profiler_with_not_null_only.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(
        not_null_only_suite
    )
    assert expectations == {"expect_column_values_to_not_be_null"}

    no_config_profiler = UserConfigurableProfiler(validator)
    no_config_suite = no_config_profiler.build_suite()
    _, expectations = get_set_of_columns_and_expectations_from_suite(no_config_suite)
    assert "expect_column_values_to_be_null" in expectations


def test_nullity_expectations_mostly_tolerance(
    nulls_validator, possible_expectations_set
):
    excluded_expectations = [i for i in possible_expectations_set if "null" not in i]

    validator = nulls_validator

    profiler = UserConfigurableProfiler(
        validator, excluded_expectations, not_null_only=False
    )
    suite = profiler.build_suite()

    for i in suite.expectations:
        assert i["kwargs"]["mostly"] == 0.66


def test_profiled_dataset_passes_own_validation(
    cardinality_validator, titanic_data_context
):
    """
    What does this test do and why?
    Confirms that a suite created on a validator with no config will pass when validated against itself
    """
    context = titanic_data_context
    profiler = UserConfigurableProfiler(
        cardinality_validator, ignored_columns=["col_none"]
    )
    suite = profiler.build_suite()

    context.save_expectation_suite(suite)
    results = context.run_validation_operator(
        "action_list_operator", assets_to_validate=[cardinality_validator]
    )

    assert results["success"]


def test_column_cardinality_functions(cardinality_validator):
    profiler = UserConfigurableProfiler(cardinality_validator)
    # assert profiler.column_info.get("col_none").get("cardinality") == "NONE"
    assert profiler.column_info.get("col_one").get("cardinality") == "ONE"
    assert profiler.column_info.get("col_two").get("cardinality") == "TWO"
    assert profiler.column_info.get("col_very_few").get("cardinality") == "VERY_FEW"
    assert profiler.column_info.get("col_few").get("cardinality") == "FEW"
    assert profiler.column_info.get("col_many").get("cardinality") == "MANY"
    assert profiler.column_info.get("col_very_many").get("cardinality") == "VERY_MANY"

    cardinality_with_ten_num_and_no_pct = (
        OrderedProfilerCardinality.get_basic_column_cardinality(num_unique=10)
    )
    assert cardinality_with_ten_num_and_no_pct.name == "VERY_FEW"

    cardinality_with_unique_pct_and_no_num = (
        OrderedProfilerCardinality.get_basic_column_cardinality(pct_unique=1.0)
    )
    assert cardinality_with_unique_pct_and_no_num.name == "UNIQUE"

    cardinality_with_no_pct_and_no_num = (
        OrderedProfilerCardinality.get_basic_column_cardinality()
    )
    assert cardinality_with_no_pct_and_no_num.name == "NONE"

    cardinality_with_large_pct_and_no_num = (
        OrderedProfilerCardinality.get_basic_column_cardinality(pct_unique=0.5)
    )
    assert cardinality_with_large_pct_and_no_num.name == "NONE"


def test_profiler_all_expectation_types_pandas(
    titanic_data_context_modular_api,
    taxi_validator_pandas,
    possible_expectations_set,
    taxi_data_semantic_types,
    taxi_data_ignored_columns,
):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected for pandas
    """
    context = titanic_data_context_modular_api

    profiler = UserConfigurableProfiler(
        taxi_validator_pandas,
        semantic_types_dict=taxi_data_semantic_types,
        ignored_columns=taxi_data_ignored_columns,
        # TODO: Add primary_or_compound_key test
        #  primary_or_compound_key=[
        #     "vendor_id",
        #     "pickup_datetime",
        #     "dropoff_datetime",
        #     "trip_distance",
        #     "pickup_location_id",
        #     "dropoff_location_id",
        #  ],
    )

    assert profiler.column_info.get("rate_code_id")
    suite = profiler.build_suite()
    assert len(suite.expectations) == 45
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    unexpected_expectations = {
        "expect_column_values_to_be_unique",
        "expect_column_values_to_be_null",
        "expect_compound_columns_to_be_unique",
    }
    assert expectations_from_suite == {
        i for i in possible_expectations_set if i not in unexpected_expectations
    }

    ignored_included_columns_overlap = [
        i for i in columns_with_expectations if i in taxi_data_ignored_columns
    ]
    assert len(ignored_included_columns_overlap) == 0

    results = context.run_validation_operator(
        "action_list_operator", assets_to_validate=[taxi_validator_pandas]
    )

    assert results["success"]


@pytest.mark.skipif(
    not is_library_loadable(library_name="pyspark"),
    reason="requires pyspark to be installed",
)
def test_profiler_all_expectation_types_spark(
    titanic_data_context_modular_api,
    taxi_validator_spark,
    possible_expectations_set,
    taxi_data_semantic_types,
    taxi_data_ignored_columns,
):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected for spark
    """
    context = titanic_data_context_modular_api

    profiler = UserConfigurableProfiler(
        taxi_validator_spark,
        semantic_types_dict=taxi_data_semantic_types,
        ignored_columns=taxi_data_ignored_columns,
        # TODO: Add primary_or_compound_key test
        #  primary_or_compound_key=[
        #     "vendor_id",
        #     "pickup_datetime",
        #     "dropoff_datetime",
        #     "trip_distance",
        #     "pickup_location_id",
        #     "dropoff_location_id",
        #  ],
    )

    assert profiler.column_info.get("rate_code_id")
    suite = profiler.build_suite()
    assert len(suite.expectations) == 45
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    unexpected_expectations = {
        "expect_column_values_to_be_unique",
        "expect_column_values_to_be_null",
        "expect_compound_columns_to_be_unique",
    }
    assert expectations_from_suite == {
        i for i in possible_expectations_set if i not in unexpected_expectations
    }

    ignored_included_columns_overlap = [
        i for i in columns_with_expectations if i in taxi_data_ignored_columns
    ]
    assert len(ignored_included_columns_overlap) == 0

    results = context.run_validation_operator(
        "action_list_operator", assets_to_validate=[taxi_validator_spark]
    )

    assert results["success"]


@pytest.mark.skipif(
    not is_library_loadable(library_name="sqlalchemy"),
    reason="requires sqlalchemy to be installed",
)
def test_profiler_all_expectation_types_sqlalchemy(
    titanic_data_context_modular_api,
    taxi_validator_sqlalchemy,
    possible_expectations_set,
    taxi_data_semantic_types,
    taxi_data_ignored_columns,
):
    """
    What does this test do and why?
    Ensures that all available expectation types work as expected for sqlalchemy
    """
    if taxi_validator_sqlalchemy == None:
        pytest.skip("a message")

    context = titanic_data_context_modular_api

    profiler = UserConfigurableProfiler(
        taxi_validator_sqlalchemy,
        semantic_types_dict=taxi_data_semantic_types,
        ignored_columns=taxi_data_ignored_columns,
        # TODO: Add primary_or_compound_key test
        #  primary_or_compound_key=[
        #     "vendor_id",
        #     "pickup_datetime",
        #     "dropoff_datetime",
        #     "trip_distance",
        #     "pickup_location_id",
        #     "dropoff_location_id",
        #  ],
    )

    assert profiler.column_info.get("rate_code_id")
    suite = profiler.build_suite()
    assert len(suite.expectations) == 45
    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    unexpected_expectations = {
        "expect_column_values_to_be_unique",
        "expect_column_values_to_be_null",
        "expect_compound_columns_to_be_unique",
    }
    assert expectations_from_suite == {
        i for i in possible_expectations_set if i not in unexpected_expectations
    }

    ignored_included_columns_overlap = [
        i for i in columns_with_expectations if i in taxi_data_ignored_columns
    ]
    assert len(ignored_included_columns_overlap) == 0

    results = context.run_validation_operator(
        "action_list_operator", assets_to_validate=[taxi_validator_sqlalchemy]
    )

    assert results["success"]


def test_error_handling_for_expect_compound_columns_to_be_unique(
    taxi_validator_pandas, taxi_data_ignored_columns, caplog
):
    # TODO: When this expectation is implemented for V3, remove this test and test for this expectation
    ignored_columns = taxi_data_ignored_columns + [
        "pickup_datetime",
        "dropoff_datetime",
        "total_amount",
        "passenger_count",
        "payment_type",
        "rate_code_id",
        "store_and_fwd_flag",
        "passenger_count",
        "store_and_fwd_flag",
        "vendor_id",
        "trip_distance",
    ]

    profiler = UserConfigurableProfiler(
        taxi_validator_pandas,
        ignored_columns=ignored_columns,
        primary_or_compound_key=[
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "trip_distance",
            "pickup_location_id",
            "dropoff_location_id",
        ],
    )
    with caplog.at_level(logging.WARNING):
        suite = profiler.build_suite()

    log_warnings = caplog.messages
    assert len(log_warnings) == 1

    assert (
        log_warnings[0]
        == "expect_compound_columns_to_be_unique is not currently available in the V3 (Batch Request) API. Specifying a compound key will not add any expectations. This will be updated when that expectation becomes available."
    )

    assert len(suite.expectations) == 2

    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    expected_expectations = {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_row_count_to_be_between",
    }

    assert expected_expectations == expectations_from_suite

    profiler_with_single_column_key = UserConfigurableProfiler(
        taxi_validator_pandas,
        ignored_columns=ignored_columns,
        primary_or_compound_key=["pickup_datetime"],
    )

    suite = profiler_with_single_column_key.build_suite()

    assert len(suite.expectations) == 3

    (
        columns_with_expectations,
        expectations_from_suite,
    ) = get_set_of_columns_and_expectations_from_suite(suite)

    expected_expectations = {
        "expect_table_columns_to_match_ordered_list",
        "expect_table_row_count_to_be_between",
        "expect_column_values_to_be_unique",
    }

    assert expected_expectations == expectations_from_suite
