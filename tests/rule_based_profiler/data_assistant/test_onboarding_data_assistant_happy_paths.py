import logging
import os
import sys
from typing import List

import pandas as pd
import pytest

import great_expectations as gx
from great_expectations import DataContext
from great_expectations.compatibility.sqlalchemy_compatibility_wrappers import (
    add_dataframe_to_db,
)
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path

yaml: YAMLHandler = YAMLHandler()

logger = logging.getLogger(__name__)

# constants used by the sql example
pg_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
CONNECTION_STRING: str = f"postgresql+psycopg2://postgres:@{pg_hostname}/test_ci"


@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 19s
def test_pandas_happy_path_onboarding_data_assistant(empty_data_context) -> None:
    """
    What does this test and why?

    The intent of this test is to ensure that our "happy path", exercised by notebooks
    in great_expectations/tests/test_fixtures/rule_based_profiler/example_notebooks/ are in working order

    The code in the notebooks (excluding explanations) and the code in the following test exercise an identical codepath.

    1. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    2. Configuring BatchRequest to load 2019 data as multiple batches
    3. Running Onboarding DataAssistant and saving resulting ExpectationSuite as 'taxi_data_2019_suite'
    4. Configuring BatchRequest to load 2020 January data
    5. Configuring and running Checkpoint using BatchRequest for 2020-01, and 'taxi_data_2019_suite'.

    This test tests the code in `DataAssistants_Instantiation_And_Running-OnboardingAssistant-Pandas.ipynb`

    """
    data_context: gx.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__, os.path.join("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    )

    datasource_config: dict = {
        "name": "taxi_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": taxi_data_path,
                "assets": {
                    "yellow_tripdata_2019": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2019)-(\\d.*)\\.csv",
                    },
                    "yellow_tripdata_2020": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2020)-(\\d.*)\\.csv",
                    },
                },
            },
        },
    }
    data_context.add_datasource(**datasource_config)

    # Batch Request
    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
    )
    batch_request: BatchRequest = multi_batch_batch_request
    batch_list = data_context.get_batch_list(batch_request=batch_request)
    assert len(batch_list) == 12

    # Running onboarding data assistant
    result = data_context.assistants.onboarding.run(
        batch_request=multi_batch_batch_request
    )

    # saving resulting ExpectationSuite
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    suite.add_expectation_configurations(
        expectation_configurations=result.expectation_configurations
    )
    data_context.add_expectation_suite(expectation_suite=suite)

    # batch_request for checkpoint
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2020",
        data_connector_query={
            "batch_filter_parameters": {"year": "2020", "month": "01"}
        },
    )

    # configuring and running checkpoint
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": single_batch_batch_request,
                "expectation_suite_name": "taxi_data_2019_suite",
            }
        ],
    }
    data_context.add_checkpoint(**checkpoint_config)
    results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    assert results.success is False


@pytest.mark.filesystem
@pytest.mark.spark
@pytest.mark.integration
@pytest.mark.slow  # 149 seconds
def test_spark_happy_path_onboarding_data_assistant(
    empty_data_context, spark_session, spark_df_taxi_data_schema
) -> None:
    """
    What does this test and why?

    The intent of this test is to ensure that our "happy path", exercised by notebooks
    in great_expectations/tests/test_fixtures/rule_based_profiler/example_notebooks/ are in working order

    The code in the notebooks (excluding explanations) and the code in the following test exercise an identical codepath.

    1. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    2. Configuring BatchRequest to load 2019 data as multiple batches
    3. Running Onboarding DataAssistant and saving resulting ExpectationSuite as 'taxi_data_2019_suite'
    4. Configuring BatchRequest to load 2020 January data
    5. Configuring and running Checkpoint using BatchRequest for 2020-01, and 'taxi_data_2019_suite'.

    This test tests the code in `DataAssistants_Instantiation_And_Running-OnboardingAssistant-Spark.ipynb`

    """
    from great_expectations.compatibility import pyspark

    schema: pyspark.types.StructType = spark_df_taxi_data_schema
    data_context: gx.DataContext = empty_data_context
    taxi_data_path: str = file_relative_path(
        __file__, os.path.join("..", "..", "test_sets", "taxi_yellow_tripdata_samples")
    )

    datasource_config: dict = {
        "name": "taxi_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SparkDFExecutionEngine",
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetFilesystemDataConnector",
                "base_directory": taxi_data_path,
                "assets": {
                    "yellow_tripdata_2019": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2019)-(\\d.*)\\.csv",
                    },
                    "yellow_tripdata_2020": {
                        "group_names": ["year", "month"],
                        "pattern": "yellow_tripdata_sample_(2020)-(\\d.*)\\.csv",
                    },
                },
            },
        },
    }
    data_context.add_datasource(**datasource_config)
    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2019",
        batch_spec_passthrough={
            "reader_method": "csv",
            "reader_options": {"header": True, "schema": schema},
        },
        data_connector_query={
            "batch_filter_parameters": {"year": "2019", "month": "01"}
        },
    )
    batch_request: BatchRequest = multi_batch_batch_request
    batch_list = data_context.get_batch_list(batch_request=batch_request)
    assert len(batch_list) == 1

    result = data_context.assistants.onboarding.run(
        batch_request=multi_batch_batch_request
    )
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    suite.add_expectation_configurations(
        expectation_configurations=result.expectation_configurations
    )
    data_context.add_expectation_suite(expectation_suite=suite)
    # batch_request for checkpoint
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_data",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_2020",
        data_connector_query={
            "batch_filter_parameters": {"year": "2020", "month": "01"}
        },
    )
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": single_batch_batch_request,
                "expectation_suite_name": "taxi_data_2019_suite",
            }
        ],
    }
    data_context.add_checkpoint(**checkpoint_config)
    results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    assert results.success is False


@pytest.mark.postgres
@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 104 seconds
def test_sql_happy_path_onboarding_data_assistant(
    empty_data_context, test_backends, sa
) -> None:
    """
    What does this test and why?

    The intent of this test is to ensure that our "happy path", exercised by notebooks
    in great_expectations/tests/test_fixtures/rule_based_profiler/example_notebooks/ are in working order

    The code in the notebooks (excluding explanations) and the code in the following test exercise an identical codepath.
    1. Loading tables into postgres Docker container by calling helper method load_data_into_postgres_database()
    2. Setting up Datasource to load 2019 taxi data and 2020 taxi data
    3. Configuring BatchRequest to load 2019 data as multiple batches
    4. Running Onboarding DataAssistant and saving resulting ExpectationSuite as 'taxi_data_2019_suite'
    5. Configuring BatchRequest to load 2020 January data
    6. Configuring and running Checkpoint using BatchRequest for 2020-01, and 'taxi_data_2019_suite'.

    This test tests the code in `DataAssistants_Instantiation_And_Running-OnboardingAssistant-Sql.ipynb`

    """
    if "postgresql" not in test_backends:
        pytest.skip("testing data assistant in sql requires postgres backend")
    else:
        load_data_into_postgres_database(sa)

    data_context: gx.DataContext = empty_data_context

    datasource_config = {
        "name": "taxi_multi_batch_sql_datasource",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "SqlAlchemyExecutionEngine",
            "connection_string": CONNECTION_STRING,
        },
        "data_connectors": {
            "configured_data_connector_multi_batch_asset": {
                "class_name": "ConfiguredAssetSqlDataConnector",
                "assets": {
                    "yellow_tripdata_sample_2019": {
                        "splitter_method": "split_on_year_and_month",
                        "splitter_kwargs": {
                            "column_name": "pickup_datetime",
                        },
                    },
                    "yellow_tripdata_sample_2020": {
                        "splitter_method": "split_on_year_and_month",
                        "splitter_kwargs": {
                            "column_name": "pickup_datetime",
                        },
                    },
                },
            },
        },
    }
    data_context.add_datasource(**datasource_config)

    multi_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multi_batch_sql_datasource",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_sample_2019",
    )

    batch_request: BatchRequest = multi_batch_batch_request
    batch_list = data_context.get_batch_list(batch_request=batch_request)
    assert len(batch_list) == 13

    result = data_context.assistants.onboarding.run(
        batch_request=multi_batch_batch_request
    )
    suite: ExpectationSuite = ExpectationSuite(
        expectation_suite_name="taxi_data_2019_suite"
    )
    suite.add_expectation_configurations(
        expectation_configurations=result.expectation_configurations
    )
    data_context.add_expectation_suite(expectation_suite=suite)
    # batch_request for checkpoint
    single_batch_batch_request: BatchRequest = BatchRequest(
        datasource_name="taxi_multi_batch_sql_datasource",
        data_connector_name="configured_data_connector_multi_batch_asset",
        data_asset_name="yellow_tripdata_sample_2020",
        data_connector_query={
            "batch_filter_parameters": {"pickup_datetime": {"year": 2020, "month": 1}},
        },
    )
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "config_version": 1,
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": single_batch_batch_request,
                "expectation_suite_name": "taxi_data_2019_suite",
            }
        ],
    }
    data_context.add_checkpoint(**checkpoint_config)
    results = data_context.run_checkpoint(checkpoint_name="my_checkpoint")
    assert results.success is False


@pytest.mark.skipif(sys.version_info < (3, 8), reason="requires Python3.8")
@pytest.mark.integration
@pytest.mark.filesystem
@pytest.mark.slow  # 6.54 seconds
def test_sql_happy_path_onboarding_data_assistant_null_column_quantiles_metric_values(
    sa,
    empty_data_context,
) -> None:
    context: DataContext = empty_data_context

    db_file = file_relative_path(
        __file__,
        os.path.join(
            "..",
            "..",
            "test_sets",
            "taxi_yellow_tripdata_samples",
            "sqlite",
            "yellow_tripdata.db",
        ),
    )

    datasource = context.sources.add_sqlite(
        name="test_datasource",
        connection_string=f"sqlite:///{db_file}",
    )

    table_name = "yellow_tripdata_sample_2019_01"
    split_col = "pickup_datetime"
    asset = (
        datasource.add_table_asset(
            name="my_asset",
            table_name=table_name,
        )
        .add_splitter_year_and_month(column_name=split_col)
        .add_sorters(["year", "month"])
    )

    batch_request = asset.build_batch_request({"year": 2019, "month": 1})

    result = context.assistants.onboarding.run(
        batch_request=batch_request,
        numeric_columns_rule={
            "estimator": "exact",
            "random_seed": 2022080401,
        },
    )
    assert len(result.metrics_by_domain) == 36
    assert len(result.expectation_configurations) == 122


@pytest.mark.postgres
@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 26.57 seconds
def test_sql_happy_path_onboarding_data_assistant_mixed_decimal_float_and_boolean_column_unique_proportion_metric_values(
    empty_data_context,
    test_backends,
    sa,
) -> None:
    if "postgresql" not in test_backends:
        pytest.skip("testing data assistant in sql requires postgres backend")

    context: DataContext = empty_data_context
    postgresql_engine: sa.engine.Engine = sa.create_engine(CONNECTION_STRING)
    # noinspection PyUnusedLocal
    conn: sa.engine.Connection = postgresql_engine.connect()

    table_name = "sampled_yellow_tripdata_test"

    try:
        csv_path: str = file_relative_path(
            __file__,
            os.path.join(
                "..",
                "..",
                "test_sets",
                "taxi_yellow_tripdata_samples",
                "samples_2021",
                "yellow_tripdata_sample_2021.csv",
            ),
        )
        df: pd.DataFrame = pd.read_csv(filepath_or_buffer=csv_path)
        df["test_bool"] = df.apply(
            lambda row: True if row["test_bool"] == "t" else False, axis=1
        )
        add_dataframe_to_db(
            df=df,
            name=table_name,
            con=conn,
            schema="public",
            index=False,
            dtype={
                "VendorID": sa.types.BIGINT(),
                "tpep_pickup_datetime": sa.DateTime(),
                "tpep_dropoff_datetime": sa.DateTime(),
                "passenger_count": sa.types.INTEGER(),
                "trip_distance": sa.types.NUMERIC(),
                "RatecodeID": sa.types.INTEGER(),
                "store_and_fwd_flag": sa.types.TEXT(),
                "PULocationID": sa.types.BIGINT(),
                "DOLocationID": sa.types.BIGINT(),
                "payment_type": sa.types.BIGINT(),
                "fare_amount": sa.types.NUMERIC(),
                "extra": sa.types.NUMERIC(),
                "mta_tax": sa.types.NUMERIC(),
                "tip_amount": sa.types.NUMERIC(),
                "tolls_amount": sa.types.NUMERIC(),
                "improvement_surcharge": sa.types.NUMERIC(),
                "total_amount": sa.types.NUMERIC(),
                "congestion_surcharge": sa.types.NUMERIC(),
                "test_bool": sa.types.Boolean(),
            },
            if_exists="replace",
        )
    except ValueError as ve:
        logger.warning(f"Unable to store information into database: {str(ve)}")

    batch_options = {"year": 2021}

    data_asset = (
        context.sources.add_postgres(
            name="postgres_demo_datasource",
            connection_string=CONNECTION_STRING,
        )
        .add_table_asset(
            name="sampled_yellow_tripdata_test",
            table_name=table_name,
        )
        .add_splitter_year_and_month(column_name="tpep_pickup_datetime")
        .add_sorters(["year", "-month"])
    )

    batch_request = data_asset.build_batch_request(batch_options)

    result = context.assistants.onboarding.run(
        batch_request=batch_request,
        numeric_columns_rule={
            "estimator": "exact",
            "random_seed": 2022080401,
        },
    )
    assert len(result.metrics_by_domain) == 49
    assert len(result.expectation_configurations) == 171
    assert list(
        filter(
            lambda element: element.expectation_type
            == "expect_column_proportion_of_unique_values_to_be_between"
            and element.kwargs["column"] == "test_bool",
            result.get_expectation_suite(
                send_usage_event=False
            ).get_column_expectations(),
        )
    )[0].kwargs == {
        "column": "test_bool",
        "min_value": 0.001002004008016032,
        "strict_max": False,
        "max_value": 0.5,
        "strict_min": False,
    }


def load_data_into_postgres_database(sa):
    """
    Method to load our 2019 and 2020 taxi data into a postgres database.  This is a helper method
    called by test_sql_happy_path_onboarding_data_assistant().
    """

    from tests.test_utils import load_data_into_test_database

    data_paths: List[str] = [
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-02.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-03.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-04.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-05.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-06.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-07.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-08.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-09.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-10.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-11.csv",
        ),
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-12.csv",
        ),
    ]
    table_name: str = "yellow_tripdata_sample_2019"

    engine: sa.engine.Engine = sa.create_engine(CONNECTION_STRING)

    # ensure we aren't appending to an existing table
    with engine.begin() as connection:
        connection.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))

    for data_path in data_paths:
        load_data_into_test_database(
            table_name=table_name,
            csv_path=data_path,
            connection_string=CONNECTION_STRING,
            load_full_dataset=True,
            drop_existing_table=False,
            convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        )

    # 2020 data
    data_paths: List[str] = [
        file_relative_path(
            __file__,
            "../../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-01.csv",
        )
    ]
    table_name: str = "yellow_tripdata_sample_2020"

    engine: sa.engine.Engine = sa.create_engine(CONNECTION_STRING)

    # ensure we aren't appending to an existing table
    with engine.begin() as connection:
        connection.execute(sa.text(f"DROP TABLE IF EXISTS {table_name}"))

    for data_path in data_paths:
        load_data_into_test_database(
            table_name=table_name,
            csv_path=data_path,
            connection_string=CONNECTION_STRING,
            load_full_dataset=True,
            drop_existing_table=False,
            convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        )
