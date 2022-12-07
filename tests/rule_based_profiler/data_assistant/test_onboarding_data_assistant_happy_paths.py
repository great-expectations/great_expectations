import os
from typing import List

import pytest

import great_expectations as gx
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import BatchRequest
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path

yaml: YAMLHandler = YAMLHandler()
# constants used by the sql example
pg_hostname = os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost")
CONNECTION_STRING: str = f"postgresql+psycopg2://postgres:@{pg_hostname}/test_ci"


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
    data_context.save_expectation_suite(expectation_suite=suite)

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
    from pyspark.sql.types import StructType

    schema: StructType = spark_df_taxi_data_schema
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
    data_context.save_expectation_suite(expectation_suite=suite)
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
    data_context.save_expectation_suite(expectation_suite=suite)
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
    connection: sa.engine.Connection = engine.connect()

    # ensure we aren't appending to an existing table
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
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
    connection: sa.engine.Connection = engine.connect()

    # ensure we aren't appending to an existing table
    connection.execute(f"DROP TABLE IF EXISTS {table_name}")
    for data_path in data_paths:
        load_data_into_test_database(
            table_name=table_name,
            csv_path=data_path,
            connection_string=CONNECTION_STRING,
            load_full_dataset=True,
            drop_existing_table=False,
            convert_colnames_to_datetime=["pickup_datetime", "dropoff_datetime"],
        )
