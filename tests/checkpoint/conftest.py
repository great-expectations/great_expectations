import os
import shutil

import pytest

from great_expectations import DataContext
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path


@pytest.fixture
def titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    # create expectation suite
    suite = context.create_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "col1", "min_value": 1, "max_value": 2},
    )
    suite.add_expectation(expectation, send_usage_event=False)
    context.save_expectation_suite(suite)
    return context


@pytest.fixture
def titanic_spark_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled(
    tmp_path_factory,
    monkeypatch,
    spark_session,
):
    # Re-enable GE_USAGE_STATS
    monkeypatch.delenv("GE_USAGE_STATS")

    project_path: str = str(tmp_path_factory.mktemp("titanic_data_context"))
    context_path: str = os.path.join(project_path, "great_expectations")
    os.makedirs(os.path.join(context_path, "expectations"), exist_ok=True)
    data_path: str = os.path.join(context_path, "..", "data", "titanic")
    os.makedirs(os.path.join(data_path), exist_ok=True)
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(
                "..",
                "test_fixtures",
                "great_expectations_v013_no_datasource_stats_enabled.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(
            os.path.join(
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(
            os.path.join(context_path, "..", "data", "titanic", "Titanic_19120414_1313")
        ),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1911.csv")),
    )
    shutil.copy(
        file_relative_path(__file__, os.path.join("..", "test_sets", "Titanic.csv")),
        str(os.path.join(context_path, "..", "data", "titanic", "Titanic_1912.csv")),
    )

    context = DataContext(context_root_dir=context_path)
    assert context.root_directory == context_path

    datasource_config: str = f"""
        class_name: Datasource

        execution_engine:
            class_name: SparkDFExecutionEngine

        data_connectors:
            my_basic_data_connector:
                class_name: InferredAssetFilesystemDataConnector
                base_directory: {data_path}
                default_regex:
                    pattern: (.*)\\.csv
                    group_names:
                        - data_asset_name

            my_special_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users:
                        base_directory: {data_path}
                        pattern: (.+)_(\\d+)_(\\d+)\\.csv
                        group_names:
                            - name
                            - timestamp
                            - size

            my_other_data_connector:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {data_path}
                glob_directive: "*.csv"

                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                        - name
                assets:
                    users: {{}}

            my_runtime_data_connector:
                module_name: great_expectations.datasource.data_connector
                class_name: RuntimeDataConnector
                batch_identifiers:
                    - pipeline_stage_name
                    - airflow_run_id
    """

    # noinspection PyUnusedLocal
    context.test_yaml_config(
        name="my_datasource", yaml_config=datasource_config, pretty_print=False
    )
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def context_with_single_taxi_csv_spark(
    empty_data_context, tmp_path_factory, spark_session
):
    context = empty_data_context

    yaml = YAMLHandler()

    base_directory = str(tmp_path_factory.mktemp("test_checkpoint_spark"))
    taxi_asset_base_directory_path: str = os.path.join(base_directory, "data")
    os.makedirs(taxi_asset_base_directory_path)

    # training data
    taxi_csv_source_file_path_training_data: str = file_relative_path(
        __file__,
        "../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
    )
    taxi_csv_destination_file_path_training_data: str = str(
        os.path.join(base_directory, "data/yellow_tripdata_sample_2019-01.csv")
    )
    shutil.copy(
        taxi_csv_source_file_path_training_data,
        taxi_csv_destination_file_path_training_data,
    )

    # test data
    taxi_csv_source_file_path_test_data: str = file_relative_path(
        __file__,
        "../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2020-01.csv",
    )
    taxi_csv_destination_file_path_test_data: str = str(
        os.path.join(base_directory, "data/yellow_tripdata_sample_2020-01.csv")
    )
    shutil.copy(
        taxi_csv_source_file_path_test_data, taxi_csv_destination_file_path_test_data
    )

    config = yaml.load(
        f"""
        class_name: Datasource
        execution_engine:
            class_name: SparkDFExecutionEngine
        data_connectors:
            configured_data_connector_multi_batch_asset:
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {taxi_asset_base_directory_path}
                assets:
                    yellow_tripdata_2019:
                        pattern: yellow_tripdata_sample_(2019)-(\\d.*)\\.csv
                        group_names:
                            - year
                            - month
                    yellow_tripdata_2020:
                        pattern: yellow_tripdata_sample_(2020)-(\\d.*)\\.csv
                        group_names:
                            - year
                            - month
            """,
    )

    context.add_datasource(
        "my_datasource",
        **config,
    )
    return context


@pytest.fixture
def context_with_single_csv_spark_and_suite(
    context_with_single_taxi_csv_spark,
):
    context: DataContext = context_with_single_taxi_csv_spark
    # create expectation suite
    suite = context.create_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": "pickup_datetime"},
    )
    suite.add_expectation(expectation, send_usage_event=False)
    context.save_expectation_suite(suite)
    return context
