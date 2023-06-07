import os
import shutil
from typing import Dict, List

import pandas as pd
import pytest

from great_expectations.core import ExpectationConfiguration
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.fluent import BatchRequest as FluentBatchRequest
from great_expectations.util import get_context


@pytest.fixture
def update_data_docs_action():
    return {
        "name": "update_data_docs",
        "action": {"class_name": "UpdateDataDocsAction"},
    }


@pytest.fixture
def store_eval_parameter_action():
    return {
        "name": "store_evaluation_params",
        "action": {"class_name": "StoreEvaluationParametersAction"},
    }


@pytest.fixture
def store_validation_result_action():
    return {
        "name": "store_validation_result",
        "action": {"class_name": "StoreValidationResultAction"},
    }


@pytest.fixture
def webhook() -> str:
    return "https://hooks.slack.com/foo/bar"


@pytest.fixture
def slack_notification_action(webhook):
    return {
        "name": "send_slack_notification",
        "action": {
            "class_name": "SlackNotificationAction",
            "slack_webhook": webhook,
            "notify_on": "all",
            "notify_with": None,
            "renderer": {
                "module_name": "great_expectations.render.renderer.slack_renderer",
                "class_name": "SlackRenderer",
            },
        },
    }


@pytest.fixture
def common_action_list(
    store_validation_result_action: dict,
    store_eval_parameter_action: dict,
    update_data_docs_action: dict,
) -> List[dict]:
    return [
        store_validation_result_action,
        store_eval_parameter_action,
        update_data_docs_action,
    ]


@pytest.fixture
def batch_request_as_dict() -> Dict[str, str]:
    return {
        "datasource_name": "my_pandas_filesystem_datasource",
        "data_asset_name": "users",
    }


@pytest.fixture
def fluent_batch_request(batch_request_as_dict: Dict[str, str]) -> FluentBatchRequest:
    return FluentBatchRequest(
        datasource_name=batch_request_as_dict["datasource_name"],
        data_asset_name=batch_request_as_dict["data_asset_name"],
    )


@pytest.fixture
def titanic_pandas_data_context_stats_enabled_and_expectation_suite_with_one_expectation(
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    # create expectation suite
    suite = context.add_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "col1", "min_value": 1, "max_value": 2},
    )
    suite.add_expectation(expectation, send_usage_event=False)
    context.update_expectation_suite(expectation_suite=suite)
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation(
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled

    datasource_name = "my_pandas_filesystem_datasource"
    datasource = context.get_datasource(datasource_name=datasource_name)

    batching_regex = r"^Titanic_1911\.csv"
    glob_directive = "*.csv"
    datasource.add_csv_asset(
        name="Titanic_1911",
        batching_regex=batching_regex,
        glob_directive=glob_directive,
    )

    datasource_name = "my_pandas_dataframes_datasource"
    datasource = context.get_datasource(datasource_name=datasource_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    asset = datasource.add_dataframe_asset(name="my_other_dataframe_asset")
    _ = asset.build_batch_request(dataframe=test_df)

    # create expectation suite
    suite = context.add_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "col1", "min_value": 1, "max_value": 2},
    )
    suite.add_expectation(expectation, send_usage_event=False)
    context.update_expectation_suite(expectation_suite=suite)
    # noinspection PyProtectedMember
    context._save_project_config()

    return context


@pytest.fixture
def titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation(
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled

    datasource_name = "my_pandas_filesystem_datasource"
    datasource = context.get_datasource(datasource_name=datasource_name)

    batching_regex = r"^Titanic_1911\.csv"
    glob_directive = "*.csv"
    datasource.add_csv_asset(
        name="Titanic_1911",
        batching_regex=batching_regex,
        glob_directive=glob_directive,
    )

    # create expectation suite
    suite = context.add_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_values_to_be_between",
        kwargs={"column": "col1", "min_value": 1, "max_value": 2},
    )
    suite.add_expectation(expectation, send_usage_event=False)
    context.update_expectation_suite(expectation_suite=suite)
    # noinspection PyProtectedMember
    context._save_project_config()

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
    context_path: str = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    os.makedirs(  # noqa: PTH103
        os.path.join(context_path, "expectations"), exist_ok=True  # noqa: PTH118
    )
    data_path: str = os.path.join(context_path, "..", "data", "titanic")  # noqa: PTH118
    os.makedirs(os.path.join(data_path), exist_ok=True)  # noqa: PTH103, PTH118
    shutil.copy(
        file_relative_path(
            __file__,
            os.path.join(  # noqa: PTH118
                "..",
                "test_fixtures",
                "great_expectations_v013_no_datasource_stats_enabled.yml",
            ),
        ),
        str(os.path.join(context_path, "great_expectations.yml")),  # noqa: PTH118
    )
    shutil.copy(
        file_relative_path(
            __file__, os.path.join("..", "test_sets", "Titanic.csv")  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_19120414_1313.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__, os.path.join("..", "test_sets", "Titanic.csv")  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_19120414_1313"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__, os.path.join("..", "test_sets", "Titanic.csv")  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1911.csv"
            )
        ),
    )
    shutil.copy(
        file_relative_path(
            __file__, os.path.join("..", "test_sets", "Titanic.csv")  # noqa: PTH118
        ),
        str(
            os.path.join(  # noqa: PTH118
                context_path, "..", "data", "titanic", "Titanic_1912.csv"
            )
        ),
    )

    context = get_context(context_root_dir=context_path)
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
    taxi_asset_base_directory_path: str = os.path.join(  # noqa: PTH118
        base_directory, "data"
    )
    os.makedirs(taxi_asset_base_directory_path)  # noqa: PTH103

    # training data
    taxi_csv_source_file_path_training_data: str = file_relative_path(
        __file__,
        "../test_sets/taxi_yellow_tripdata_samples/yellow_tripdata_sample_2019-01.csv",
    )
    taxi_csv_destination_file_path_training_data: str = str(
        os.path.join(  # noqa: PTH118
            base_directory, "data/yellow_tripdata_sample_2019-01.csv"
        )
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
        os.path.join(  # noqa: PTH118
            base_directory, "data/yellow_tripdata_sample_2020-01.csv"
        )
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

    # noinspection PyProtectedMember
    context._save_project_config()

    return context


@pytest.fixture
def context_with_single_csv_spark_and_suite(
    context_with_single_taxi_csv_spark,
):
    context = context_with_single_taxi_csv_spark
    # create expectation suite
    suite = context.add_expectation_suite("my_expectation_suite")
    expectation = ExpectationConfiguration(
        expectation_type="expect_column_to_exist",
        kwargs={"column": "pickup_datetime"},
    )
    suite.add_expectation(expectation, send_usage_event=False)
    context.update_expectation_suite(expectation_suite=suite)
    # noinspection PyProtectedMember
    context._save_project_config()
    return context
