import os
import shutil
from typing import Dict, List

import boto3
import pandas as pd
import pytest
from moto import mock_sns

import great_expectations.expectations as gxe
from great_expectations.core import (
    ExpectationSuite,
    ExpectationSuiteValidationResult,
    RunIdentifier,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import get_context
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.types.base import DataContextConfig
from great_expectations.data_context.types.resource_identifiers import (
    BatchIdentifier,
    ExpectationSuiteIdentifier,
    GXCloudIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.datasource.fluent import BatchRequest as FluentBatchRequest


@pytest.fixture
def update_data_docs_action():
    return {
        "name": "update_data_docs",
        "action": {"class_name": "UpdateDataDocsAction"},
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
    update_data_docs_action: dict,
) -> List[dict]:
    return [
        store_validation_result_action,
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
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    # create expectation suite
    suite = context.suites.add(ExpectationSuite("my_expectation_suite"))
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="col1",
        min_value=1,
        max_value=2,
    )
    suite.add_expectation(expectation=expectation)
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture
def titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

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
    suite = context.suites.add(ExpectationSuite("my_expectation_suite"))
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="col1",
        min_value=1,
        max_value=2,
    )
    suite.add_expectation(expectation)
    # noinspection PyProtectedMember
    context._save_project_config()

    return context


@pytest.fixture
def titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

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
    suite = context.suites.add(ExpectationSuite("my_expectation_suite"))
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="col1",
        min_value=1,
        max_value=2,
    )
    suite.add_expectation(expectation)
    # noinspection PyProtectedMember
    context._save_project_config()

    return context


@pytest.fixture
def context_with_single_taxi_csv_spark(empty_data_context, tmp_path_factory, spark_session):
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
    shutil.copy(taxi_csv_source_file_path_test_data, taxi_csv_destination_file_path_test_data)

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
    suite = context.suites.add(ExpectationSuite("my_expectation_suite"))
    expectation = gxe.ExpectColumnToExist(
        column="pickup_datetime",
    )
    suite.add_expectation(expectation)
    # noinspection PyProtectedMember
    context._save_project_config()
    return context


@pytest.fixture(scope="module")
def basic_data_context_config_for_validation_operator():
    return DataContextConfig(
        config_version=2,
        plugins_directory=None,
        evaluation_parameter_store_name="evaluation_parameter_store",
        expectations_store_name="expectations_store",
        checkpoint_store_name="checkpoint_store",
        datasources={},
        stores={
            "expectations_store": {"class_name": "ExpectationsStore"},
            "checkpoint_store": {"class_name": "CheckpointStore"},
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "validation_result_store": {"class_name": "ValidationsStore"},
            "metrics_store": {"class_name": "MetricStore"},
        },
        validations_store_name="validation_result_store",
        data_docs_sites={},
        validation_operators={
            "store_val_res_and_extract_eval_params": {
                "class_name": "ActionListValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                            "target_store_name": "validation_result_store",
                        },
                    },
                ],
            },
            "errors_and_warnings_validation_operator": {
                "class_name": "WarningAndFailureExpectationSuitesValidationOperator",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                            "target_store_name": "validation_result_store",
                        },
                    },
                ],
            },
        },
    )


@pytest.fixture(scope="module")
def basic_in_memory_data_context_for_validation_operator(
    basic_data_context_config_for_validation_operator,
):
    return get_context(basic_data_context_config_for_validation_operator)


@pytest.fixture(scope="module")
def checkpoint_ge_cloud_id():
    return "bfe7dc64-5320-49b0-91c1-2e8029e06c4d"


@pytest.fixture(scope="module")
def validation_result_suite_ge_cloud_id():
    return "bfe7dc64-5320-49b0-91c1-2e8029e06c4d"


@pytest.fixture(scope="module")
def validation_result_suite():
    return ExpectationSuiteValidationResult(
        results=[],
        success=True,
        suite_name="empty_suite",
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.8.0__develop",
            "expectation_suite_name": "asset.default",
            "run_id": "test_100",
        },
    )


@pytest.fixture(scope="module")
def validation_result_suite_ge_cloud_identifier(validation_result_suite_ge_cloud_id):
    return GXCloudIdentifier(
        resource_type=GXCloudRESTResource.CHECKPOINT,
        id=validation_result_suite_ge_cloud_id,
    )


@pytest.fixture(scope="module")
def validation_result_suite_with_ge_cloud_id(validation_result_suite_ge_cloud_id):
    return ExpectationSuiteValidationResult(
        results=[],
        success=True,
        suite_name="empty_suite",
        statistics={
            "evaluated_expectations": 0,
            "successful_expectations": 0,
            "unsuccessful_expectations": 0,
            "success_percent": None,
        },
        meta={
            "great_expectations_version": "v0.8.0__develop",
            "expectation_suite_name": "asset.default",
            "run_id": "test_100",
        },
        id=validation_result_suite_ge_cloud_id,
    )


@pytest.fixture(scope="module")
def validation_result_suite_id():
    return ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(run_name="test_100"),
        batch_identifier="1234",
    )


@pytest.fixture(scope="module")
def validation_result_suite_extended_id():
    return ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.default"),
        run_id=RunIdentifier(run_name="test_100", run_time="Tue May 08 15:14:45 +0800 2012"),
        batch_identifier=BatchIdentifier(batch_identifier="1234", data_asset_name="asset"),
    )


@pytest.fixture
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture
def sns(aws_credentials):
    with mock_sns():
        conn = boto3.client("sns")
        yield conn
