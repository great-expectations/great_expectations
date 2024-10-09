from typing import Dict, List

import pandas as pd
import pytest

import great_expectations.expectations as gxe
from great_expectations.core import ExpectationSuite
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
    datasource = context.data_sources.get(datasource_name=datasource_name)

    batching_regex = r"^Titanic_1911\.csv"
    glob_directive = "*.csv"
    datasource.add_csv_asset(
        name="Titanic_1911",
        batching_regex=batching_regex,
        glob_directive=glob_directive,
    )

    datasource_name = "my_pandas_dataframes_datasource"
    datasource = context.data_sources.get(datasource_name=datasource_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    asset = datasource.add_dataframe_asset(name="my_other_dataframe_asset")
    _ = asset.build_batch_request(options={"dataframe": test_df})

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
    datasource = context.data_sources.get(datasource_name=datasource_name)

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
