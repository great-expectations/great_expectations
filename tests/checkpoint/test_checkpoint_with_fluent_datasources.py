from __future__ import annotations

import copy
import logging

import pytest

import great_expectations.expectations as gxe
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import (
    ExpectationSuite,
)
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.datasource.fluent.batch_request import (
    BatchRequest as FluentBatchRequest,
)
from great_expectations.render import RenderedAtomicContent

yaml = YAMLHandler()

logger = logging.getLogger(__name__)


@pytest.mark.filesystem
@pytest.mark.slow  # 1.25s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run(
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    data_context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501
    # add checkpoint config
    checkpoint_config = CheckpointConfig(
        name="my_checkpoint",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
            },
        ],
    )
    checkpoint_config_key = ConfigurationIdentifier(configuration_key=checkpoint_config.name)
    data_context.checkpoint_store.set(key=checkpoint_config_key, value=checkpoint_config)
    checkpoint: Checkpoint = data_context.get_legacy_checkpoint(checkpoint_config.name)

    data_context.suites.add(ExpectationSuite("my_expectation_suite"))
    result = checkpoint.run()

    assert len(data_context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.slow  # 1.19s
@pytest.mark.filesystem
def test_newstyle_checkpoint_result_validations_include_rendered_content(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    data_context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request: dict = {
        "datasource_name": "my_pandas_filesystem_datasource",
        "data_asset_name": "Titanic_1911",
    }

    include_rendered_content: bool = True

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [
            {
                "batch_request": batch_request,
                "include_rendered_content": include_rendered_content,
            },
        ],
    }

    data_context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = data_context.get_legacy_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()
    validation_result_identifier: ValidationResultIdentifier = (
        result.list_validation_result_identifiers()[0]
    )
    expectation_validation_result: ExpectationValidationResult | dict = result.run_results[
        validation_result_identifier
    ]["validation_result"]
    for result in expectation_validation_result.results:
        for rendered_content in result.rendered_content:
            assert isinstance(rendered_content, RenderedAtomicContent)


@pytest.mark.filesystem
@pytest.mark.slow  # 1.22s
def test_newstyle_checkpoint_result_validations_include_rendered_content_data_context_variable(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
):
    data_context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request: dict = {
        "datasource_name": "my_pandas_filesystem_datasource",
        "data_asset_name": "Titanic_1911",
    }

    data_context.include_rendered_content.globally = True

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [
            {
                "batch_request": batch_request,
            },
        ],
    }

    data_context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = data_context.get_legacy_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()
    validation_result_identifier: ValidationResultIdentifier = (
        result.list_validation_result_identifiers()[0]
    )
    expectation_validation_result: ExpectationValidationResult | dict = result.run_results[
        validation_result_identifier
    ]["validation_result"]
    for result in expectation_validation_result.results:
        for rendered_content in result.rendered_content:
            assert isinstance(rendered_content, RenderedAtomicContent)


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_pandasdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    batch_request = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_sparkdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    batch_request = FluentBatchRequest(
        datasource_name="my_spark_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_validations_batch_request_sql_asset_in_checkpoint_run_sqlalchemy(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
@pytest.mark.slow  # 1.31s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_multi_validation_pandasdf_and_sparkdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)

    batch_request_1: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    batch_request_2: dict = {
        "datasource_name": "my_spark_filesystem_datasource",
        "data_asset_name": "users",
    }

    batch_request_3: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    context.suites.add(ExpectationSuite("my_expectation_suite"))
    # noinspection PyUnusedLocal
    result = checkpoint.run(
        validations=[
            {"batch_request": batch_request_0},
            {"batch_request": batch_request_1},
            {"batch_request": batch_request_2},
            {"batch_request": batch_request_3},
        ]
    )

    assert len(context.validation_results_store.list_keys()) == 4
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_multi_validation_batch_request_sql_asset_objects_in_validations_sqlalchemy(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request_0 = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    batch_request_1 = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_10",
    }

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[
            {"batch_request": batch_request_0},
            {"batch_request": batch_request_1},
        ],
    )

    result = checkpoint.run()

    assert len(context.validation_results_store.list_keys()) == 2
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_batch_data_in_top_level_batch_request_pandasdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_batch_data_in_top_level_batch_request_sparkdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_sql_asset_in_top_level_batch_request_sqlalchemy(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_sqlite_datasource",
        data_asset_name="table_partitioned_by_date_column__A_query_asset_limit_5",
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        batch_request=batch_request,
    )

    result = checkpoint.run()

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_sql_asset_in_checkpoint_run_sqlalchemy(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_sqlite_datasource",
        data_asset_name="table_partitioned_by_date_column__A_query_asset_limit_5",
    )

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_dataframe_asset_in_context_run_checkpoint_pandasdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_dataframe_asset_in_context_run_checkpoint_sparkdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_sql_asset_in_context_run_checkpoint_sqlalchemy(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_sqlite_datasource",
        data_asset_name="table_partitioned_by_date_column__A_query_asset_limit_5",
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_validations_batch_request_dataframe_in_context_run_checkpoint_pandasdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": batch_request}],
    )

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_validations_batch_request_dataframe_in_context_run_checkpoint_sparkdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": batch_request}],
    )

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_validations_batch_request_sql_asset_in_context_run_checkpoint_sqlalchemy(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[{"batch_request": batch_request}],
    )

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_printable_validation_result_with_batch_request_dataframe_pandasdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert type(repr(result)) == str  # noqa: E721


@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_printable_validation_result_with_batch_request_dataframe_sparkdf(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert type(repr(result)) == str  # noqa: E721


@pytest.mark.slow  # 1.75s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_batch_request_in_checkpoint_yml_and_checkpoint_run_pandas(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request_0,
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result = checkpoint.run()
    assert not result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )
    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))

    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0,
        max_value=71,
    )
    suite.add_expectation(expectation)

    result = checkpoint.run(
        expectation_suite_name="my_new_expectation_suite",
        batch_request=batch_request_1,
    )
    assert result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 1.75s
@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_batch_request_in_checkpoint_yml_and_checkpoint_run_spark(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request_0,
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result = checkpoint.run()
    assert not result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0,
        max_value=71,
    )
    suite.add_expectation(expectation)

    result = checkpoint.run(
        expectation_suite_name="my_new_expectation_suite",
        batch_request=batch_request_1,
    )
    assert result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 2.35s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_checkpoint_run_pandas(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request_0}],
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result = checkpoint.run()
    assert result["success"] is False
    assert len(result.run_results.values()) == 1
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))
    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0,
        max_value=71,
    )
    suite.add_expectation(expectation)

    result = checkpoint.run(
        validations=[
            {
                "batch_request": batch_request_1,
                "expectation_suite_name": "my_new_expectation_suite",
            }
        ],
    )
    assert result["success"] is False
    assert len(result.run_results.values()) == 2
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 2.35s
@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_checkpoint_run_spark(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request_0}],
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_legacy_checkpoint(name="my_checkpoint")

    result = checkpoint.run()
    assert result["success"] is False
    assert len(result.run_results.values()) == 1
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))

    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0,
        max_value=71,
    )
    suite.add_expectation(expectation)

    result = checkpoint.run(
        validations=[
            {
                "batch_request": batch_request_1,
                "expectation_suite_name": "my_new_expectation_suite",
            }
        ],
    )
    assert result["success"] is False
    assert len(result.run_results.values()) == 2
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 1.91s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_batch_request_in_checkpoint_yml_and_context_run_checkpoint_pandas(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request_0,
    }

    context.add_checkpoint(**checkpoint_config)

    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))

    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0.0,
        max_value=71.0,
    )
    suite.add_expectation(expectation)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run()
    assert result["success"] is False
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        batch_request=batch_request_1,
        expectation_suite_name="my_new_expectation_suite",
    )
    assert result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 1.91s
@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_batch_request_in_checkpoint_yml_and_context_run_checkpoint_spark(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request_0,
    }

    context.add_checkpoint(**checkpoint_config)

    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))

    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0.0,
        max_value=71.0,
    )
    suite.add_expectation(expectation)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run()
    assert result["success"] is False
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        batch_request=batch_request_1,
        expectation_suite_name="my_new_expectation_suite",
    )
    assert result["success"]
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 2.46s
@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_context_run_checkpoint_pandas(  # noqa: E501
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request_0}],
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run()
    assert result["success"] is False
    assert len(result.run_results.values()) == 1
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))

    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0.0,
        max_value=71.0,
    )
    suite.add_expectation(expectation)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[
            {
                "batch_request": batch_request_1,
                "expectation_suite_name": "my_new_expectation_suite",
            }
        ],
    )
    assert result["success"] is False
    assert len(result.run_results.values()) == 2
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.slow  # 2.46s
@pytest.mark.spark
def test_newstyle_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_context_run_checkpoint_spark(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_and_expectation_suite_with_one_expectation  # noqa: E501

    batch_request_0: dict = copy.deepcopy(batch_request_as_dict)
    batch_request_0["options"] = {
        "timestamp": "19120414",
        "size": "1313",
    }

    batch_request_1: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "name": "my_checkpoint",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request_0}],
    }

    context.add_checkpoint(**checkpoint_config)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run()
    assert result["success"] is False
    assert len(result.run_results.values()) == 1
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )

    suite = context.suites.add(ExpectationSuite("my_new_expectation_suite"))

    expectation = gxe.ExpectColumnValuesToBeBetween(
        column="Age",
        min_value=0.0,
        max_value=71.0,
    )
    suite.add_expectation(expectation)

    checkpoint = context.get_legacy_checkpoint("my_checkpoint")
    result = checkpoint.run(
        validations=[
            {
                "batch_request": batch_request_1,
                "expectation_suite_name": "my_new_expectation_suite",
            }
        ],
    )
    assert result["success"] is False
    assert len(result.run_results.values()) == 2
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[0]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 0
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "evaluated_expectations"
        ]
        == 1
    )
    assert (
        list(result.run_results.values())[1]["validation_result"]["statistics"][
            "successful_expectations"
        ]
        == 1
    )


@pytest.mark.filesystem
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_single_runtime_batch_request_sql_asset_in_validations_sqlalchemy(  # noqa: E501
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled  # noqa: E501

    # create expectation suite
    context.suites.add(ExpectationSuite("my_expectation_suite"))

    batch_request = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=context,
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[{"batch_request": batch_request}],
    )

    result = checkpoint.run()

    assert len(context.validation_results_store.list_keys()) == 1
    assert result["success"]
