import copy
from typing import Dict, List, Union

import pytest

from great_expectations.checkpoint.checkpoint import (
    Checkpoint,
    CheckpointResult,
    SimpleCheckpoint,
)
from great_expectations.core import ExpectationValidationResult
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.resource_identifiers import (
    ValidationResultIdentifier,
)
from great_expectations.datasource.fluent import BatchRequest as FluentBatchRequest
from great_expectations.render import RenderedAtomicContent
from great_expectations.util import deep_filter_properties_iterable


@pytest.fixture
def context_with_data_source_and_empty_suite(
    titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_with_checkpoints_v1_with_empty_store_stats_enabled
    context.add_expectation_suite(expectation_suite_name="one")
    return context


@pytest.fixture
def simple_checkpoint_defaults(
    context_with_data_source_and_empty_suite,
) -> SimpleCheckpoint:
    return SimpleCheckpoint(
        name="foo", data_context=context_with_data_source_and_empty_suite
    )


@pytest.fixture
def one_validation(
    batch_request_as_dict: Dict[str, str]
) -> Dict[str, Union[str, Dict[str, str]]]:
    return {
        "batch_request": batch_request_as_dict,
        "expectation_suite_name": "one",
    }


@pytest.fixture
def two_validations(
    one_validation: Dict[str, Union[str, Dict[str, str]]]
) -> List[Dict[str, Union[str, Dict[str, str]]]]:
    second_validation: Dict[str, Union[str, Dict[str, str]]] = copy.deepcopy(
        one_validation
    )
    second_validation["expectation_suite_name"] = "two"
    return [
        one_validation,
        second_validation,
    ]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_defaults_run_and_basic_run_params_without_persisting_checkpoint(
    simple_checkpoint_defaults, one_validation
):
    result = simple_checkpoint_defaults.run(
        run_name="bar",
        validations=[one_validation],
    )
    assert isinstance(result, CheckpointResult)
    assert result.run_id.run_name == "bar"
    assert result.list_expectation_suite_names() == ["one"]
    assert len(result.list_validation_results()) == 1
    assert result.success


@pytest.mark.filesystem
@pytest.mark.slow  # 1.09s
def test_simple_checkpoint_runtime_kwargs_processing_site_names_only_without_persisting_checkpoint(
    simple_checkpoint_defaults,
    common_action_list,
    batch_request_as_dict,
    one_validation,
):
    expected_runtime_kwargs: dict = {
        "name": "foo",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "module_name": "great_expectations.checkpoint",
        "template_name": None,
        "run_name_template": None,
        "expectation_suite_name": None,
        "batch_request": None,
        "action_list": common_action_list,
        "evaluation_parameters": None,
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": batch_request_as_dict,
                "expectation_suite_name": "one",
            },
        ],
        "profilers": None,
    }

    result: CheckpointResult = simple_checkpoint_defaults.run(
        run_name="bar",
        validations=[one_validation],
        site_names=["local_site"],
    )
    assert isinstance(result, CheckpointResult)
    assert result.run_id.run_name == "bar"
    assert result.list_expectation_suite_names() == ["one"]
    assert len(result.list_validation_results()) == 1
    assert result.success

    substituted_runtime_config: dict = (
        simple_checkpoint_defaults.get_substituted_config(
            runtime_kwargs=expected_runtime_kwargs
        )
    )
    assert deep_filter_properties_iterable(
        properties=substituted_runtime_config,
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=expected_runtime_kwargs,
        clean_falsy=True,
    )


@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 1.23s
def test_simple_checkpoint_runtime_kwargs_processing_all_kwargs(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    simple_checkpoint_defaults,
    one_validation,
    batch_request_as_dict,
    slack_notification_action,
    common_action_list,
    monkeypatch,
):
    monkeypatch.setenv("GE_ENVIRONMENT", "my_ge_environment")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("VAR", "test")

    partial_batch_request_0: dict = {
        "datasource_name": "my_pandas_filesystem_datasource",
    }

    partial_batch_request_1: dict = {
        "data_asset_name": "my_dataframe_asset",
    }

    partial_batch_request_2: dict = {
        "options": {
            "timestamp": "19120414",
            "size": "1313",
        },
    }

    expected_runtime_kwargs: dict = {
        "name": "foo",
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "module_name": "great_expectations.checkpoint",
        "template_name": "my_simple_template_checkpoint",
        "run_name_template": "my_runtime_run_name_template",
        "expectation_suite_name": "my_runtime_suite",
        "batch_request": partial_batch_request_1,
        "action_list": common_action_list + [slack_notification_action],
        "evaluation_parameters": {
            "aux_param_0": "1",
            "aux_param_1": "1 + 1",
            "environment": "my_ge_environment",
            "my_runtime_key": "my_runtime_value",
            "tolerance": 0.01,
        },
        "runtime_configuration": {
            "my_runtime_key": "my_runtime_value",
            "result_format": {
                "result_format": "BASIC",
                "partial_unexpected_count": 20,
            },
        },
        "validations": [
            {
                "batch_request": partial_batch_request_0,
                "expectation_suite_name": "one",
            }
        ],
        "profilers": None,
    }

    result: CheckpointResult = simple_checkpoint_defaults.run(
        run_name="bar",
        template_name="my_simple_template_checkpoint",
        run_name_template="my_runtime_run_name_template",
        expectation_suite_name="my_runtime_suite",
        batch_request=partial_batch_request_2,
        validations=[one_validation],
        evaluation_parameters={"my_runtime_key": "my_runtime_value"},
        runtime_configuration={"my_runtime_key": "my_runtime_value"},
        site_names=["local_site"],
        notify_with=["local_site"],
        notify_on="failure",
        slack_webhook="https://hooks.slack.com/my_slack_webhook.geocities",
    )
    assert isinstance(result, CheckpointResult)
    assert result.run_id.run_name == "bar"
    assert result.list_expectation_suite_names() == ["one"]
    assert len(result.list_validation_results()) == 1
    assert result.success

    substituted_runtime_config: dict = (
        simple_checkpoint_defaults.get_substituted_config(
            runtime_kwargs=expected_runtime_kwargs
        )
    )
    assert deep_filter_properties_iterable(
        properties=substituted_runtime_config,
        clean_falsy=True,
    ) == deep_filter_properties_iterable(
        properties=expected_runtime_kwargs,
        clean_falsy=True,
    )


@pytest.mark.filesystem
@pytest.mark.slow  # 1.12s
def test_simple_checkpoint_defaults_run_with_top_level_batch_request_and_suite(
    simple_checkpoint_defaults,
    fluent_batch_request,
):
    result = simple_checkpoint_defaults.run(
        run_name="bar",
        batch_request=fluent_batch_request,
        expectation_suite_name="one",
        validations=[{"expectation_suite_name": "one"}],
    )
    assert isinstance(result, CheckpointResult)
    assert result.success
    assert len(result.run_results) == 1


@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 1.61s
def test_simple_checkpoint_defaults_run_multiple_validations_without_persistence(
    context_with_data_source_and_empty_suite,
    simple_checkpoint_defaults,
    two_validations,
):
    context_with_data_source_and_empty_suite.add_expectation_suite(
        expectation_suite_name="two"
    )
    assert len(context_with_data_source_and_empty_suite.list_expectation_suites()) == 2
    result = simple_checkpoint_defaults.run(
        run_name="bar",
        validations=two_validations,
    )
    assert isinstance(result, CheckpointResult)
    assert result.run_id.run_name == "bar"
    assert sorted(result.list_expectation_suite_names()) == sorted(["one", "two"])
    assert len(result.list_validation_results()) == 2
    assert result.success


@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 1.62s
def test_simple_checkpoint_defaults_run_multiple_validations_with_persisted_checkpoint_loaded_from_store(
    context_with_data_source_and_empty_suite,
    simple_checkpoint_defaults,
    two_validations,
):
    context: FileDataContext = context_with_data_source_and_empty_suite
    context.add_expectation_suite(expectation_suite_name="two")
    assert len(context.list_expectation_suites()) == 2

    # persist to store
    checkpoint_class_args: dict = simple_checkpoint_defaults.get_config(
        mode=ConfigOutputModes.JSON_DICT
    )
    context.add_checkpoint(**checkpoint_class_args)
    checkpoint_name = simple_checkpoint_defaults.name
    assert context.list_checkpoints() == [checkpoint_name]
    # reload from store
    del simple_checkpoint_defaults
    checkpoint: Checkpoint = context.get_checkpoint(checkpoint_name)
    result = checkpoint.run(run_name="bar", validations=two_validations)
    assert isinstance(result, CheckpointResult)
    assert result.run_id.run_name == "bar"
    assert sorted(result.list_expectation_suite_names()) == sorted(["one", "two"])
    assert len(result.list_validation_results()) == 2
    assert result.success


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_single_dataframe_batch_request_in_validations_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_single_dataframe_batch_request_data_in_validations_sparkdf(
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates

    batch_request: dict = {
        "datasource_name": "my_spark_filesystem_datasource",
        "data_asset_name": "users",
    }

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_single_batch_request_sql_asset_object_in_validations_sqlalchemy(
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: dict = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[{"batch_request": batch_request}],
    )

    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_multiple_batch_request_sql_asset_objects_in_validations_sqlalchemy(
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request_0: dict = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    batch_request_1: dict = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_10",
    }

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        validations=[
            {"batch_request": batch_request_0},
            {"batch_request": batch_request_1},
        ],
    )

    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 2
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_batch_request_in_top_level_batch_request_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_batch_request_in_top_level_batch_request_sparkdf(
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_spark_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_sql_asset_object_in_top_level_batch_request_sqlalchemy(
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_sqlite_datasource",
        data_asset_name="table_partitioned_by_date_column__A_query_asset_limit_5",
    )

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
        batch_request=batch_request,
    )

    result = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_batch_request_in_checkpoint_run_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_batch_request_in_checkpoint_run_sparkdf(
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_spark_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_sql_asset_object_in_checkpoint_run_sqlalchemy(
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_sqlite_datasource",
        data_asset_name="table_partitioned_by_date_column__A_query_asset_limit_5",
    )

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_validations_in_checkpoint_run_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_validations_in_checkpoint_run_sparkdf(
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_runtime_validations_batch_request_sql_asset_object_in_checkpoint_run_sqlalchemy(
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: dict = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(validations=[{"batch_request": batch_request}])

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_batch_request_in_context_run_checkpoint_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint", batch_request=batch_request
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_batch_request_in_context_run_checkpoint_sparkdf(
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_spark_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint", batch_request=batch_request
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_batch_request_sql_asset_object_in_context_run_checkpoint_sqlalchemy(
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_sqlite_datasource",
        data_asset_name="table_partitioned_by_date_column__A_query_asset_limit_5",
    )

    # add checkpoint config
    checkpoint_config_dict: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config_dict)

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint", batch_request=batch_request
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_validations_in_context_run_checkpoint_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint", validations=[{"batch_request": batch_request}]
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_dataframe_validations_in_context_run_checkpoint_sparkdf(
    titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_spark_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: dict = {
        "datasource_name": "my_spark_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint", validations=[{"batch_request": batch_request}]
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_validation_result_when_run_validations_batch_request_sql_asset_object_in_context_run_checkpoint_sqlalchemy(
    titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
    sa,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_and_sqlite_datasources_with_checkpoints_v1_with_empty_store_stats_enabled

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request = {
        "datasource_name": "my_sqlite_datasource",
        "data_asset_name": "table_partitioned_by_date_column__A_query_asset_limit_5",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
    }

    context.add_checkpoint(**checkpoint_config)

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint", validations=[{"batch_request": batch_request}]
    )

    assert len(context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.filesystem
@pytest.mark.integration
def test_simple_checkpoint_instantiates_and_produces_a_printable_validation_result_with_dataframe_batch_request_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates,
    common_action_list,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_with_checkpoints_v1_with_templates

    # create expectation suite
    context.add_expectation_suite(expectation_suite_name="my_expectation_suite")

    batch_request: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_dataframe_asset",
    )

    # add checkpoint config
    checkpoint: SimpleCheckpoint = SimpleCheckpoint(
        name="my_checkpoint",
        data_context=context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(batch_request=batch_request)

    assert type(repr(result)) == str


@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 1.73s
def test_simple_checkpoint_instantiates_and_produces_a_correct_validation_result_dataframe_batch_request_in_checkpoint_yml_and_checkpoint_run_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    batch_request_0: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    batch_request_1: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_other_dataframe_asset",
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request_0,
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_checkpoint(name="my_checkpoint")

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

    result = checkpoint.run(batch_request=batch_request_1)
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


@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 2.32s
def test_simple_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_checkpoint_run_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    batch_request_0: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    batch_request_1: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_other_dataframe_asset",
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request_0}],
    }

    context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = context.get_checkpoint(name="my_checkpoint")

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

    result = checkpoint.run(validations=[{"batch_request": batch_request_1}])
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
@pytest.mark.integration
@pytest.mark.slow  # 1.87s
def test_simple_checkpoint_instantiates_and_produces_a_correct_validation_result_batch_request_in_checkpoint_yml_and_context_run_checkpoint_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    batch_request_0: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    batch_request_1: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_other_dataframe_asset",
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "batch_request": batch_request_0,
    }

    context.add_checkpoint(**checkpoint_config)

    result = context.run_checkpoint(checkpoint_name="my_checkpoint")
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

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint", batch_request=batch_request_1
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


@pytest.mark.filesystem
@pytest.mark.integration
@pytest.mark.slow  # 2.44s
def test_simple_checkpoint_instantiates_and_produces_a_correct_validation_result_validations_in_checkpoint_yml_and_context_run_checkpoint_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    batch_request_0: dict = {
        "datasource_name": "my_pandas_dataframes_datasource",
        "data_asset_name": "my_dataframe_asset",
    }

    batch_request_1: FluentBatchRequest = FluentBatchRequest(
        datasource_name="my_pandas_dataframes_datasource",
        data_asset_name="my_other_dataframe_asset",
    )

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "SimpleCheckpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": common_action_list,
        "validations": [{"batch_request": batch_request_0}],
    }

    context.add_checkpoint(**checkpoint_config)

    result = context.run_checkpoint(checkpoint_name="my_checkpoint")
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

    result = context.run_checkpoint(
        checkpoint_name="my_checkpoint",
        validations=[{"batch_request": batch_request_1}],
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
@pytest.mark.integration
@pytest.mark.slow  # 1.16s
def test_simple_checkpoint_result_validations_include_rendered_content_pandasdf(
    titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
    common_action_list,
    batch_request_as_dict,
):
    context: FileDataContext = titanic_data_context_with_fluent_pandas_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    expectation_suite_name = "my_expectation_suite"
    include_rendered_content = True

    # add checkpoint config
    checkpoint_config = {
        "class_name": "SimpleCheckpoint",
        "validations": [
            {
                "batch_request": batch_request_as_dict,
                "expectation_suite_name": expectation_suite_name,
                "include_rendered_content": include_rendered_content,
            }
        ],
    }
    checkpoint = SimpleCheckpoint(
        name="my_checkpoint", data_context=context, **checkpoint_config
    )

    result: CheckpointResult = checkpoint.run()

    validation_result_identifier: ValidationResultIdentifier = (
        result.list_validation_result_identifiers()[0]
    )
    expectation_validation_result: ExpectationValidationResult | dict = (
        result.run_results[validation_result_identifier]["validation_result"]
    )
    for result in expectation_validation_result.results:
        for rendered_content in result.rendered_content:
            assert isinstance(rendered_content, RenderedAtomicContent)
