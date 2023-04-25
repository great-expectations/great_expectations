import logging
import pickle
import unittest
from typing import Dict, List, Optional, TYPE_CHECKING
from unittest import mock

import pytest

import great_expectations as gx
import great_expectations.exceptions as gx_exceptions
from great_expectations.checkpoint import Checkpoint
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.datasource.fluent.interfaces import (
    BatchRequest as FluentBatchRequest,
)
from great_expectations.core.config_peer import ConfigOutputModes
from great_expectations.core.expectation_validation_result import (
    ExpectationValidationResult,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context import FileDataContext
from great_expectations.data_context.types.base import (
    CheckpointConfig,
    checkpointConfigSchema,
)
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.render import RenderedAtomicContent
from great_expectations.util import (
    filter_properties_dict,
)
from great_expectations.validator.validator import Validator

if TYPE_CHECKING:
    from great_expectations.core.data_context_key import DataContextKey


yaml = YAMLHandler()

logger = logging.getLogger(__name__)


@pytest.fixture
def batch_request_as_dict() -> Dict[str, str]:
    return {
        "datasource_name": "my_pandas_filesystem_datasource",
        "data_asset_name": "users",
    }


@pytest.fixture
def common_action_list() -> List[dict]:
    return [
        {
            "name": "store_validation_result",
            "action": {
                "class_name": "StoreValidationResultAction",
            },
        },
        {
            "name": "store_evaluation_params",
            "action": {
                "class_name": "StoreEvaluationParametersAction",
            },
        },
        {
            "name": "update_data_docs",
            "action": {
                "class_name": "UpdateDataDocsAction",
            },
        },
    ]


@pytest.mark.integration
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_checkpoint_configuration_no_nesting_using_test_yaml_config(
    mock_emit,
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    monkeypatch,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    checkpoint: Checkpoint

    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    yaml_config: str = """
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_pandas_filesystem_datasource
          data_asset_name: users
        expectation_suite_name: users.delivery
        action_list:
            - name: store_validation_result
              action:
                class_name: StoreValidationResultAction
            - name: store_evaluation_params
              action:
                class_name: StoreEvaluationParametersAction
            - name: update_data_docs
              action:
                class_name: UpdateDataDocsAction
    evaluation_parameters:
      param1: "$MY_PARAM"
      param2: 1 + "$OLD_PARAM"
    runtime_configuration:
      result_format:
        result_format: BASIC
        partial_unexpected_count: 20
    """

    expected_checkpoint_config: dict = {
        "name": "my_fancy_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
                "expectation_suite_name": "users.delivery",
                "action_list": [
                    {
                        "name": "store_validation_result",
                        "action": {"class_name": "StoreValidationResultAction"},
                    },
                    {
                        "name": "store_evaluation_params",
                        "action": {"class_name": "StoreEvaluationParametersAction"},
                    },
                    {
                        "name": "update_data_docs",
                        "action": {"class_name": "UpdateDataDocsAction"},
                    },
                ],
            },
        ],
        "evaluation_parameters": {"param1": "1", "param2": '1 + "2"'},
        "runtime_configuration": {
            "result_format": {
                "result_format": "BASIC",
                "partial_unexpected_count": 20,
            },
        },
        "template_name": None,
        "run_name_template": "%Y-%M-foo-bar-template-test",
        "expectation_suite_name": None,
        "batch_request": None,
        "action_list": [],
        "profilers": [],
    }

    checkpoint: Checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_fancy_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.get_config(mode=ConfigOutputModes.DICT),
        clean_falsy=True,
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
        clean_falsy=True,
    )

    # Test usage stats messages
    assert mock_emit.call_count == 1

    # Substitute current anonymized name since it changes for each run
    anonymized_checkpoint_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]

    # noinspection PyUnresolvedReferences
    expected_events: List[unittest.mock._Call] = [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_checkpoint_name,
                    "parent_class": "Checkpoint",
                },
                "success": True,
            },
        ),
    ]
    # noinspection PyUnresolvedReferences
    actual_events: List[unittest.mock._Call] = mock_emit.call_args_list
    assert actual_events == expected_events

    assert len(data_context.list_checkpoints()) == 0
    data_context.add_checkpoint(**yaml.load(yaml_config))
    assert len(data_context.list_checkpoints()) == 1

    data_context.add_expectation_suite(expectation_suite_name="users.delivery")
    result: CheckpointResult = data_context.run_checkpoint(
        checkpoint_name=checkpoint.name,
    )
    assert len(result.list_validation_results()) == 1
    assert len(data_context.validations_store.list_keys()) == 1
    assert result.success

    data_context.delete_checkpoint(name="my_fancy_checkpoint")
    assert len(data_context.list_checkpoints()) == 0


@pytest.mark.integration
@pytest.mark.slow  # 1.74s
def test_checkpoint_configuration_nesting_provides_defaults_for_most_elements_test_yaml_config(
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    monkeypatch,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    checkpoint: Checkpoint

    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    yaml_config: str = """
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    validations:
      - batch_request:
          datasource_name: my_pandas_filesystem_datasource
          data_asset_name: users
      - batch_request:
          datasource_name: my_pandas_filesystem_datasource
          data_asset_name: exploration
    expectation_suite_name: users.delivery
    action_list:
        - name: store_validation_result
          action:
            class_name: StoreValidationResultAction
        - name: store_evaluation_params
          action:
            class_name: StoreEvaluationParametersAction
        - name: update_data_docs
          action:
            class_name: UpdateDataDocsAction
    evaluation_parameters:
      param1: "$MY_PARAM"
      param2: 1 + "$OLD_PARAM"
    runtime_configuration:
      result_format:
        result_format: BASIC
        partial_unexpected_count: 20
    """

    expected_checkpoint_config: dict = {
        "name": "my_fancy_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
            },
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "exploration",
                },
            },
        ],
        "expectation_suite_name": "users.delivery",
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
        "evaluation_parameters": {"param1": "1", "param2": '1 + "2"'},
        "runtime_configuration": {
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20},
        },
        "template_name": None,
        "run_name_template": "%Y-%M-foo-bar-template-test",
        "batch_request": None,
        "profilers": [],
    }

    checkpoint: Checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_fancy_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.get_config(mode=ConfigOutputModes.DICT),
        clean_falsy=True,
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
        clean_falsy=True,
    )

    assert len(data_context.list_checkpoints()) == 0
    data_context.add_checkpoint(**yaml.load(yaml_config))
    assert len(data_context.list_checkpoints()) == 1

    data_context.add_expectation_suite(expectation_suite_name="users.delivery")
    result: CheckpointResult = data_context.run_checkpoint(
        checkpoint_name=checkpoint.name,
    )
    assert len(result.list_validation_results()) == 2
    assert len(data_context.validations_store.list_keys()) == 2
    assert result.success

    data_context.delete_checkpoint(name="my_fancy_checkpoint")
    assert len(data_context.list_checkpoints()) == 0


@pytest.mark.integration
@pytest.mark.slow  # 1.75s
def test_checkpoint_configuration_warning_error_quarantine_test_yaml_config(
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    monkeypatch,
):
    monkeypatch.setenv("GE_ENVIRONMENT", "my_ge_environment")

    checkpoint: Checkpoint

    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    yaml_config: str = """
    name: airflow_users_node_3
    config_version: 1
    class_name: Checkpoint
    batch_request:
        datasource_name: my_pandas_filesystem_datasource
        data_asset_name: users
    validations:
      - expectation_suite_name: users.warning  # runs the top-level action list against the top-level batch_request
      - expectation_suite_name: users.error  # runs the locally-specified action_list union the top level action-list against the top-level batch_request
        action_list:
        - name: quarantine_failed_data
          action:
              class_name: CreateQuarantineData
        - name: advance_passed_data
          action:
              class_name: CreatePassedData
    action_list:
        - name: store_validation_result
          action:
            class_name: StoreValidationResultAction
        - name: store_evaluation_params
          action:
            class_name: StoreEvaluationParametersAction
        - name: update_data_docs
          action:
            class_name: UpdateDataDocsAction
    evaluation_parameters:
        environment: $GE_ENVIRONMENT
        tolerance: 0.01
    runtime_configuration:
        result_format:
          result_format: BASIC
          partial_unexpected_count: 20
    """

    mock_create_quarantine_data = mock.MagicMock()
    mock_create_quarantine_data.run.return_value = True
    # noinspection PyUnresolvedReferences
    gx.validation_operators.CreateQuarantineData = mock_create_quarantine_data

    mock_create_passed_data = mock.MagicMock()
    mock_create_passed_data.run.return_value = True
    # noinspection PyUnresolvedReferences
    gx.validation_operators.CreatePassedData = mock_create_passed_data

    expected_checkpoint_config: dict = {
        "name": "airflow_users_node_3",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "batch_request": {
            "datasource_name": "my_pandas_filesystem_datasource",
            "data_asset_name": "users",
        },
        "validations": [
            {"expectation_suite_name": "users.warning"},
            {
                "expectation_suite_name": "users.error",
                "action_list": [
                    {
                        "name": "quarantine_failed_data",
                        "action": {"class_name": "CreateQuarantineData"},
                    },
                    {
                        "name": "advance_passed_data",
                        "action": {"class_name": "CreatePassedData"},
                    },
                ],
            },
        ],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
        "evaluation_parameters": {
            "environment": "my_ge_environment",
            "tolerance": 0.01,
        },
        "runtime_configuration": {
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20},
        },
        "template_name": None,
        "run_name_template": None,
        "expectation_suite_name": None,
        "profilers": [],
    }

    checkpoint: Checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="airflow_users_node_3",
    )
    assert filter_properties_dict(
        properties=checkpoint.get_config(mode=ConfigOutputModes.DICT),
        clean_falsy=True,
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
        clean_falsy=True,
    )

    assert len(data_context.list_checkpoints()) == 0
    data_context.add_checkpoint(**yaml.load(yaml_config))
    assert len(data_context.list_checkpoints()) == 1

    data_context.add_expectation_suite(expectation_suite_name="users.warning")
    data_context.add_expectation_suite(expectation_suite_name="users.error")
    result: CheckpointResult = data_context.run_checkpoint(
        checkpoint_name=checkpoint.name,
    )
    assert len(result.list_validation_results()) == 2
    assert len(data_context.validations_store.list_keys()) == 2
    assert result.success

    data_context.delete_checkpoint(name="airflow_users_node_3")
    assert len(data_context.list_checkpoints()) == 0


@pytest.mark.integration
@pytest.mark.slow  # 3.10s
def test_checkpoint_configuration_template_parsing_and_usage_test_yaml_config(
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    monkeypatch,
):
    monkeypatch.setenv("VAR", "test")
    monkeypatch.setenv("MY_PARAM", "1")
    monkeypatch.setenv("OLD_PARAM", "2")

    checkpoint: Checkpoint
    yaml_config: str
    expected_checkpoint_config: dict
    result: CheckpointResult

    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    yaml_config = """
    name: my_base_checkpoint
    config_version: 1
    class_name: Checkpoint
    run_name_template: "%Y-%M-foo-bar-template-$VAR"
    action_list:
    - name: store_validation_result
      action:
        class_name: StoreValidationResultAction
    - name: store_evaluation_params
      action:
        class_name: StoreEvaluationParametersAction
    - name: update_data_docs
      action:
        class_name: UpdateDataDocsAction
    evaluation_parameters:
      param1: "$MY_PARAM"
      param2: 1 + "$OLD_PARAM"
    runtime_configuration:
        result_format:
          result_format: BASIC
          partial_unexpected_count: 20
    """

    expected_checkpoint_config = {
        "name": "my_base_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "template_name": None,
        "run_name_template": "%Y-%M-foo-bar-template-test",
        "expectation_suite_name": None,
        "batch_request": None,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction"},
            },
        ],
        "evaluation_parameters": {"param1": "1", "param2": '1 + "2"'},
        "runtime_configuration": {
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20},
        },
        "validations": [],
        "profilers": [],
    }

    checkpoint: Checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_base_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.get_config(mode=ConfigOutputModes.DICT),
        clean_falsy=True,
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
        clean_falsy=True,
    )

    assert len(data_context.list_checkpoints()) == 0
    data_context.add_checkpoint(**yaml.load(yaml_config))
    assert len(data_context.list_checkpoints()) == 1

    with pytest.raises(
        gx_exceptions.DataContextError,
        match=r'Checkpoint "my_base_checkpoint" must be called with a validator or contain either a batch_request or validations.',
    ):
        # noinspection PyUnusedLocal
        result: CheckpointResult = data_context.run_checkpoint(
            checkpoint_name=checkpoint.name,
        )

    data_context.add_expectation_suite(expectation_suite_name="users.delivery")

    result = data_context.run_checkpoint(
        checkpoint_name="my_base_checkpoint",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
                "expectation_suite_name": "users.delivery",
            },
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "exploration",
                },
                "expectation_suite_name": "users.delivery",
            },
        ],
    )
    assert len(result.list_validation_results()) == 2
    assert len(data_context.validations_store.list_keys()) == 2
    assert result.success

    yaml_config = """
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    template_name: my_base_checkpoint
    validations:
    - batch_request:
        datasource_name: my_pandas_filesystem_datasource
        data_asset_name: users
    - batch_request:
        datasource_name: my_pandas_filesystem_datasource
        data_asset_name: exploration
    expectation_suite_name: users.delivery
    """

    expected_checkpoint_config = {
        "name": "my_fancy_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
        "template_name": "my_base_checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
            },
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "exploration",
                },
            },
        ],
        "expectation_suite_name": "users.delivery",
        "run_name_template": None,
        "batch_request": None,
        "action_list": [],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "profilers": [],
    }

    checkpoint: Checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_fancy_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.get_config(mode=ConfigOutputModes.DICT),
        clean_falsy=True,
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
        clean_falsy=True,
    )

    assert len(data_context.list_checkpoints()) == 1
    data_context.add_checkpoint(**yaml.load(yaml_config))
    assert len(data_context.list_checkpoints()) == 2

    result: CheckpointResult = data_context.run_checkpoint(
        checkpoint_name=checkpoint.name,
    )
    assert len(result.list_validation_results()) == 2
    assert len(data_context.validations_store.list_keys()) == 4
    assert result.success

    data_context.delete_checkpoint(name="my_base_checkpoint")
    data_context.delete_checkpoint(name="my_fancy_checkpoint")
    assert len(data_context.list_checkpoints()) == 0


@pytest.mark.integration
@pytest.mark.slow  # 1.25s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run(
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    common_action_list,
):
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    # add checkpoint config
    checkpoint_config = CheckpointConfig(
        name="my_checkpoint",
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
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
    checkpoint_config_key = ConfigurationIdentifier(
        configuration_key=checkpoint_config.name
    )
    data_context.checkpoint_store.set(
        key=checkpoint_config_key, value=checkpoint_config
    )
    checkpoint: Checkpoint = data_context.get_checkpoint(checkpoint_config.name)

    with pytest.raises(
        gx_exceptions.DataContextError, match=r"expectation_suite .* not found"
    ):
        checkpoint.run()

    assert len(data_context.validations_store.list_keys()) == 0

    data_context.add_expectation_suite("my_expectation_suite")
    result = checkpoint.run()

    assert len(data_context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.integration
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_with_checkpoint_name_in_meta_when_run(
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    checkpoint_name: str = "test_checkpoint_name"
    # add checkpoint config
    checkpoint_config = CheckpointConfig(
        name=checkpoint_name,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=[
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
        ],
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "users",
                },
            },
        ],
    )
    checkpoint_config_key = ConfigurationIdentifier(
        configuration_key=checkpoint_config.name
    )
    data_context.checkpoint_store.set(
        key=checkpoint_config_key, value=checkpoint_config
    )
    checkpoint: Checkpoint = data_context.get_checkpoint(checkpoint_config.name)

    assert len(data_context.validations_store.list_keys()) == 0

    data_context.add_expectation_suite("my_expectation_suite")
    result: CheckpointResult = checkpoint.run()

    assert len(data_context.validations_store.list_keys()) == 1
    assert result["success"]

    validation_result_identifier: DataContextKey = (
        data_context.validations_store.list_keys()[0]
    )
    validation_result: ExpectationSuiteValidationResult = (
        data_context.validations_store.get(validation_result_identifier)
    )

    assert "checkpoint_name" in validation_result.meta
    assert validation_result.meta["checkpoint_name"] == checkpoint_name


@pytest.mark.integration
@pytest.mark.slow  # 1.15s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_with_validator_specified_in_constructor(
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    batch_request_as_dict,
    common_action_list,
):
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    batch_request: FluentBatchRequest = FluentBatchRequest(**batch_request_as_dict)
    data_context.add_expectation_suite("my_expectation_suite")
    validator: Validator = data_context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="my_expectation_suite",
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=data_context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        validator=validator,
        action_list=common_action_list,
    )

    result = checkpoint.run()

    assert len(data_context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.integration
@pytest.mark.slow  # 1.15s
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run_with_validator_specified_in_run(
    titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    batch_request_as_dict,
    common_action_list,
):
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    batch_request: FluentBatchRequest = FluentBatchRequest(**batch_request_as_dict)
    data_context.add_expectation_suite("my_expectation_suite")
    validator: Validator = data_context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="my_expectation_suite",
    )
    checkpoint: Checkpoint = Checkpoint(
        name="my_checkpoint",
        data_context=data_context,
        config_version=1,
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=common_action_list,
    )

    result = checkpoint.run(
        validator=validator,
    )

    assert len(data_context.validations_store.list_keys()) == 1
    assert result["success"]


@pytest.mark.slow  # 1.19s
@pytest.mark.integration
def test_newstyle_checkpoint_result_can_be_pickled(
    titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
):
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    batch_request: dict = {
        "datasource_name": "my_pandas_filesystem_datasource",
        "data_asset_name": "Titanic_1911",
    }

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "Checkpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
        "batch_request": batch_request,
    }

    data_context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = data_context.get_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()
    assert isinstance(pickle.dumps(result), bytes)


@pytest.mark.integration
@pytest.mark.slow  # 1.19s
@pytest.mark.integration
def test_newstyle_checkpoint_result_validations_include_rendered_content(
    titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
):
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    batch_request: dict = {
        "datasource_name": "my_pandas_filesystem_datasource",
        "data_asset_name": "Titanic_1911",
    }

    include_rendered_content: bool = True

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "Checkpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
        "validations": [
            {
                "batch_request": batch_request,
                "include_rendered_content": include_rendered_content,
            },
        ],
    }

    data_context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = data_context.get_checkpoint(name="my_checkpoint")

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


@pytest.mark.integration
@pytest.mark.slow  # 1.22s
def test_newstyle_checkpoint_result_validations_include_rendered_content_data_context_variable(
    titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation,
):
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    batch_request: dict = {
        "datasource_name": "my_pandas_filesystem_datasource",
        "data_asset_name": "Titanic_1911",
    }

    data_context.include_rendered_content.globally = True

    # add checkpoint config
    checkpoint_config: dict = {
        "class_name": "Checkpoint",
        "name": "my_checkpoint",
        "config_version": 1,
        "run_name_template": "%Y-%M-foo-bar-template",
        "expectation_suite_name": "my_expectation_suite",
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {
                    "class_name": "StoreValidationResultAction",
                },
            },
            {
                "name": "store_evaluation_params",
                "action": {
                    "class_name": "StoreEvaluationParametersAction",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
        ],
        "validations": [
            {
                "batch_request": batch_request,
            },
        ],
    }

    data_context.add_checkpoint(**checkpoint_config)
    checkpoint: Checkpoint = data_context.get_checkpoint(name="my_checkpoint")

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


@pytest.mark.integration
@pytest.mark.parametrize(
    "checkpoint_config,expected_validation_id",
    [
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                config_version=1,
                run_name_template="%Y-%M-foo-bar-template",
                expectation_suite_name="my_expectation_suite",
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[
                    {
                        "batch_request": {
                            "datasource_name": "my_pandas_filesystem_datasource",
                            "data_asset_name": "Titanic_1911",
                        },
                    },
                ],
            ),
            None,
            id="no ids",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                config_version=1,
                default_validation_id="7e2bb5c9-cdbe-4c7a-9b2b-97192c55c95b",
                run_name_template="%Y-%M-foo-bar-template",
                expectation_suite_name="my_expectation_suite",
                batch_request={
                    "datasource_name": "my_pandas_filesystem_datasource",
                    "data_asset_name": "Titanic_1911",
                },
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[],
            ),
            "7e2bb5c9-cdbe-4c7a-9b2b-97192c55c95b",
            id="default validation id",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                config_version=1,
                run_name_template="%Y-%M-foo-bar-template",
                expectation_suite_name="my_expectation_suite",
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[
                    {
                        "id": "f22601d9-00b7-4d54-beb6-605d87a74e40",
                        "batch_request": {
                            "datasource_name": "my_pandas_filesystem_datasource",
                            "data_asset_name": "Titanic_1911",
                        },
                    },
                ],
            ),
            "f22601d9-00b7-4d54-beb6-605d87a74e40",
            id="nested validation id",
        ),
        pytest.param(
            CheckpointConfig(
                name="my_checkpoint",
                config_version=1,
                default_validation_id="7e2bb5c9-cdbe-4c7a-9b2b-97192c55c95b",
                run_name_template="%Y-%M-foo-bar-template",
                expectation_suite_name="my_expectation_suite",
                action_list=[
                    {
                        "name": "store_validation_result",
                        "action": {
                            "class_name": "StoreValidationResultAction",
                        },
                    },
                ],
                validations=[
                    {
                        "id": "f22601d9-00b7-4d54-beb6-605d87a74e40",
                        "batch_request": {
                            "datasource_name": "my_pandas_filesystem_datasource",
                            "data_asset_name": "Titanic_1911",
                        },
                    },
                ],
            ),
            "f22601d9-00b7-4d54-beb6-605d87a74e40",
            id="both default and nested validation id",
        ),
    ],
)
def test_checkpoint_run_adds_validation_ids_to_expectation_suite_validation_result_meta(
    checkpoint_config: CheckpointConfig,
    expected_validation_id: str,
    titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation: FileDataContext,
) -> None:
    data_context: FileDataContext = titanic_pandas_data_context_with_fluent_datasources_stats_enabled_and_expectation_suite_with_one_expectation

    checkpoint_config_dict: dict = checkpointConfigSchema.dump(checkpoint_config)
    data_context.add_checkpoint(**checkpoint_config_dict)
    checkpoint: Checkpoint = data_context.get_checkpoint(name="my_checkpoint")

    result: CheckpointResult = checkpoint.run()

    # Always have a single validation result based on the test's parametrization
    validation_result: ExpectationValidationResult | dict = tuple(
        result.run_results.values()
    )[0]["validation_result"]

    actual_validation_id: Optional[str] = validation_result.meta["validation_id"]
    assert expected_validation_id == actual_validation_id
