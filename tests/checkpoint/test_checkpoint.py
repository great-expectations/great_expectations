import logging
import os
import unittest.mock as mock
from typing import List

import pandas as pd
import pytest
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import great_expectations as ge
import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.checkpoint import Checkpoint, LegacyCheckpoint
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.util import filter_properties_dict
from great_expectations.validation_operators.types.validation_operator_result import (
    ValidationOperatorResult,
)

yaml = YAML()

logger = logging.getLogger(__name__)


def test_basic_checkpoint_config_validation(
    empty_data_context,
    caplog,
):
    yaml_config_erroneous: str
    config_erroneous: CommentedMap
    checkpoint_config: CheckpointConfig
    checkpoint: Checkpoint

    yaml_config_erroneous = f"""
    name: misconfigured_checkpoint
    unexpected_property: UNKOWN_PROPERTY_VALUE
    """
    config_erroneous = yaml.load(yaml_config_erroneous)
    with pytest.raises(TypeError):
        # noinspection PyUnusedLocal
        checkpoint_config = CheckpointConfig(**config_erroneous)
    with pytest.raises(KeyError):
        # noinspection PyUnusedLocal
        checkpoint = empty_data_context.test_yaml_config(
            yaml_config=yaml_config_erroneous,
            name="my_erroneous_checkpoint",
        )

    yaml_config_erroneous = f"""
    config_version: 1
    """
    config_erroneous = yaml.load(yaml_config_erroneous)
    with pytest.raises(ge_exceptions.InvalidConfigError):
        # noinspection PyUnusedLocal
        checkpoint_config = CheckpointConfig.from_commented_map(
            commented_map=config_erroneous
        )
    with pytest.raises(KeyError):
        # noinspection PyUnusedLocal
        checkpoint = empty_data_context.test_yaml_config(
            yaml_config=yaml_config_erroneous,
            name="my_erroneous_checkpoint",
        )
    with pytest.raises(ge_exceptions.InvalidConfigError):
        # noinspection PyUnusedLocal
        checkpoint = empty_data_context.test_yaml_config(
            yaml_config=yaml_config_erroneous,
            name="my_erroneous_checkpoint",
            class_name="Checkpoint",
        )

    yaml_config_erroneous = f"""
    config_version: 1
    name: my_erroneous_checkpoint
    class_name: Checkpoint
    """
    # noinspection PyUnusedLocal
    checkpoint = empty_data_context.test_yaml_config(
        yaml_config=yaml_config_erroneous,
        name="my_erroneous_checkpoint",
        class_name="Checkpoint",
    )
    assert (
        'Your current Checkpoint configuration has an empty or missing "validations" attribute'
        in caplog.text
    )
    assert (
        'Your current Checkpoint configuration has an empty or missing "action_list" attribute'
        in caplog.text
    )

    assert len(empty_data_context.list_checkpoints()) == 1

    yaml_config: str = f"""
    name: my_checkpoint
    config_version: 1
    class_name: Checkpoint
    validations: []
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
    """

    expected_checkpoint_config: dict = {
        "name": "my_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "module_name": "great_expectations.checkpoint",
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
    }

    config: CommentedMap = yaml.load(yaml_config)
    checkpoint_config = CheckpointConfig(**config)
    checkpoint = Checkpoint(
        data_context=empty_data_context,
        name="my_checkpoint",
        checkpoint_config=checkpoint_config,
    )
    assert (
        filter_properties_dict(
            properties=checkpoint.self_check()["config"],
        )
        == expected_checkpoint_config
    )
    assert (
        filter_properties_dict(
            properties=checkpoint.config.to_json_dict(),
        )
        == expected_checkpoint_config
    )

    checkpoint = empty_data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_checkpoint",
    )
    assert (
        filter_properties_dict(
            properties=checkpoint.self_check()["config"],
        )
        == expected_checkpoint_config
    )
    assert (
        filter_properties_dict(
            properties=checkpoint.config.to_json_dict(),
        )
        == expected_checkpoint_config
    )

    assert len(empty_data_context.list_checkpoints()) == 2

    empty_data_context.create_expectation_suite(
        expectation_suite_name="my_expectation_suite"
    )
    results: List[ValidationOperatorResult] = empty_data_context.run_checkpoint(
        checkpoint_name=checkpoint.config.name,
    )
    assert len(results) == 0


def test_checkpoint_configuration_no_nesting_using_test_yaml_config(
    titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing,
):
    os.environ["VAR"] = "test"
    os.environ["MY_PARAM"] = "1"
    os.environ["OLD_PARAM"] = "2"

    checkpoint: Checkpoint

    data_context: DataContext = titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing

    yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    # TODO: <Alex>The EvaluationParameters substitution capability does not work for Checkpoints yet.</Alex>
    # TODO: <Alex>The template substitution capability also does not work for Checkpoints yet.</Alex>
    # run_name_template: %Y-%M-foo-bar-template-"$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          partition_request:
            index: -1
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
          # TODO: <Alex>The EvaluationParameters substitution and/or operations capabilities do not work for Checkpoints yet.</Alex>
          # param1: "$MY_PARAM"
          # param2: 1 + "$OLD_PARAM"
          param1: 1
          param2: 2
        runtime_configuration:
          result_format:
            result_format: BASIC
            partial_unexpected_count: 20
    """

    expected_checkpoint_config: dict = {
        "name": "my_fancy_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {
                        "index": -1,
                    },
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
                "evaluation_parameters": {"param1": 1, "param2": 2},
                "runtime_configuration": {
                    "result_format": {
                        "result_format": "BASIC",
                        "partial_unexpected_count": 20,
                    }
                },
            }
        ],
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "run_name_template": None,
        "expectation_suite_name": None,
        "batch_request": None,
        "action_list": [],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "profilers": [],
    }

    checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_fancy_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.config.to_json_dict(),
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
    )

    assert len(data_context.list_checkpoints()) == 1

    data_context.create_expectation_suite(expectation_suite_name="users.delivery")
    results: List[ValidationOperatorResult] = data_context.run_checkpoint(
        checkpoint_name=checkpoint.config.name,
    )
    assert len(results) == 1
    assert len(data_context.validations_store.list_keys()) == 1
    assert results[0].success


def test_checkpoint_configuration_nesting_provides_defaults_for_most_elements_test_yaml_config(
    titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing,
):
    os.environ["VAR"] = "test"
    os.environ["MY_PARAM"] = "1"
    os.environ["OLD_PARAM"] = "2"

    checkpoint: Checkpoint

    data_context: DataContext = titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing

    yaml_config: str = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    # TODO: <Alex>The EvaluationParameters substitution capability does not work for Checkpoints yet.</Alex>
    # TODO: <Alex>The template substitution capability also does not work for Checkpoints yet.</Alex>
    # run_name_template: %Y-%M-foo-bar-template-"$VAR"
    validations:
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_special_data_connector
          data_asset_name: users
          partition_request:
            index: -1
      - batch_request:
          datasource_name: my_datasource
          data_connector_name: my_other_data_connector
          data_asset_name: users
          partition_request:
            index: -2
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
      # TODO: <Alex>The EvaluationParameters substitution and/or operations capabilities do not work for Checkpoints yet.</Alex>
      # param1: "$MY_PARAM"
      # param2: 1 + "$OLD_PARAM"
      param1: 1
      param2: 2
    runtime_configuration:
      result_format:
        result_format: BASIC
        partial_unexpected_count: 20
    """

    expected_checkpoint_config: dict = {
        "name": "my_fancy_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {
                        "index": -1,
                    },
                }
            },
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_other_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {
                        "index": -2,
                    },
                }
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
        "evaluation_parameters": {"param1": 1, "param2": 2},
        "runtime_configuration": {
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "run_name_template": None,
        "batch_request": None,
        "profilers": [],
    }

    checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_fancy_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.config.to_json_dict(),
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
    )

    assert len(data_context.list_checkpoints()) == 1

    data_context.create_expectation_suite(expectation_suite_name="users.delivery")
    results: List[ValidationOperatorResult] = data_context.run_checkpoint(
        checkpoint_name=checkpoint.config.name,
    )
    assert len(results) == 2
    assert len(data_context.validations_store.list_keys()) == 2
    assert all([result.success for result in results])


def test_checkpoint_configuration_using_RuntimeDataConnector_with_Airflow_test_yaml_config(
    titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing,
):
    checkpoint: Checkpoint

    data_context: DataContext = titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing

    yaml_config: str = f"""
    name: airflow_checkpoint
    config_version: 1
    class_name: Checkpoint
    validations:
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_runtime_data_connector
        data_asset_name: IN_MEMORY_DATA_ASSET
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
    """

    expected_checkpoint_config: dict = {
        "name": "airflow_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_runtime_data_connector",
                    "data_asset_name": "IN_MEMORY_DATA_ASSET",
                }
            }
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
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "run_name_template": None,
        "batch_request": None,
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "profilers": [],
    }

    checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="airflow_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.config.to_json_dict(),
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
    )

    assert len(data_context.list_checkpoints()) == 1

    data_context.create_expectation_suite(expectation_suite_name="users.delivery")
    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    results: List[ValidationOperatorResult] = data_context.run_checkpoint(
        checkpoint_name=checkpoint.config.name,
        batch_request={
            "batch_data": test_df,
            "partition_request": {
                "partition_identifiers": {
                    "airflow_run_id": 1234567890,
                }
            },
        },
    )
    assert len(results) == 1
    assert len(data_context.validations_store.list_keys()) == 1
    assert results[0].success


def test_checkpoint_configuration_warning_error_quarantine_test_yaml_config(
    titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing,
):
    os.environ["GE_ENVIRONMENT"] = "my_ge_environment"

    checkpoint: Checkpoint

    data_context: DataContext = titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing

    yaml_config: str = f"""
    name: airflow_users_node_3
    config_version: 1
    class_name: Checkpoint
    batch_request:
        datasource_name: my_datasource
        data_connector_name: my_special_data_connector
        data_asset_name: users
        partition_request:
            index: -1
    validations:
      - expectation_suite_name: users.warning  # runs the top-level action list against the top-level batch_request
      - expectation_suite_name: users.error  # runs the locally-specified_action_list (?UNION THE TOP LEVEL?) against the top-level batch_request
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
    ge.validation_operators.CreateQuarantineData = mock_create_quarantine_data

    mock_create_passed_data = mock.MagicMock()
    mock_create_passed_data.run.return_value = True
    ge.validation_operators.CreatePassedData = mock_create_passed_data

    expected_checkpoint_config: dict = {
        "name": "airflow_users_node_3",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "batch_request": {
            "datasource_name": "my_datasource",
            "data_connector_name": "my_special_data_connector",
            "data_asset_name": "users",
            "partition_request": {
                "index": -1,
            },
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
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "run_name_template": None,
        "expectation_suite_name": None,
        "profilers": [],
    }

    checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="airflow_users_node_3",
    )
    assert filter_properties_dict(
        properties=checkpoint.config.to_json_dict(),
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
    )

    assert len(data_context.list_checkpoints()) == 1

    data_context.create_expectation_suite(expectation_suite_name="users.warning")
    data_context.create_expectation_suite(expectation_suite_name="users.error")
    results: List[ValidationOperatorResult] = data_context.run_checkpoint(
        checkpoint_name=checkpoint.config.name,
    )
    assert len(results) == 2
    assert len(data_context.validations_store.list_keys()) == 2
    assert all([result.success for result in results])


def test_checkpoint_configuration_template_parsing_and_usage_test_yaml_config(
    titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing,
):
    os.environ["VAR"] = "test"
    os.environ["MY_PARAM"] = "1"
    os.environ["OLD_PARAM"] = "2"

    checkpoint: Checkpoint
    yaml_config: str
    expected_checkpoint_config: dict
    results: List[ValidationOperatorResult]

    data_context: DataContext = titanic_pandas_data_context_with_v013_datasource_for_checkpoints_v1_config_testing

    yaml_config = f"""
    name: my_base_checkpoint
    config_version: 1
    class_name: Checkpoint
    # TODO: <Alex>The EvaluationParameters substitution capability does not work for Checkpoints yet.</Alex>
    # TODO: <Alex>The template substitution capability also does not work for Checkpoints yet.</Alex>
    # run_name_template: %Y-%M-foo-bar-template-"$VAR"
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
      # TODO: <Alex>The EvaluationParameters substitution and/or operations capabilities do not work for Checkpoints yet.</Alex>
      # param1: "$MY_PARAM"
      # param2: 1 + "$OLD_PARAM"
      param1: 1
      param2: 2
    runtime_configuration:
        result_format:
          result_format: BASIC
          partial_unexpected_count: 20
    """

    expected_checkpoint_config = {
        "name": "my_base_checkpoint",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": None,
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
        "evaluation_parameters": {"param1": 1, "param2": 2},
        "runtime_configuration": {
            "result_format": {"result_format": "BASIC", "partial_unexpected_count": 20}
        },
        "validations": [],
        "profilers": [],
    }

    checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_base_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.config.to_json_dict(),
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
    )

    assert len(data_context.list_checkpoints()) == 1

    data_context.create_expectation_suite(expectation_suite_name="users.delivery")

    results = data_context.run_checkpoint(
        checkpoint_name="my_base_checkpoint",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {
                        "index": -1,
                    },
                },
                "expectation_suite_name": "users.delivery",
            },
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_other_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {
                        "index": -2,
                    },
                },
                "expectation_suite_name": "users.delivery",
            },
        ],
    )
    assert len(results) == 2
    assert len(data_context.validations_store.list_keys()) == 2
    assert all([result.success for result in results])

    yaml_config = f"""
    name: my_fancy_checkpoint
    config_version: 1
    class_name: Checkpoint
    template_name: my_base_checkpoint
    validations:
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_special_data_connector
        data_asset_name: users
        partition_request:
          index: -1
    - batch_request:
        datasource_name: my_datasource
        data_connector_name: my_other_data_connector
        data_asset_name: users
        partition_request:
          index: -2
    expectation_suite_name: users.delivery
    """

    expected_checkpoint_config = {
        "name": "my_fancy_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "template_name": "my_base_checkpoint",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {
                        "index": -1,
                    },
                }
            },
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_other_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {
                        "index": -2,
                    },
                }
            },
        ],
        "expectation_suite_name": "users.delivery",
        "module_name": "great_expectations.checkpoint",
        "run_name_template": None,
        "batch_request": None,
        "action_list": [],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "profilers": [],
    }

    checkpoint = data_context.test_yaml_config(
        yaml_config=yaml_config,
        name="my_fancy_checkpoint",
    )
    assert filter_properties_dict(
        properties=checkpoint.config.to_json_dict(),
    ) == filter_properties_dict(
        properties=expected_checkpoint_config,
    )

    assert len(data_context.list_checkpoints()) == 2

    results: List[ValidationOperatorResult] = data_context.run_checkpoint(
        checkpoint_name=checkpoint.config.name,
    )
    assert len(results) == 2
    assert len(data_context.validations_store.list_keys()) == 4
    assert all([result.success for result in results])


def test_legacy_checkpoint_instantiates_and_produces_a_validation_result_when_run(
    filesystem_csv_data_context,
):
    base_directory = filesystem_csv_data_context.list_datasources()[0][
        "batch_kwargs_generators"
    ]["subdir_reader"]["base_directory"]
    batch_kwargs = {
        "path": base_directory + "/f1.csv",
        "datasource": "rad_datasource",
        "reader_method": "read_csv",
    }

    checkpoint_config_dict = {
        "validation_operator_name": "action_list_operator",
        "batches": [
            {"batch_kwargs": batch_kwargs, "expectation_suite_names": ["my_suite"]}
        ],
    }

    checkpoint = LegacyCheckpoint(
        data_context=filesystem_csv_data_context,
        name="my_checkpoint",
        checkpoint_config=checkpoint_config_dict,
    )

    with pytest.raises(
        ge_exceptions.DataContextError, match=r"expectation_suite .* not found"
    ):
        checkpoint.run()

    assert len(filesystem_csv_data_context.validations_store.list_keys()) == 0

    filesystem_csv_data_context.create_expectation_suite("my_suite")
    print(filesystem_csv_data_context.list_datasources())
    # noinspection PyUnusedLocal
    results = checkpoint.run()

    assert len(filesystem_csv_data_context.validations_store.list_keys()) == 1


# TODO: add more test cases
def test_newstyle_checkpoint_instantiates_and_produces_a_validation_result_when_run(
    titanic_pandas_multibatch_data_context_with_013_datasource,
):
    context = titanic_pandas_multibatch_data_context_with_013_datasource
    # add checkpoint config
    checkpoint_config = CheckpointConfig(
        config_version=1,
        name="my_checkpoint",
        run_name_template="%Y-%M-foo-bar-template",
        expectation_suite_name="my_expectation_suite",
        action_list=[
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
        validations=[
            {
                "batch_request": {
                    "datasource_name": "titanic_multi_batch",
                    "data_connector_name": "my_data_connector",
                    "data_asset_name": "Titanic_1911",
                }
            }
        ],
    )
    checkpoint_config_key = ConfigurationIdentifier(
        configuration_key=checkpoint_config.name
    )
    context.checkpoint_store.set(key=checkpoint_config_key, value=checkpoint_config)
    checkpoint = context.get_checkpoint(checkpoint_config.name, return_config=False)

    with pytest.raises(
        ge_exceptions.DataContextError, match=r"expectation_suite .* not found"
    ):
        checkpoint.run()

    assert len(context.validations_store.list_keys()) == 0

    context.create_expectation_suite("my_expectation_suite")
    print(context.list_datasources())
    # noinspection PyUnusedLocal
    results = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1


# TODO: Add test case for recusrive template cases (nested templates) and template_name specified at runtime
def test_newstyle_checkpoint_config_substitution_simple(
    titanic_pandas_multibatch_data_context_with_013_datasource,
):
    context = titanic_pandas_multibatch_data_context_with_013_datasource

    # add template config
    checkpoint_template_config = CheckpointConfig(
        config_version=1,
        name="my_template_checkpoint",
        run_name_template="%Y-%M-foo-bar-template-$VAR",
        action_list=[
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
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
        },
        runtime_configuration={
            "result_format": {
                "result_format": "BASIC",
                "partial_unexpected_count": 20,
            }
        },
    )
    checkpoint_template_config_key = ConfigurationIdentifier(
        configuration_key=checkpoint_template_config.name
    )
    context.checkpoint_store.set(
        key=checkpoint_template_config_key, value=checkpoint_template_config
    )

    # add child config
    simplified_checkpoint_config = CheckpointConfig(
        config_version=1,
        name="my_simplified_checkpoint",
        template_name="my_template_checkpoint",
        expectation_suite_name="users.delivery",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {"partition_index": -1},
                }
            },
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_other_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {"partition_index": -2},
                }
            },
        ],
    )
    simplified_checkpoint_config_key = ConfigurationIdentifier(
        configuration_key=simplified_checkpoint_config.name
    )
    context.checkpoint_store.set(
        key=simplified_checkpoint_config_key, value=simplified_checkpoint_config
    )

    simplified_checkpoint = Checkpoint(
        name=simplified_checkpoint_config.name,
        data_context=context,
        checkpoint_config=simplified_checkpoint_config,
    )

    # template only
    expected_substituted_checkpoint_config_template_only = CheckpointConfig(
        config_version=1,
        name="my_simplified_checkpoint",
        expectation_suite_name="users.delivery",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_special_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {"partition_index": -1},
                }
            },
            {
                "batch_request": {
                    "datasource_name": "my_datasource",
                    "data_connector_name": "my_other_data_connector",
                    "data_asset_name": "users",
                    "partition_request": {"partition_index": -2},
                }
            },
        ],
        run_name_template="%Y-%M-foo-bar-template-$VAR",
        action_list=[
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
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
        },
        runtime_configuration={
            "result_format": {
                "result_format": "BASIC",
                "partial_unexpected_count": 20,
            }
        },
    )

    substituted_config_template_only = simplified_checkpoint.get_substituted_config()
    assert (
        substituted_config_template_only.to_json_dict()
        == expected_substituted_checkpoint_config_template_only.to_json_dict()
    )
    # make sure operation is idempotent
    simplified_checkpoint.get_substituted_config()
    assert (
        substituted_config_template_only.to_json_dict()
        == expected_substituted_checkpoint_config_template_only.to_json_dict()
    )

    # template and runtime kwargs
    expected_substituted_checkpoint_config_template_and_runtime_kwargs = (
        CheckpointConfig(
            config_version=1,
            name="my_simplified_checkpoint",
            expectation_suite_name="runtime_suite_name",
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_special_data_connector",
                        "data_asset_name": "users",
                        "partition_request": {"partition_index": -1},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector",
                        "data_asset_name": "users",
                        "partition_request": {"partition_index": -2},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_2",
                        "data_asset_name": "users",
                        "partition_request": {"partition_index": -3},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_3",
                        "data_asset_name": "users",
                        "partition_request": {"partition_index": -4},
                    }
                },
            ],
            run_name_template="runtime_run_template",
            action_list=[
                {
                    "name": "store_validation_result",
                    "action": {
                        "class_name": "StoreValidationResultAction",
                    },
                },
                {
                    "name": "store_evaluation_params",
                    "action": {
                        "class_name": "MyCustomStoreEvaluationParametersAction",
                    },
                },
                {
                    "name": "update_data_docs_deluxe",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                    },
                },
            ],
            evaluation_parameters={
                "environment": "runtime-$GE_ENVIRONMENT",
                "tolerance": 1.0e-2,
                "aux_param_0": "runtime-$MY_PARAM",
                "aux_param_1": "1 + $MY_PARAM",
                "new_runtime_eval_param": "bloopy!",
            },
            runtime_configuration={
                "result_format": {
                    "result_format": "BASIC",
                    "partial_unexpected_count": 999,
                    "new_runtime_config_key": "bleepy!",
                }
            },
        )
    )

    substituted_config_template_and_runtime_kwargs = (
        simplified_checkpoint.get_substituted_config(
            runtime_kwargs={
                "expectation_suite_name": "runtime_suite_name",
                "validations": [
                    {
                        "batch_request": {
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_other_data_connector_2",
                            "data_asset_name": "users",
                            "partition_request": {"partition_index": -3},
                        }
                    },
                    {
                        "batch_request": {
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_other_data_connector_3",
                            "data_asset_name": "users",
                            "partition_request": {"partition_index": -4},
                        }
                    },
                ],
                "run_name_template": "runtime_run_template",
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
                            "class_name": "MyCustomStoreEvaluationParametersAction",
                        },
                    },
                    {
                        "name": "update_data_docs",
                        "action": None,
                    },
                    {
                        "name": "update_data_docs_deluxe",
                        "action": {
                            "class_name": "UpdateDataDocsAction",
                        },
                    },
                ],
                "evaluation_parameters": {
                    "environment": "runtime-$GE_ENVIRONMENT",
                    "tolerance": 1.0e-2,
                    "aux_param_0": "runtime-$MY_PARAM",
                    "aux_param_1": "1 + $MY_PARAM",
                    "new_runtime_eval_param": "bloopy!",
                },
                "runtime_configuration": {
                    "result_format": {
                        "result_format": "BASIC",
                        "partial_unexpected_count": 999,
                        "new_runtime_config_key": "bleepy!",
                    }
                },
            }
        )
    )
    assert (
        substituted_config_template_and_runtime_kwargs.to_json_dict()
        == expected_substituted_checkpoint_config_template_and_runtime_kwargs.to_json_dict()
    )
