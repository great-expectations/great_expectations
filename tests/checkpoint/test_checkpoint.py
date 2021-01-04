import logging

import pytest
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.checkpoint import Checkpoint, LegacyCheckpoint
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)

yaml = YAML()

logger = logging.getLogger(__name__)


def test_checkpoint_config(empty_data_context):
    yaml_config_erroneous: str
    config_erroneous: CommentedMap
    checkpoint_config: CheckpointConfig

    yaml_config_erroneous = f"""
    name: misconfigured_checkpoint
    unexpected_property: UNKOWN_PROPERTY_VALUE
    """
    config_erroneous = yaml.load(yaml_config_erroneous)
    with pytest.raises(TypeError):
        # noinspection PyUnusedLocal
        checkpoint_config = CheckpointConfig(**config_erroneous)

    yaml_config_erroneous = f"""
    config_version: 1
    """
    config_erroneous = yaml.load(yaml_config_erroneous)
    with pytest.raises(ge_exceptions.InvalidConfigError):
        # noinspection PyUnusedLocal
        checkpoint_config = CheckpointConfig.from_commented_map(
            commented_map=config_erroneous
        )

    yaml_config: str = f"""
    name: my_checkpoint
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

    config: CommentedMap = yaml.load(yaml_config)
    checkpoint_config: CheckpointConfig = CheckpointConfig(**config)
    checkpoint: Checkpoint = Checkpoint(
        data_context=empty_data_context,
        name="my_checkpoint",
        checkpoint_config=checkpoint_config,
    )

    assert checkpoint.self_check()["config"] == {
        "name": "my_checkpoint",
        "config_version": None,
        "class_name": "LegacyCheckpoint",
        "validation_operator_name": "action_list_operator",
    }


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
    results = checkpoint.run()

    assert len(context.validations_store.list_keys()) == 1


@pytest.fixture
def context_with_checkpoint_templates(titanic_pandas_multibatch_data_context_with_013_datasource):
    context = titanic_pandas_multibatch_data_context_with_013_datasource

    # add simple template config
    simple_checkpoint_template_config = CheckpointConfig(
        config_version=1,
        name="my_simple_template_checkpoint",
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
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
        },
    )
    simple_checkpoint_template_config_key = ConfigurationIdentifier(
        configuration_key=simple_checkpoint_template_config.name
    )
    context.checkpoint_store.set(
        key=simple_checkpoint_template_config_key, value=simple_checkpoint_template_config
    )

    # add nested template configs
    nested_checkpoint_template_config_1 = CheckpointConfig(
        config_version=1,
        name="my_nested_checkpoint_template_1",
        run_name_template="%Y-%M-foo-bar-template-$VAR",
        expectation_suite_name="suite_from_template_1",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource_template_1",
                    "data_connector_name": "my_special_data_connector_template_1",
                    "data_asset_name": "users_from_template_1",
                    "partition_request": {
                        "partition_index": -999
                    }
                }
            }
        ],
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
            "environment": "FOO",
            "tolerance": "FOOBOO",
            "aux_param_0": "FOOBARBOO",
            "aux_param_1": "FOOBARBOO",
            "template_1_key": 456
        },
        runtime_configuration={
            "result_format": "FOOBARBOO",
            "partial_unexpected_count": "FOOBARBOO",
            "template_1_key": 123
        },
    )
    nested_checkpoint_template_config_1_key = ConfigurationIdentifier(
        configuration_key=nested_checkpoint_template_config_1.name
    )
    context.checkpoint_store.set(
        key=nested_checkpoint_template_config_1_key, value=nested_checkpoint_template_config_1
    )

    nested_checkpoint_template_config_2 = CheckpointConfig(
        config_version=1,
        name="my_nested_checkpoint_template_2",
        template_name="my_nested_checkpoint_template_1",
        run_name_template="%Y-%M-foo-bar-template-$VAR-template-2",
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
                    "class_name": "MyCustomStoreEvaluationParametersActionTemplate2",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
            {
                "name": "new_action_from_template_2",
                "action": {
                    "class_name": "Template2SpecialAction"
                }
            }
        ],
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
        },
        runtime_configuration={
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
        },
    )
    nested_checkpoint_template_config_2_key = ConfigurationIdentifier(
        configuration_key=nested_checkpoint_template_config_2.name
    )
    context.checkpoint_store.set(
        key=nested_checkpoint_template_config_2_key, value=nested_checkpoint_template_config_2
    )

    nested_checkpoint_template_config_3 = CheckpointConfig(
        config_version=1,
        name="my_nested_checkpoint_template_3",
        template_name="my_nested_checkpoint_template_2",
        run_name_template="%Y-%M-foo-bar-template-$VAR-template-3",
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
                    "class_name": "MyCustomStoreEvaluationParametersActionTemplate3",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
            {
                "name": "new_action_from_template_3",
                "action": {
                    "class_name": "Template3SpecialAction"
                }
            }
        ],
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
            "template_3_key": 123
        },
        runtime_configuration={
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
            "template_3_key": "bloopy!"
        },
    )
    nested_checkpoint_template_config_3_key = ConfigurationIdentifier(
        configuration_key=nested_checkpoint_template_config_3.name
    )
    context.checkpoint_store.set(
        key=nested_checkpoint_template_config_3_key, value=nested_checkpoint_template_config_3
    )

    return context


def test_newstyle_checkpoint_config_substitution_simple(
    context_with_checkpoint_templates,
):
    context = context_with_checkpoint_templates

    simplified_checkpoint_config = CheckpointConfig(
        config_version=1,
        name="my_simplified_checkpoint",
        template_name="my_simple_template_checkpoint",
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
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
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
                "result_format": "BASIC",
                "partial_unexpected_count": 999,
                "new_runtime_config_key": "bleepy!",
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
                    "result_format": "BASIC",
                    "partial_unexpected_count": 999,
                    "new_runtime_config_key": "bleepy!",
                },
            }
        )
    )
    assert (
        substituted_config_template_and_runtime_kwargs.to_json_dict()
        == expected_substituted_checkpoint_config_template_and_runtime_kwargs.to_json_dict()
    )


def test_newstyle_checkpoint_config_substitution_nested(
    context_with_checkpoint_templates,
):
    context = context_with_checkpoint_templates

    nested_checkpoint_config = CheckpointConfig(
        config_version=1,
        name="my_nested_checkpoint",
        template_name="my_nested_checkpoint_template_2",
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
    nested_checkpoint = Checkpoint(
        name=nested_checkpoint_config.name,
        data_context=context,
        checkpoint_config=nested_checkpoint_config,
    )

    # template only
    expected_nested_checkpoint_config_template_only = CheckpointConfig(
        config_version=1,
        name="my_nested_checkpoint",
        expectation_suite_name="users.delivery",
        validations=[
            {
                "batch_request": {
                    "datasource_name": "my_datasource_template_1",
                    "data_connector_name": "my_special_data_connector_template_1",
                    "data_asset_name": "users_from_template_1",
                    "partition_request": {
                        "partition_index": -999
                    }
                }
            },
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
        run_name_template="%Y-%M-foo-bar-template-$VAR-template-2",
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
                    "class_name": "MyCustomStoreEvaluationParametersActionTemplate2",
                },
            },
            {
                "name": "update_data_docs",
                "action": {
                    "class_name": "UpdateDataDocsAction",
                },
            },
            {
                "name": "new_action_from_template_2",
                "action": {
                    "class_name": "Template2SpecialAction"
                }
            }
        ],
        evaluation_parameters={
            "environment": "$GE_ENVIRONMENT",
            "tolerance": 1.0e-2,
            "aux_param_0": "$MY_PARAM",
            "aux_param_1": "1 + $MY_PARAM",
            "template_1_key": 456
        },
        runtime_configuration={
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
            "template_1_key": 123
        },
    )

    substituted_config_template_only = nested_checkpoint.get_substituted_config()
    assert (
        substituted_config_template_only.to_json_dict()
        == expected_nested_checkpoint_config_template_only.to_json_dict()
    )
    # make sure operation is idempotent
    nested_checkpoint.get_substituted_config()
    assert (
        substituted_config_template_only.to_json_dict()
        == expected_nested_checkpoint_config_template_only.to_json_dict()
    )

    # template and runtime kwargs
    expected_nested_checkpoint_config_template_and_runtime_template_name = (
        CheckpointConfig(
            config_version=1,
            name="my_nested_checkpoint",
            expectation_suite_name="runtime_suite_name",
            validations=[
                {
                    "batch_request": {
                        "datasource_name": "my_datasource_template_1",
                        "data_connector_name": "my_special_data_connector_template_1",
                        "data_asset_name": "users_from_template_1",
                        "partition_request": {
                            "partition_index": -999
                        }
                    }
                },
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
                        "data_connector_name": "my_other_data_connector_2_runtime",
                        "data_asset_name": "users",
                        "partition_request": {"partition_index": -3},
                    }
                },
                {
                    "batch_request": {
                        "datasource_name": "my_datasource",
                        "data_connector_name": "my_other_data_connector_3_runtime",
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
                        "class_name": "MyCustomRuntimeStoreEvaluationParametersAction",
                    },
                },
                {
                    "name": "new_action_from_template_2",
                    "action": {
                        "class_name": "Template2SpecialAction"
                    }
                },
                {
                    "name": "new_action_from_template_3",
                    "action": {
                        "class_name": "Template3SpecialAction"
                    }
                },
                {
                    "name": "update_data_docs_deluxe_runtime",
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
                "template_1_key": 456,
                "template_3_key": 123,
                "new_runtime_eval_param": "bloopy!",
            },
            runtime_configuration={
                "result_format": "BASIC",
                "partial_unexpected_count": 999,
                "template_1_key": 123,
                "template_3_key": "bloopy!",
                "new_runtime_config_key": "bleepy!",
            },
        )
    )

    substituted_config_template_and_runtime_kwargs = (
        nested_checkpoint.get_substituted_config(
            runtime_kwargs={
                "expectation_suite_name": "runtime_suite_name",
                "template_name": "my_nested_checkpoint_template_3",
                "validations": [
                    {
                        "batch_request": {
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_other_data_connector_2_runtime",
                            "data_asset_name": "users",
                            "partition_request": {"partition_index": -3},
                        }
                    },
                    {
                        "batch_request": {
                            "datasource_name": "my_datasource",
                            "data_connector_name": "my_other_data_connector_3_runtime",
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
                            "class_name": "MyCustomRuntimeStoreEvaluationParametersAction",
                        },
                    },
                    {
                        "name": "update_data_docs",
                        "action": None,
                    },
                    {
                        "name": "update_data_docs_deluxe_runtime",
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
                    "result_format": "BASIC",
                    "partial_unexpected_count": 999,
                    "new_runtime_config_key": "bleepy!",
                },
            }
        )
    )
    assert (
        substituted_config_template_and_runtime_kwargs.to_json_dict()
        == expected_nested_checkpoint_config_template_and_runtime_template_name.to_json_dict()
    )
