import logging
from pathlib import Path
from typing import Dict, List, Union

import pytest

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.store import CheckpointStore
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.util import filter_properties_dict, gen_directory_tree_str
from tests.test_utils import build_checkpoint_store_using_filesystem

logger = logging.getLogger(__name__)


def test_checkpoint_store(empty_data_context):
    store_name: str = "checkpoint_store"
    base_directory: str = str(Path(empty_data_context.root_directory) / "checkpoints")

    checkpoint_store: CheckpointStore = build_checkpoint_store_using_filesystem(
        store_name=store_name,
        base_directory=base_directory,
        overwrite_existing=True,
    )

    assert len(checkpoint_store.list_keys()) == 0

    with pytest.raises(TypeError):
        checkpoint_store.set(
            key="my_first_checkpoint", value="this is not a checkpoint"
        )

    assert len(checkpoint_store.list_keys()) == 0

    checkpoint_name_0: str = "my_checkpoint_0"
    run_name_template_0: str = "%Y-%M-my-run-template-$VAR"
    validations_0: Union[List, Dict] = [
        {
            "batch_request": {
                "datasource_name": "my_pandas_datasource",
                "data_connector_name": "my_runtime_data_connector",
                "data_asset_name": "my_website_logs",
            },
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
        }
    ]
    expectation_suite_name_0: str = "my.test.expectation_suite.name"
    evaluation_parameters_0: dict = {
        "environment": "$GE_ENVIRONMENT",
        "tolerance": 1.0e-2,
        "aux_param_0": "$MY_PARAM",
        "aux_param_1": "1 + $MY_PARAM",
    }
    runtime_configuration_0: dict = {
        "result_format": {
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
        },
    }
    my_checkpoint_config_0: CheckpointConfig = CheckpointConfig(
        name=checkpoint_name_0,
        run_name_template=run_name_template_0,
        expectation_suite_name=expectation_suite_name_0,
        evaluation_parameters=evaluation_parameters_0,
        runtime_configuration=runtime_configuration_0,
        validations=validations_0,
    )

    key_0: ConfigurationIdentifier = ConfigurationIdentifier(
        configuration_key=checkpoint_name_0,
    )
    checkpoint_store.set(key=key_0, value=my_checkpoint_config_0)

    assert len(checkpoint_store.list_keys()) == 1

    assert filter_properties_dict(
        properties=checkpoint_store.get(key=key_0).to_json_dict(),
    ) == filter_properties_dict(
        properties=my_checkpoint_config_0.to_json_dict(),
    )

    dir_tree: str = gen_directory_tree_str(startpath=base_directory)
    assert (
        dir_tree
        == """checkpoints/
    .ge_store_backend_id
    my_checkpoint_0.yml
"""
    )

    checkpoint_name_1: str = "my_checkpoint_1"
    run_name_template_1: str = "%Y-%M-my-run-template-$VAR"
    validations_1: Union[List, Dict] = [
        {
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
            ]
        }
    ]
    expectation_suite_name_1: str = "my.test.expectation_suite.name"
    batch_request_1: dict = {
        "datasource_name": "my_pandas_datasource",
        "data_connector_name": "my_runtime_data_connector",
        "data_asset_name": "my_website_logs",
    }
    evaluation_parameters_1: dict = {
        "environment": "$GE_ENVIRONMENT",
        "tolerance": 1.0e-2,
        "aux_param_0": "$MY_PARAM",
        "aux_param_1": "1 + $MY_PARAM",
    }
    runtime_configuration_1: dict = {
        "result_format": {
            "result_format": "BASIC",
            "partial_unexpected_count": 20,
        },
    }
    my_checkpoint_config_1: CheckpointConfig = CheckpointConfig(
        name=checkpoint_name_1,
        run_name_template=run_name_template_1,
        expectation_suite_name=expectation_suite_name_1,
        batch_request=batch_request_1,
        evaluation_parameters=evaluation_parameters_1,
        runtime_configuration=runtime_configuration_1,
        validations=validations_1,
    )

    key_1: ConfigurationIdentifier = ConfigurationIdentifier(
        configuration_key=checkpoint_name_1,
    )
    checkpoint_store.set(key=key_1, value=my_checkpoint_config_1)

    assert len(checkpoint_store.list_keys()) == 2

    assert filter_properties_dict(
        properties=checkpoint_store.get(key=key_1).to_json_dict(),
    ) == filter_properties_dict(
        properties=my_checkpoint_config_1.to_json_dict(),
    )

    dir_tree: str = gen_directory_tree_str(startpath=base_directory)
    assert (
        dir_tree
        == """checkpoints/
    .ge_store_backend_id
    my_checkpoint_0.yml
    my_checkpoint_1.yml
"""
    )

    self_check_report: dict = convert_to_json_serializable(
        data=checkpoint_store.self_check()
    )
    assert self_check_report == {
        "keys": ["my_checkpoint_0", "my_checkpoint_1"],
        "len_keys": 2,
        "config": {
            "store_name": "checkpoint_store",
            "class_name": "CheckpointStore",
            "module_name": "great_expectations.data_context.store.checkpoint_store",
            "overwrite_existing": True,
            "store_backend": {
                "base_directory": f"{empty_data_context.root_directory}/checkpoints",
                "platform_specific_separator": True,
                "fixed_length_key": False,
                "suppress_store_backend_id": False,
                "module_name": "great_expectations.data_context.store.tuple_store_backend",
                "class_name": "TupleFilesystemStoreBackend",
                "filepath_template": "{0}.yml",
            },
        },
    }

    checkpoint_store.remove_key(key=key_0)
    checkpoint_store.remove_key(key=key_1)
    assert len(checkpoint_store.list_keys()) == 0
