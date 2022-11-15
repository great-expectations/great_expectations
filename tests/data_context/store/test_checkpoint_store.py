import logging
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union
from unittest import mock

import pytest
from marshmallow.exceptions import ValidationError

import great_expectations.exceptions as ge_exceptions
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.data_context import DataContext
from great_expectations.data_context.store import CheckpointStore
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import filter_properties_dict, gen_directory_tree_str
from tests.core.usage_statistics.util import (
    usage_stats_exceptions_exist,
    usage_stats_invalid_messages_exist,
)
from tests.test_utils import build_checkpoint_store_using_filesystem

logger = logging.getLogger(__name__)


@pytest.fixture
def checkpoint_store_with_mock_backend() -> Tuple[CheckpointStore, mock.MagicMock]:
    store = CheckpointStore(store_name="checkpoint_store")
    mock_backend = mock.MagicMock()
    store._store_backend = mock_backend

    return store, mock_backend


@pytest.mark.integration
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
    my_checkpoint_config_0 = CheckpointConfig(
        name=checkpoint_name_0,
        run_name_template=run_name_template_0,
        expectation_suite_name=expectation_suite_name_0,
        evaluation_parameters=evaluation_parameters_0,
        runtime_configuration=runtime_configuration_0,
        validations=validations_0,
    )

    key_0 = ConfigurationIdentifier(
        configuration_key=checkpoint_name_0,
    )
    checkpoint_store.set(key=key_0, value=my_checkpoint_config_0)

    assert len(checkpoint_store.list_keys()) == 1

    assert filter_properties_dict(
        properties=checkpoint_store.get(key=key_0).to_json_dict(),
        clean_falsy=True,
    ) == filter_properties_dict(
        properties=my_checkpoint_config_0.to_json_dict(),
        clean_falsy=True,
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
    my_checkpoint_config_1 = CheckpointConfig(
        name=checkpoint_name_1,
        run_name_template=run_name_template_1,
        expectation_suite_name=expectation_suite_name_1,
        batch_request=batch_request_1,
        evaluation_parameters=evaluation_parameters_1,
        runtime_configuration=runtime_configuration_1,
        validations=validations_1,
    )

    key_1 = ConfigurationIdentifier(
        configuration_key=checkpoint_name_1,
    )
    checkpoint_store.set(key=key_1, value=my_checkpoint_config_1)

    assert len(checkpoint_store.list_keys()) == 2

    assert filter_properties_dict(
        properties=checkpoint_store.get(key=key_1).to_json_dict(),
        clean_falsy=True,
    ) == filter_properties_dict(
        properties=my_checkpoint_config_1.to_json_dict(),
        clean_falsy=True,
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
                "filepath_suffix": ".yml",
            },
        },
    }

    checkpoint_store.remove_key(key=key_0)
    checkpoint_store.remove_key(key=key_1)
    assert len(checkpoint_store.list_keys()) == 0


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@pytest.mark.integration
def test_instantiation_with_test_yaml_config(
    mock_emit, caplog, empty_data_context_stats_enabled
):
    empty_data_context_stats_enabled.test_yaml_config(
        yaml_config="""
module_name: great_expectations.data_context.store.checkpoint_store
class_name: CheckpointStore
store_backend:
    class_name: TupleFilesystemStoreBackend
    base_directory: checkpoints/
"""
    )
    assert mock_emit.call_count == 1
    # Substitute current anonymized name since it changes for each run
    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
        "anonymized_name"
    ]
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.test_yaml_config",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "CheckpointStore",
                    "anonymized_store_backend": {
                        "parent_class": "TupleFilesystemStoreBackend"
                    },
                },
                "success": True,
            }
        ),
    ]

    # Confirm that logs do not contain any exceptions or invalid messages
    assert not usage_stats_exceptions_exist(messages=caplog.messages)
    assert not usage_stats_invalid_messages_exist(messages=caplog.messages)


@pytest.mark.unit
@pytest.mark.cloud
def test_ge_cloud_response_json_to_object_dict() -> None:
    store = CheckpointStore(store_name="checkpoint_store")

    checkpoint_id = "7b5e962c-3c67-4a6d-b311-b48061d52103"
    checkpoint_config = {
        "name": "oss_test_checkpoint",
        "config_version": 1.0,
        "class_name": "Checkpoint",
        "expectation_suite_name": "oss_test_expectation_suite",
        "validations": [
            {
                "expectation_suite_name": "taxi.demo_pass",
            },
            {
                "batch_request": {
                    "datasource_name": "oss_test_datasource",
                    "data_connector_name": "oss_test_data_connector",
                    "data_asset_name": "users",
                },
            },
        ],
    }
    response_json = {
        "data": {
            "id": checkpoint_id,
            "attributes": {
                "checkpoint_config": checkpoint_config,
            },
        }
    }

    expected = checkpoint_config
    expected["ge_cloud_id"] = checkpoint_id

    actual = store.ge_cloud_response_json_to_object_dict(response_json)

    assert actual == expected


@pytest.mark.unit
def test_serialization_self_check(capsys) -> None:
    store = CheckpointStore(store_name="checkpoint_store")

    with mock.patch("random.choice", lambda _: "0"):
        store.serialization_self_check(pretty_print=True)

    stdout = capsys.readouterr().out

    test_key = "ConfigurationIdentifier::test-name-00000000000000000000"
    messages = [
        f"Attempting to add a new test key {test_key} to Checkpoint store...",
        f"Test key {test_key} successfully added to Checkpoint store.",
        f"Attempting to retrieve the test value associated with key {test_key} from Checkpoint store...",
        "Test value successfully retrieved from Checkpoint store",
        f"Cleaning up test key {test_key} and value from Checkpoint store...",
        "Test key and value successfully removed from Checkpoint store",
    ]

    for message in messages:
        assert message in stdout


@pytest.mark.parametrize(
    "path,exists",
    [
        ("", False),
        ("my_fake_dir", False),
        (
            file_relative_path(
                __file__,
                "../../integration/fixtures/gcp_deployment/great_expectations",
            ),
            True,
        ),
    ],
)
@pytest.mark.unit
def test_default_checkpoints_exist(path: str, exists: bool) -> None:
    store = CheckpointStore(store_name="checkpoint_store")
    assert store.default_checkpoints_exist(path) is exists


@pytest.mark.unit
def test_list_checkpoints(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.list_keys.return_value = [("a", "b", "c"), ("d", "e", "f")]

    checkpoints = store.list_checkpoints(ge_cloud_mode=False)
    assert checkpoints == ["a.b.c", "d.e.f"]


@pytest.mark.unit
@pytest.mark.cloud
def test_list_checkpoints_cloud_mode(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.list_keys.return_value = [("a", "b", "c"), ("d", "e", "f")]

    checkpoints = store.list_checkpoints(ge_cloud_mode=True)
    assert checkpoints == [
        ConfigurationIdentifier("a.b.c"),
        ConfigurationIdentifier("d.e.f"),
    ]


@pytest.mark.unit
def test_delete_checkpoint(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend

    store.delete_checkpoint(name="my_checkpoint")

    mock_backend.remove_key.assert_called_once_with(
        ConfigurationIdentifier("my_checkpoint")
    )


@pytest.mark.cloud
@pytest.mark.unit
def test_delete_checkpoint_with_cloud_id(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend

    store.delete_checkpoint(ge_cloud_id="abc123")

    mock_backend.remove_key.assert_called_once_with(
        GXCloudIdentifier(
            resource_type=GXCloudRESTResource.CHECKPOINT, ge_cloud_id="abc123"
        )
    )


@pytest.mark.unit
def test_delete_checkpoint_with_invalid_key_raises_error(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    def _raise_key_error(_: Any) -> None:
        raise ge_exceptions.InvalidKeyError(message="invalid key")

    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.remove_key.side_effect = _raise_key_error

    with pytest.raises(ge_exceptions.CheckpointNotFoundError) as e:
        store.delete_checkpoint(name="my_fake_checkpoint")

    assert 'Non-existent Checkpoint configuration named "my_fake_checkpoint".' in str(
        e.value
    )


@pytest.mark.unit
def test_get_checkpoint(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock],
    checkpoint_config: dict,
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.get.return_value = checkpoint_config

    checkpoint = store.get_checkpoint(name=checkpoint_config["name"], ge_cloud_id=None)

    actual_checkpoint_config = checkpoint.to_json_dict()
    for key, val in checkpoint_config.items():
        assert val == actual_checkpoint_config.get(key)


@pytest.mark.unit
def test_get_checkpoint_with_nonexistent_checkpoint_raises_error(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    def _raise_key_error(_: Any) -> None:
        raise ge_exceptions.InvalidKeyError(message="invalid key")

    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.get.side_effect = _raise_key_error

    with pytest.raises(ge_exceptions.CheckpointNotFoundError) as e:
        store.get_checkpoint(name="my_fake_checkpoint", ge_cloud_id=None)

    assert 'Non-existent Checkpoint configuration named "my_fake_checkpoint".' in str(
        e.value
    )


@pytest.mark.unit
def test_get_checkpoint_with_invalid_checkpoint_config_raises_error(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    def _raise_validation_error(_: Any) -> None:
        raise ValidationError(message="invalid config")

    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.get.return_value = {"class_name": "Checkpoint"}

    with mock.patch(
        "great_expectations.data_context.store.CheckpointStore.deserialize",
        side_effect=_raise_validation_error,
    ), pytest.raises(ge_exceptions.InvalidCheckpointConfigError) as e:
        store.get_checkpoint(name="my_fake_checkpoint", ge_cloud_id=None)

    assert "Invalid Checkpoint configuration" in str(e.value)


@pytest.mark.unit
def test_get_checkpoint_with_invalid_legacy_checkpoint_raises_error(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.get.return_value = (
        CheckpointConfig().to_json_dict()
    )  # Defaults to empty LegacyCheckpoint

    with pytest.raises(ge_exceptions.CheckpointError) as e:
        store.get_checkpoint(name="my_checkpoint", ge_cloud_id=None)

    assert (
        "Attempt to instantiate LegacyCheckpoint with insufficient and/or incorrect arguments"
        in str(e.value)
    )


@pytest.mark.unit
def test_add_checkpoint(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, mock.MagicMock]
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend

    context = mock.MagicMock(spec=DataContext)
    context._usage_statistics_handler = mock.MagicMock()
    checkpoint = Checkpoint(name="my_checkpoint", data_context=context)

    store.add_checkpoint(checkpoint=checkpoint, name="my_checkpoint", ge_cloud_id=None)

    mock_backend.set.assert_called_once_with(
        ("my_checkpoint",),
        "name: my_checkpoint\nconfig_version:\nmodule_name: great_expectations.checkpoint\nclass_name: LegacyCheckpoint\n",
    )
