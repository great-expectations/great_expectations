from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union
from unittest.mock import ANY as MOCK_ANY

import pytest
from marshmallow.exceptions import ValidationError

import great_expectations.exceptions as gx_exceptions
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.data_context.cloud_constants import GXCloudRESTResource
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store import CheckpointStore
from great_expectations.data_context.types.base import CheckpointConfig
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
    GXCloudIdentifier,
)
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import filter_properties_dict, gen_directory_tree_str
from tests.test_utils import build_checkpoint_store_using_filesystem

if TYPE_CHECKING:
    from unittest.mock import MagicMock  # noqa: TID251

    from pytest_mock import MockerFixture

logger = logging.getLogger(__name__)


@pytest.fixture
def checkpoint_store_with_mock_backend(
    mocker: MockerFixture,
) -> Tuple[CheckpointStore, MagicMock]:
    store = CheckpointStore(store_name="checkpoint_store")
    mock_backend = mocker.MagicMock()
    store._store_backend = mock_backend

    return store, mock_backend


@pytest.mark.filesystem
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
        checkpoint_store.set(key="my_first_checkpoint", value="this is not a checkpoint")

    assert len(checkpoint_store.list_keys()) == 0

    checkpoint_name_0: str = "my_checkpoint_0"
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
                    "name": "update_data_docs",
                    "action": {
                        "class_name": "UpdateDataDocsAction",
                    },
                },
            ],
        }
    ]
    expectation_suite_name_0: str = "my.test.expectation_suite.name"
    suite_parameters_0: dict = {
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
        expectation_suite_name=expectation_suite_name_0,
        suite_parameters=suite_parameters_0,
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
    suite_parameters_1: dict = {
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
        expectation_suite_name=expectation_suite_name_1,
        batch_request=batch_request_1,
        suite_parameters=suite_parameters_1,
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

    checkpoint_store.remove_key(key=key_0)
    checkpoint_store.remove_key(key=key_1)
    assert len(checkpoint_store.list_keys()) == 0


@pytest.mark.cloud
@pytest.mark.parametrize(
    "response_json, expected, error_type",
    [
        pytest.param(
            {
                "data": {
                    "id": "7b5e962c-3c67-4a6d-b311-b48061d52103",
                    "attributes": {
                        "checkpoint_config": {
                            "name": "oss_test_checkpoint",
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
                    },
                }
            },
            {
                "expectation_suite_name": "oss_test_expectation_suite",
                "id": "7b5e962c-3c67-4a6d-b311-b48061d52103",
                "name": "oss_test_checkpoint",
                "validations": [
                    {"expectation_suite_name": "taxi.demo_pass"},
                    {
                        "batch_request": {
                            "data_asset_name": "users",
                            "data_connector_name": "oss_test_data_connector",
                            "datasource_name": "oss_test_datasource",
                        },
                    },
                ],
            },
            None,
            id="single_config",
        ),
        pytest.param({"data": []}, None, ValueError, id="empty_payload"),
        pytest.param(
            {
                "data": [
                    {
                        "id": "7b5e962c-3c67-4a6d-b311-b48061d52103",
                        "attributes": {
                            "checkpoint_config": {
                                "name": "oss_test_checkpoint",
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
                        },
                    }
                ]
            },
            {
                "expectation_suite_name": "oss_test_expectation_suite",
                "id": "7b5e962c-3c67-4a6d-b311-b48061d52103",
                "name": "oss_test_checkpoint",
                "validations": [
                    {"expectation_suite_name": "taxi.demo_pass"},
                    {
                        "batch_request": {
                            "data_asset_name": "users",
                            "data_connector_name": "oss_test_data_connector",
                            "datasource_name": "oss_test_datasource",
                        },
                    },
                ],
            },
            None,
            id="single_config_in_list",
        ),
    ],
)
def test_gx_cloud_response_json_to_object_dict(
    response_json: dict, expected: dict | None, error_type: type[Exception] | None
) -> None:
    if error_type:
        with pytest.raises(error_type):
            _ = CheckpointStore.gx_cloud_response_json_to_object_dict(response_json)
    else:
        actual = CheckpointStore.gx_cloud_response_json_to_object_dict(response_json)
        assert actual == expected


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
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.list_keys.return_value = [("a", "b", "c"), ("d", "e", "f")]

    checkpoints = store.list_checkpoints(ge_cloud_mode=False)
    assert checkpoints == ["a.b.c", "d.e.f"]


@pytest.mark.cloud
def test_list_checkpoints_cloud_mode(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
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
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend

    store.delete_checkpoint(name="my_checkpoint")

    mock_backend.remove_key.assert_called_once_with(ConfigurationIdentifier("my_checkpoint"))


@pytest.mark.cloud
def test_delete_checkpoint_with_cloud_id(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend

    store.delete_checkpoint(id="abc123")

    mock_backend.remove_key.assert_called_once_with(
        GXCloudIdentifier(resource_type=GXCloudRESTResource.CHECKPOINT, id="abc123")
    )


@pytest.mark.unit
def test_delete_checkpoint_with_invalid_key_raises_error(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
) -> None:
    def _raise_key_error(_: Any) -> None:
        raise gx_exceptions.InvalidKeyError(message="invalid key")

    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.remove_key.side_effect = _raise_key_error

    with pytest.raises(gx_exceptions.CheckpointNotFoundError) as e:
        store.delete_checkpoint(name="my_fake_checkpoint")

    assert 'Non-existent Checkpoint configuration named "my_fake_checkpoint".' in str(e.value)


@pytest.mark.unit
def test_get_checkpoint(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
    checkpoint_config: dict,
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.get.return_value = checkpoint_config

    checkpoint = store.get_checkpoint(name=checkpoint_config["name"], id=None)

    actual_checkpoint_config = checkpoint.to_json_dict()
    for key, val in checkpoint_config.items():
        assert val == actual_checkpoint_config.get(key)


@pytest.mark.unit
def test_get_checkpoint_with_nonexistent_checkpoint_raises_error(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
) -> None:
    def _raise_key_error(_: Any) -> None:
        raise gx_exceptions.InvalidKeyError(message="invalid key")

    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.get.side_effect = _raise_key_error

    with pytest.raises(gx_exceptions.CheckpointNotFoundError) as e:
        store.get_checkpoint(name="my_fake_checkpoint", id=None)

    assert 'Non-existent Checkpoint configuration named "my_fake_checkpoint".' in str(e.value)


@pytest.mark.unit
def test_get_checkpoint_with_invalid_checkpoint_config_raises_error(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
    mocker: MockerFixture,
) -> None:
    def _raise_validation_error(_: Any) -> None:
        raise ValidationError(message="invalid config")

    store, mock_backend = checkpoint_store_with_mock_backend
    mock_backend.get.return_value = {"class_name": "Checkpoint"}

    with mocker.patch(
        "great_expectations.data_context.store.CheckpointStore.deserialize",
        side_effect=_raise_validation_error,
    ), pytest.raises(gx_exceptions.InvalidCheckpointConfigError) as e:
        store.get_checkpoint(name="my_fake_checkpoint", id=None)

    assert "Invalid Checkpoint configuration" in str(e.value)


@pytest.mark.unit
def test_add_checkpoint(
    checkpoint_store_with_mock_backend: Tuple[CheckpointStore, MagicMock],
    mocker: MockerFixture,
) -> None:
    store, mock_backend = checkpoint_store_with_mock_backend

    context = mocker.MagicMock(spec=FileDataContext)
    checkpoint_name = "my_checkpoint"
    checkpoint = Checkpoint(name=checkpoint_name, data_context=context)

    store.add_checkpoint(checkpoint=checkpoint)

    mock_backend.add.assert_called_once_with(
        (checkpoint_name,),
        MOCK_ANY,  # Complex serialized payload so keeping it simple
    )
