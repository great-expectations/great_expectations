from unittest.mock import Mock

import pytest

from great_expectations import set_context
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.factory.checkpoint_factory import CheckpointFactory
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.store.checkpoint_store import CheckpointStore
from great_expectations.exceptions import DataContextError


@pytest.fixture
def checkpoint_dict():
    return {
        "name": "oss_test_checkpoint",
        "expectation_suite_name": "oss_test_expectation_checkpoint",
        "validations": [
            {
                "name": None,
                "id": None,
                "expectation_checkpoint_name": "taxi.demo_pass",
                "expectation_checkpoint_ge_cloud_id": None,
                "batch_request": None,
            },
        ],
        "action_list": [
            {
                "action": {"class_name": "StoreValidationResultAction"},
                "name": "store_validation_result",
            },
        ],
    }


def _assert_checkpoint_equality(actual: Checkpoint, expected: Checkpoint):
    # Checkpoint equality is currently defined as equality of the config
    # TODO: We should change this to a more robust comparison (instead of memory addresses)
    actual_config = actual.config.to_json_dict()
    expected_config = expected.config.to_json_dict()
    assert actual_config == expected_config


@pytest.mark.unit
def test_checkpoint_factory_get_uses_store_get(checkpoint_dict: dict):
    # Arrange
    name = "test-checkpoint"
    store = Mock(spec=CheckpointStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    store.get.return_value = checkpoint_dict
    context = Mock(spec=AbstractDataContext)
    factory = CheckpointFactory(store=store, context=context)
    set_context(context)

    # Act
    result = factory.get(name=name)

    # Assert
    store.get.assert_called_once_with(key=key)
    _assert_checkpoint_equality(actual=result, expected=Checkpoint(**checkpoint_dict))


@pytest.mark.unit
def test_checkpoint_factory_get_raises_error_on_missing_key(checkpoint_dict: dict):
    # Arrange
    name = "test-checkpoint"
    store = Mock(spec=CheckpointStore)
    store.has_key.return_value = False
    store.get.return_value = checkpoint_dict
    context = Mock(spec=AbstractDataContext)
    factory = CheckpointFactory(store=store, context=context)
    set_context(context)

    # Act
    with pytest.raises(
        DataContextError, match=f"Checkpoint with name {name} was not found."
    ):
        factory.get(name=name)

    # Assert
    store.get.assert_not_called()


@pytest.mark.unit
def test_checkpoint_factory_add_uses_store_add():
    # Arrange
    name = "test-checkpoint"
    store = Mock(spec=CheckpointStore)
    store.has_key.return_value = False
    key = store.get_key.return_value
    context = Mock(spec=AbstractDataContext)
    factory = CheckpointFactory(store=store, context=context)
    set_context(context)
    checkpoint = Checkpoint(name=name)
    config = checkpoint.get_config()
    store.get.return_value = config

    # Act
    factory.add(checkpoint=checkpoint)

    # Assert
    store.add.assert_called_once_with(key=key, value=config)


@pytest.mark.unit
def test_checkpoint_factory_add_raises_for_duplicate_key():
    # Arrange
    name = "test-checkpoint"
    store = Mock(spec=CheckpointStore)
    store.has_key.return_value = True
    context = Mock(spec=AbstractDataContext)
    factory = CheckpointFactory(store=store, context=context)
    set_context(context)
    checkpoint = Checkpoint(name=name)

    # Act
    with pytest.raises(
        DataContextError,
        match=f"Cannot add Checkpoint with name {name} because it already exists.",
    ):
        factory.add(checkpoint=checkpoint)

    # Assert
    store.add.assert_not_called()


@pytest.mark.unit
def test_checkpoint_factory_delete_uses_store_remove_key():
    # Arrange
    name = "test-checkpoint"
    store = Mock(spec=CheckpointStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    context = Mock(spec=AbstractDataContext)
    factory = CheckpointFactory(store=store, context=context)
    set_context(context)
    checkpoint = Checkpoint(name=name)

    # Act
    factory.delete(checkpoint=checkpoint)

    # Assert
    store.remove_key.assert_called_once_with(
        key=key,
    )


@pytest.mark.unit
def test_checkpoint_factory_delete_raises_for_missing_checkpoint():
    # Arrange
    name = "test-checkpoint"
    store = Mock(spec=CheckpointStore)
    store.has_key.return_value = False
    context = Mock(spec=AbstractDataContext)
    factory = CheckpointFactory(store=store, context=context)
    set_context(context)
    checkpoint = Checkpoint(name=name)

    # Act
    with pytest.raises(
        DataContextError,
        match=f"Cannot delete Checkpoint with name {name} because it cannot be found.",
    ):
        factory.delete(checkpoint=checkpoint)

    # Assert
    store.remove_key.assert_not_called()


@pytest.mark.filesystem
def test_checkpoint_factory_is_initialized_with_context_filesystem(empty_data_context):
    assert isinstance(empty_data_context.checkpoints, CheckpointFactory)


@pytest.mark.cloud
def test_checkpoint_factory_is_initialized_with_context_cloud(empty_cloud_data_context):
    assert isinstance(empty_cloud_data_context.checkpoints, CheckpointFactory)


@pytest.mark.filesystem
def test_checkpoint_factory_add_success_filesystem(empty_data_context):
    _test_checkpoint_factory_add_success(empty_data_context)


@pytest.mark.cloud
def test_checkpoint_factory_add_success_cloud(empty_cloud_context_fluent):
    _test_checkpoint_factory_add_success(empty_cloud_context_fluent)


def _test_checkpoint_factory_add_success(context):
    # Arrange
    name = "test-checkpoint"
    checkpoint = Checkpoint(name=name)
    with pytest.raises(
        DataContextError, match=f"Checkpoint with name {name} was not found."
    ):
        context.checkpoints.get(name)

    # Act
    created_checkpoint = context.checkpoints.add(checkpoint=checkpoint)

    # Assert
    _assert_checkpoint_equality(
        actual=created_checkpoint, expected=context.checkpoints.get(name=name)
    )


@pytest.mark.filesystem
def test_checkpoint_factory_delete_success_filesystem(empty_data_context):
    _test_checkpoint_factory_delete_success(empty_data_context)


@pytest.mark.cloud
def test_checkpoint_factory_delete_success_cloud(empty_cloud_context_fluent):
    _test_checkpoint_factory_delete_success(empty_cloud_context_fluent)


def _test_checkpoint_factory_delete_success(context):
    # Arrange
    name = "test-checkpoint"
    checkpoint = Checkpoint(name=name)
    checkpoint = context.checkpoints.add(checkpoint=checkpoint)

    # Act
    context.checkpoints.delete(checkpoint)

    # Assert
    with pytest.raises(
        DataContextError,
        match=f"Checkpoint with name {name} was not found.",
    ):
        context.checkpoints.get(name)


class TestCheckpointFactoryAnalytics:
    # TODO: Write tests once analytics are in place
    pass
