import pytest
from pytest_mock import MockerFixture

from great_expectations.checkpoint.v1_checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.factory.checkpoint_factory import CheckpointFactory
from great_expectations.core.validation_definition import ValidationDefinition
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.store.checkpoint_store import (
    V1CheckpointStore as CheckpointStore,
)
from great_expectations.exceptions import DataContextError


@pytest.mark.unit
def test_checkpoint_factory_get_uses_store_get(mocker: MockerFixture):
    # Arrange
    name = "test-checkpoint"
    store = mocker.MagicMock(spec=CheckpointStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    checkpoint = Checkpoint(
        name=name, validation_definitions=[mocker.Mock(spec=ValidationDefinition)]
    )
    store.get.return_value = checkpoint
    factory = CheckpointFactory(store=store)

    # Act
    result = factory.get(name=name)

    # Assert
    store.get.assert_called_once_with(key=key)

    assert result == checkpoint


@pytest.mark.unit
def test_checkpoint_factory_get_raises_error_on_missing_key(mocker: MockerFixture):
    # Arrange
    name = "test-checkpoint"
    store = mocker.MagicMock(spec=CheckpointStore)
    store.has_key.return_value = False
    checkpoint = Checkpoint(
        name=name, validation_definitions=[mocker.Mock(spec=ValidationDefinition)]
    )
    store.get.return_value = checkpoint
    factory = CheckpointFactory(store=store)

    # Act
    with pytest.raises(DataContextError, match=f"Checkpoint with name {name} was not found."):
        factory.get(name=name)

    # Assert
    store.get.assert_not_called()


@pytest.mark.unit
def test_checkpoint_factory_add_uses_store_add(mocker: MockerFixture):
    # Arrange
    name = "test-checkpoint"
    store = mocker.MagicMock(spec=CheckpointStore)
    store.has_key.return_value = False
    key = store.get_key.return_value
    store.get.return_value = None
    factory = CheckpointFactory(store=store)
    checkpoint = Checkpoint(
        name=name, validation_definitions=[mocker.Mock(spec=ValidationDefinition)]
    )
    store.get.return_value = checkpoint

    # Act
    factory.add(checkpoint=checkpoint)

    # Assert
    store.add.assert_called_once_with(key=key, value=checkpoint.dict())


@pytest.mark.unit
def test_checkpoint_factory_add_raises_for_duplicate_key(mocker: MockerFixture):
    # Arrange
    name = "test-checkpoint"
    store = mocker.MagicMock(spec=CheckpointStore)
    store.has_key.return_value = True
    factory = CheckpointFactory(store=store)
    checkpoint = Checkpoint(
        name=name, validation_definitions=[mocker.Mock(spec=ValidationDefinition)]
    )

    # Act
    with pytest.raises(
        DataContextError,
        match=f"Cannot add Checkpoint with name {name} because it already exists.",
    ):
        factory.add(checkpoint=checkpoint)

    # Assert
    store.add.assert_not_called()


@pytest.mark.unit
def test_checkpoint_factory_delete_uses_store_remove_key(mocker: MockerFixture):
    # Arrange
    name = "test-checkpoint"
    store = mocker.MagicMock(spec=CheckpointStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    factory = CheckpointFactory(store=store)
    checkpoint = Checkpoint(
        name=name, validation_definitions=[mocker.Mock(spec=ValidationDefinition)]
    )

    # Act
    factory.delete(checkpoint=checkpoint)

    # Assert
    store.remove_key.assert_called_once_with(
        key=key,
    )


@pytest.mark.unit
def test_checkpoint_factory_delete_raises_for_missing_checkpoint(mocker: MockerFixture):
    # Arrange
    name = "test-checkpoint"
    store = mocker.MagicMock(spec=CheckpointStore)
    store.has_key.return_value = False
    factory = CheckpointFactory(store=store)
    checkpoint = Checkpoint(
        name=name, validation_definitions=[mocker.Mock(spec=ValidationDefinition)]
    )

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
    ds = context.sources.add_pandas("my_datasource")
    asset = ds.add_csv_asset("my_asset", "data.csv")
    batch_def = asset.add_batch_definition("my_batch_definition")
    suite = ExpectationSuite(name="my_suite")

    checkpoint = Checkpoint(
        name=name,
        validation_definitions=[
            ValidationDefinition(name="validation_def", data=batch_def, suite=suite)
        ],
    )
    with pytest.raises(DataContextError, match=f"Checkpoint with name {name} was not found."):
        context.checkpoints.get(name)

    # Act
    created_checkpoint = context.checkpoints.add(checkpoint=checkpoint)

    # Assert
    assert created_checkpoint == context.checkpoints.get(name=name)


@pytest.mark.filesystem
def test_checkpoint_factory_delete_success_filesystem(empty_data_context):
    _test_checkpoint_factory_delete_success(empty_data_context)


@pytest.mark.cloud
def test_checkpoint_factory_delete_success_cloud(empty_cloud_context_fluent):
    _test_checkpoint_factory_delete_success(empty_cloud_context_fluent)


def _test_checkpoint_factory_delete_success(context):
    # Arrange
    name = "test-checkpoint"
    ds = context.sources.add_pandas("my_datasource")
    asset = ds.add_csv_asset("my_asset", "data.csv")
    batch_def = asset.add_batch_definition("my_batch_definition")
    suite = ExpectationSuite(name="my_suite")

    checkpoint = context.checkpoints.add(
        checkpoint=Checkpoint(
            name=name,
            validation_definitions=[
                ValidationDefinition(name="validation_def", data=batch_def, suite=suite)
            ],
        )
    )

    # Act
    context.checkpoints.delete(checkpoint)

    # Assert
    with pytest.raises(
        DataContextError,
        match=f"Checkpoint with name {name} was not found.",
    ):
        context.checkpoints.get(name)


@pytest.mark.parametrize(
    "context_fixture_name",
    [
        pytest.param("empty_cloud_context_fluent", id="cloud", marks=pytest.mark.unit),
        pytest.param("in_memory_runtime_context", id="ephemeral", marks=pytest.mark.unit),
        pytest.param("empty_data_context", id="filesystem", marks=pytest.mark.filesystem),
    ],
)
def test_checkpoint_factory_all(context_fixture_name: str, request: pytest.FixtureRequest):
    context: AbstractDataContext = request.getfixturevalue(context_fixture_name)

    # Arrange
    ds = context.sources.add_pandas("my_datasource")
    asset = ds.add_csv_asset("my_asset", "data.csv")  # type: ignore[arg-type]
    batch_def = asset.add_batch_definition("my_batch_definition")
    suite = ExpectationSuite(name="my_suite")

    checkpoint_a = context.checkpoints.add(
        Checkpoint(
            name="a",
            validation_definitions=[
                ValidationDefinition(name="val def a", data=batch_def, suite=suite)
            ],
        )
    )
    checkpoint_b = context.checkpoints.add(
        Checkpoint(
            name="b",
            validation_definitions=[
                ValidationDefinition(name="val def b", data=batch_def, suite=suite)
            ],
        )
    )

    # Act
    result = context.checkpoints.all()
    result = sorted(result, key=lambda x: x.name)

    # Assert
    assert [r.name for r in result] == [checkpoint_a.name, checkpoint_b.name]
    assert result == [checkpoint_a, checkpoint_b]


class TestCheckpointFactoryAnalytics:
    # TODO: Write tests once analytics are in place
    pass
