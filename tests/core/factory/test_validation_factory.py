from unittest.mock import Mock

import pytest

from great_expectations.core.batch_config import BatchConfig
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.factory.validation_factory import ValidationFactory
from great_expectations.core.validation_config import ValidationConfig
from great_expectations.data_context.data_context.abstract_data_context import (
    AbstractDataContext,
)
from great_expectations.data_context.data_context.cloud_data_context import (
    CloudDataContext,
)
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.store.validation_config_store import (
    ValidationConfigStore,
)
from great_expectations.exceptions import DataContextError


@pytest.fixture
def validation_config() -> ValidationConfig:
    data = Mock(spec=BatchConfig)
    suite = Mock(spec=ExpectationSuite)
    return ValidationConfig(
        name="test-validation",
        data=data,
        suite=suite,
    )


@pytest.mark.unit
def test_validation_factory_get_uses_store_get(validation_config: ValidationConfig):
    # Arrange
    name = validation_config.name
    store = Mock(spec=ValidationConfigStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    store.get.return_value = validation_config
    factory = ValidationFactory(store=store)

    # Act
    result = factory.get(name=name)

    # Assert
    store.get.assert_called_once_with(key=key)
    assert result == validation_config


@pytest.mark.unit
def test_validation_factory_get_raises_error_on_missing_key(
    validation_config: ValidationConfig,
):
    # Arrange
    name = validation_config.name
    store = Mock(spec=ValidationConfigStore)
    store.has_key.return_value = False
    store.get.return_value = validation_config
    factory = ValidationFactory(store=store)

    # Act
    with pytest.raises(
        DataContextError, match=f"ValidationConfig with name {name} was not found."
    ):
        factory.get(name=name)

    # Assert
    store.get.assert_not_called()


@pytest.mark.unit
def test_validation_factory_add_uses_store_add(validation_config: ValidationConfig):
    # Arrange
    store = Mock(spec=ValidationConfigStore)
    store.has_key.return_value = False
    key = store.get_key.return_value
    factory = ValidationFactory(store=store)
    store.get.return_value = validation_config

    # Act
    factory.add(validation=validation_config)

    # Assert
    store.add.assert_called_once_with(key=key, value=validation_config)


@pytest.mark.unit
def test_validation_factory_add_raises_for_duplicate_key(
    validation_config: ValidationConfig,
):
    # Arrange
    name = validation_config.name
    store = Mock(spec=ValidationConfigStore)
    store.has_key.return_value = True
    factory = ValidationFactory(store=store)

    # Act
    with pytest.raises(
        DataContextError,
        match=f"Cannot add ValidationConfig with name {name} because it already exists.",
    ):
        factory.add(validation=validation_config)

    # Assert
    store.add.assert_not_called()


@pytest.mark.unit
def test_validation_factory_delete_uses_store_remove_key(
    validation_config: ValidationConfig,
):
    # Arrange
    store = Mock(spec=ValidationConfigStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    factory = ValidationFactory(store=store)

    # Act
    factory.delete(validation=validation_config)

    # Assert
    store.remove_key.assert_called_once_with(
        key=key,
    )


@pytest.mark.unit
def test_validation_factory_delete_raises_for_missing_validation(
    validation_config: ValidationConfig,
):
    # Arrange
    name = validation_config.name
    store = Mock(spec=ValidationConfigStore)
    store.has_key.return_value = False
    factory = ValidationFactory(store=store)

    # Act
    with pytest.raises(
        DataContextError,
        match=f"Cannot delete ValidationConfig with name {name} because it cannot be found.",
    ):
        factory.delete(validation=validation_config)

    # Assert
    store.remove_key.assert_not_called()


@pytest.mark.filesystem
def test_validation_factory_is_initialized_with_context_filesystem(
    empty_data_context: FileDataContext,
):
    assert isinstance(empty_data_context.validations, ValidationFactory)


@pytest.mark.cloud
def test_validation_factory_is_initialized_with_context_cloud(
    empty_cloud_data_context: CloudDataContext,
):
    assert isinstance(empty_cloud_data_context.validations, ValidationFactory)


@pytest.mark.filesystem
def test_validation_factory_add_success_filesystem(
    empty_data_context: FileDataContext, validation_config: ValidationConfig
):
    _test_validation_factory_add_success(
        context=empty_data_context, validation_config=validation_config
    )


@pytest.mark.cloud
def test_validation_factory_add_success_cloud(
    empty_cloud_context_fluent: CloudDataContext, validation_config: ValidationConfig
):
    _test_validation_factory_add_success(
        context=empty_cloud_context_fluent, validation_config=validation_config
    )


def _test_validation_factory_add_success(
    context: AbstractDataContext, validation_config: ValidationConfig
):
    # Arrange
    name = validation_config.name
    with pytest.raises(
        DataContextError, match=f"ValidationConfig with name {name} was not found."
    ):
        context.validations.get(name)

    # Act
    created_validation = context.validations.add(validation=validation_config)

    # Assert
    assert created_validation == context.validations.get(name=name)


@pytest.mark.filesystem
def test_validation_factory_delete_success_filesystem(
    empty_data_context: FileDataContext, validation_config: ValidationConfig
):
    _test_validation_factory_delete_success(
        context=empty_data_context, validation_config=validation_config
    )


@pytest.mark.cloud
def test_validation_factory_delete_success_cloud(
    empty_cloud_context_fluent: CloudDataContext, validation_config: ValidationConfig
):
    _test_validation_factory_delete_success(
        context=empty_cloud_context_fluent, validation_config=validation_config
    )


def _test_validation_factory_delete_success(
    context: AbstractDataContext, validation_config: ValidationConfig
):
    # Arrange
    name = validation_config.name
    validation_config = context.validations.add(validation=validation_config)

    # Act
    context.validations.delete(validation_config)

    # Assert
    with pytest.raises(
        DataContextError,
        match=f"ValidationConfig with name {name} was not found.",
    ):
        context.validations.get(name)


class TestValidationFactoryAnalytics:
    # TODO: Write tests once analytics are in place
    pass
