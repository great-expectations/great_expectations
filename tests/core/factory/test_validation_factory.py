import json

import pytest
from pytest_mock import MockerFixture

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
def validation_config(mocker: MockerFixture) -> ValidationConfig:
    data = mocker.Mock(spec=BatchConfig)
    suite = mocker.Mock(spec=ExpectationSuite)
    return ValidationConfig(
        name="test-validation",
        data=data,
        suite=suite,
    )


@pytest.fixture
def validation_config_json(validation_config: ValidationConfig) -> str:
    return json.dumps(
        {
            "name": validation_config.name,
            "id": None,
            "data": {
                "datasource": {
                    "name": "test-datasource",
                    "id": "f4b3d8f2-7e4d-4f0b-9b90-6f711d0e3e2f",
                },
                "asset": {
                    "name": "test-asset",
                    "id": "10b7e57d-958b-4d28-aa1d-e89bc0401eea",
                },
                "batch_config": {
                    "name": "test-batch-config",
                    "id": "bcd13e3e-3e3e-4e3e-8e3e-3e3e3e3e3e3e",
                },
            },
            "suite": {
                "name": "test-expectation-suite",
                "id": "06022df2-6699-49b2-8c03-df77678363a0",
            },
        }
    )


@pytest.mark.unit
def test_validation_factory_get_uses_store_get(
    mocker: MockerFixture, validation_config: ValidationConfig
):
    # Arrange
    name = validation_config.name
    store = mocker.Mock(spec=ValidationConfigStore)
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
    mocker: MockerFixture,
    validation_config: ValidationConfig,
):
    # Arrange
    name = validation_config.name
    store = mocker.Mock(spec=ValidationConfigStore)
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
def test_validation_factory_add_uses_store_add(
    mocker: MockerFixture, validation_config: ValidationConfig
):
    # Arrange
    store = mocker.Mock(spec=ValidationConfigStore)
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
    mocker: MockerFixture,
    validation_config: ValidationConfig,
):
    # Arrange
    name = validation_config.name
    store = mocker.Mock(spec=ValidationConfigStore)
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
    mocker: MockerFixture,
    validation_config: ValidationConfig,
):
    # Arrange
    store = mocker.Mock(spec=ValidationConfigStore)
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
    mocker: MockerFixture,
    validation_config: ValidationConfig,
):
    # Arrange
    name = validation_config.name
    store = mocker.Mock(spec=ValidationConfigStore)
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
    empty_data_context: FileDataContext,
    validation_config: ValidationConfig,
    validation_config_json: str,
    mocker: MockerFixture,
):
    _test_validation_factory_add_success(
        mocker=mocker,
        context=empty_data_context,
        validation_config=validation_config,
        validation_config_json=validation_config_json,
    )


@pytest.mark.cloud
def test_validation_factory_add_success_cloud(
    empty_cloud_context_fluent: CloudDataContext,
    validation_config: ValidationConfig,
    validation_config_json: str,
    mocker: MockerFixture,
):
    _test_validation_factory_add_success(
        mocker=mocker,
        context=empty_cloud_context_fluent,
        validation_config=validation_config,
        validation_config_json=validation_config_json,
    )


def _test_validation_factory_add_success(
    mocker: MockerFixture,
    context: AbstractDataContext,
    validation_config: ValidationConfig,
    validation_config_json: str,
):
    # Arrange
    name = validation_config.name
    with pytest.raises(
        DataContextError, match=f"ValidationConfig with name {name} was not found."
    ):
        context.validations.get(name)

    # Act
    with mocker.patch.object(
        ValidationConfig, "json", return_value=validation_config_json
    ):
        created_validation = context.validations.add(validation=validation_config)

    # Assert
    validation_names = {
        key.to_tuple()[0] for key in context.validation_config_store.list_keys()
    }
    assert created_validation.name in validation_names


@pytest.mark.filesystem
def test_validation_factory_delete_success_filesystem(
    empty_data_context: FileDataContext,
    validation_config: ValidationConfig,
    validation_config_json: str,
    mocker: MockerFixture,
):
    _test_validation_factory_delete_success(
        mocker=mocker,
        context=empty_data_context,
        validation_config=validation_config,
        validation_config_json=validation_config_json,
    )


@pytest.mark.cloud
def test_validation_factory_delete_success_cloud(
    empty_cloud_context_fluent: CloudDataContext,
    validation_config: ValidationConfig,
    validation_config_json: str,
    mocker: MockerFixture,
):
    _test_validation_factory_delete_success(
        mocker=mocker,
        context=empty_cloud_context_fluent,
        validation_config=validation_config,
        validation_config_json=validation_config_json,
    )


def _test_validation_factory_delete_success(
    mocker: MockerFixture,
    context: AbstractDataContext,
    validation_config: ValidationConfig,
    validation_config_json: str,
):
    # Arrange
    name = validation_config.name

    with mocker.patch.object(
        ValidationConfig, "json", return_value=validation_config_json
    ):
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
