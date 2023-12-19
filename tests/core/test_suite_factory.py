from unittest.mock import Mock

import pytest

from great_expectations import set_context
from great_expectations.core import ExpectationSuite
from great_expectations.core.suite_factory import SuiteFactory
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.exceptions import DataContextError


@pytest.mark.unit
def test_suite_factory_get_uses_store_get():
    # Arrange
    name = "test-suite"
    store = Mock(spec=ExpectationsStore)
    store.has_key.return_value = True
    key = store.get_key_by_name.return_value
    suite_dict = {"name": name, "ge_cloud_id": "3a758816-64c8-46cb-8f7e-03c12cea1d67"}
    store.get.return_value = suite_dict
    factory = SuiteFactory(store=store, include_rendered_content=False)
    context = Mock(spec=AbstractDataContext)
    set_context(context)

    # Act
    result = factory.get(name=name)

    # Assert
    store.get.assert_called_once_with(key=key)
    assert result == ExpectationSuite(name=name)


@pytest.mark.unit
def test_suite_factory_get_raises_error_on_missing_key():
    # Arrange
    name = "test-suite"
    store = Mock(spec=ExpectationsStore)
    store.has_key.return_value = False
    suite_dict = {"name": name, "ge_cloud_id": "3a758816-64c8-46cb-8f7e-03c12cea1d67"}
    store.get.return_value = suite_dict
    factory = SuiteFactory(store=store, include_rendered_content=False)
    context = Mock(spec=AbstractDataContext)
    set_context(context)

    # Act
    with pytest.raises(
        DataContextError, match=f"ExpectationSuite `{name}` was not found."
    ):
        factory.get(name=name)

    # Assert
    store.get.assert_not_called()


@pytest.mark.unit
def test_suite_factory_add_uses_store_add():
    # Arrange
    name = "test-suite"
    store = Mock(spec=ExpectationsStore)
    store.has_key.return_value = False
    key = store.get_key.return_value
    factory = SuiteFactory(store=store, include_rendered_content=False)
    context = Mock(spec=AbstractDataContext)
    set_context(context)
    suite = ExpectationSuite(name=name)

    # Act
    factory.add(suite=suite)

    # Assert
    store.add.assert_called_once_with(key=key, value=suite)


@pytest.mark.unit
def test_suite_factory_add_raises_for_duplicate_key():
    # Arrange
    name = "test-suite"
    store = Mock(spec=ExpectationsStore)
    store.has_key.return_value = True
    factory = SuiteFactory(store=store, include_rendered_content=False)
    context = Mock(spec=AbstractDataContext)
    set_context(context)
    suite = ExpectationSuite(name=name)

    # Act
    with pytest.raises(
        DataContextError,
        match=f"Cannot add ExpectationSuite with name {suite.name} because it already exists.",
    ):
        factory.add(suite=suite)

    # Assert
    store.add.assert_not_called()


@pytest.mark.unit
def test_suite_factory_delete_uses_store_remove_key():
    # Arrange
    name = "test-suite"
    store = Mock(spec=ExpectationsStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    factory = SuiteFactory(store=store, include_rendered_content=False)
    context = Mock(spec=AbstractDataContext)
    set_context(context)
    suite = ExpectationSuite(name=name)

    # Act
    factory.delete(suite=suite)

    # Assert
    store.remove_key.assert_called_once_with(
        key=key,
    )


@pytest.mark.unit
def test_suite_factory_delete_raises_for_missing_suite():
    # Arrange
    name = "test-suite"
    store = Mock(spec=ExpectationsStore)
    store.has_key.return_value = False
    factory = SuiteFactory(store=store, include_rendered_content=False)
    context = Mock(spec=AbstractDataContext)
    set_context(context)
    suite = ExpectationSuite(name=name)

    # Act
    with pytest.raises(
        DataContextError,
        match=f"Cannot delete ExpectationSuite with name {suite.name} because it cannot be found.",
    ):
        factory.delete(suite=suite)

    # Assert
    store.remove_key.assert_not_called()


@pytest.mark.filesystem
def test_suite_factory_is_initialized_with_context(empty_data_context):
    assert isinstance(empty_data_context.suites, SuiteFactory)


@pytest.mark.filesystem
def test_suite_factory_add_success_filesystem(empty_data_context):
    # Arrange
    name = "test-suite"
    suite = ExpectationSuite(name=name)
    with pytest.raises(
        DataContextError, match=f"ExpectationSuite `{name}` was not found."
    ):
        empty_data_context.suites.get(name)

    # Act
    created_suite = empty_data_context.suites.add(suite=suite)

    # Assert
    assert empty_data_context.suites.get(name=name) == created_suite


@pytest.mark.filesystem
def test_suite_factory_add_success_cloud(empty_cloud_context_fluent):
    # Arrange
    name = "test-suite"
    suite = ExpectationSuite(name=name)
    with pytest.raises(
        DataContextError, match=f"ExpectationSuite `{name}` was not found."
    ):
        empty_cloud_context_fluent.suites.get(name)

    # Act
    created_suite = empty_cloud_context_fluent.suites.add(suite=suite)

    # Assert
    assert empty_cloud_context_fluent.suites.get(name=name) == created_suite


@pytest.mark.filesystem
def test_suite_factory_delete_success_filesystem(empty_data_context):
    # Arrange
    name = "test-suite"
    suite = ExpectationSuite(name=name)
    suite = empty_data_context.suites.add(suite=suite)

    # Act
    empty_data_context.suites.delete(suite)

    # Assert
    with pytest.raises(
        DataContextError, match=f"ExpectationSuite `{name}` was not found."
    ):
        empty_data_context.suites.get(name)


@pytest.mark.cloud
def test_suite_factory_delete_success_cloud(empty_cloud_context_fluent):
    # Arrange
    name = "test-suite"
    suite = ExpectationSuite(name=name)
    suite = empty_cloud_context_fluent.suites.add(suite=suite)

    # Act
    empty_cloud_context_fluent.suites.delete(suite)

    # Assert
    with pytest.raises(
        DataContextError, match=f"ExpectationSuite `{name}` was not found."
    ):
        empty_cloud_context_fluent.suites.get(name)
