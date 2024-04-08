from unittest import mock
from unittest.mock import Mock  # noqa: TID251

import pytest

from great_expectations import set_context
from great_expectations.analytics.events import (
    ExpectationSuiteCreatedEvent,
    ExpectationSuiteDeletedEvent,
)
from great_expectations.core import ExpectationSuite
from great_expectations.core.factory.suite_factory import SuiteFactory
from great_expectations.data_context import AbstractDataContext
from great_expectations.data_context.store import ExpectationsStore
from great_expectations.exceptions import DataContextError


@pytest.mark.unit
def test_suite_factory_get_uses_store_get():
    # Arrange
    name = "test-suite"
    store = Mock(spec=ExpectationsStore)
    store.has_key.return_value = True
    key = store.get_key.return_value
    suite_dict = {"name": name, "id": "3a758816-64c8-46cb-8f7e-03c12cea1d67"}
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
    suite_dict = {"name": name, "id": "3a758816-64c8-46cb-8f7e-03c12cea1d67"}
    store.get.return_value = suite_dict
    factory = SuiteFactory(store=store, include_rendered_content=False)
    context = Mock(spec=AbstractDataContext)
    set_context(context)

    # Act
    with pytest.raises(DataContextError, match=f"ExpectationSuite with name {name} was not found."):
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
def test_suite_factory_is_initialized_with_context_filesystem(empty_data_context):
    assert isinstance(empty_data_context.suites, SuiteFactory)


@pytest.mark.cloud
def test_suite_factory_is_initialized_with_context_cloud(empty_cloud_data_context):
    assert isinstance(empty_cloud_data_context.suites, SuiteFactory)


@pytest.mark.filesystem
def test_suite_factory_add_success_filesystem(empty_data_context):
    _test_suite_factory_add_success(empty_data_context)


@pytest.mark.filesystem
def test_suite_factory_add_success_cloud(empty_cloud_context_fluent):
    _test_suite_factory_add_success(empty_cloud_context_fluent)


def _test_suite_factory_add_success(context):
    # Arrange
    name = "test-suite"
    suite = ExpectationSuite(name=name)
    with pytest.raises(DataContextError, match=f"ExpectationSuite with name {name} was not found."):
        context.suites.get(name)
    # Act
    created_suite = context.suites.add(suite=suite)
    # Assert
    assert context.suites.get(name=name) == created_suite


@pytest.mark.filesystem
def test_suite_factory_delete_success_filesystem(empty_data_context):
    _test_suite_factory_delete_success(empty_data_context)


@pytest.mark.cloud
def test_suite_factory_delete_success_cloud(empty_cloud_context_fluent):
    _test_suite_factory_delete_success(empty_cloud_context_fluent)


def _test_suite_factory_delete_success(context):
    # Arrange
    name = "test-suite"
    suite = ExpectationSuite(name=name)
    suite = context.suites.add(suite=suite)
    # Act
    context.suites.delete(suite)
    # Assert
    with pytest.raises(
        DataContextError,
        match=f"ExpectationSuite with name {name} was not found.",
    ):
        context.suites.get(name)


@pytest.mark.parametrize(
    "context_fixture_name",
    [
        pytest.param("empty_cloud_context_fluent", id="cloud", marks=pytest.mark.cloud),
        pytest.param("in_memory_runtime_context", id="ephemeral", marks=pytest.mark.big),
        pytest.param("empty_data_context", id="filesystem", marks=pytest.mark.filesystem),
    ],
)
def test_suite_factory_all(context_fixture_name: str, request: pytest.FixtureRequest):
    context: AbstractDataContext = request.getfixturevalue(context_fixture_name)

    # Arrange
    suite_a = ExpectationSuite(name="a suite")
    suite_b = ExpectationSuite(name="b suite")

    context.suites.add(suite=suite_a)
    context.suites.add(suite=suite_b)

    # Act
    result = context.suites.all()
    result = sorted(result, key=lambda x: x.name)

    # Assert
    assert [r.name for r in result] == [suite_a.name, suite_b.name]
    assert result == [suite_a, suite_b]


class TestSuiteFactoryAnalytics:
    @pytest.mark.filesystem
    def test_suite_factory_add_emits_event_filesystem(self, empty_data_context):
        self._test_suite_factory_add_emits_event(empty_data_context)

    @pytest.mark.cloud
    def test_suite_factory_add_emits_event_cloud(self, empty_cloud_context_fluent):
        self._test_suite_factory_add_emits_event(empty_cloud_context_fluent)

    def _test_suite_factory_add_emits_event(self, context):
        # Arrange
        name = "test-suite"
        suite = ExpectationSuite(name=name)

        # Act
        with mock.patch(
            "great_expectations.core.factory.suite_factory.submit_event", autospec=True
        ) as mock_submit:
            _ = context.suites.add(suite=suite)

        # Assert
        mock_submit.assert_called_once_with(
            event=ExpectationSuiteCreatedEvent(expectation_suite_id=suite.id)
        )

    @pytest.mark.filesystem
    def test_suite_factory_delete_emits_event_filesystem(self, empty_data_context):
        self._test_suite_factory_delete_emits_event(empty_data_context)

    @pytest.mark.cloud
    def test_suite_factory_delete_emits_event_cloud(self, empty_cloud_context_fluent):
        self._test_suite_factory_delete_emits_event(empty_cloud_context_fluent)

    def _test_suite_factory_delete_emits_event(self, context):
        # Arrange
        name = "test-suite"
        suite = ExpectationSuite(name=name)
        suite = context.suites.add(suite=suite)

        # Act
        with mock.patch(
            "great_expectations.core.factory.suite_factory.submit_event", autospec=True
        ) as mock_submit:
            context.suites.delete(suite=suite)

        # Assert
        mock_submit.assert_called_once_with(
            event=ExpectationSuiteDeletedEvent(expectation_suite_id=suite.id)
        )
