from unittest import mock

from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.rule_based_profiler.toolkit import get_profiler, list_profilers


def test_get_profiler(empty_data_context: DataContext):
    store = ProfilerStore("profiler_store")
    get_profiler(
        data_context=empty_data_context,
        profiler_store=store,
        name="my_profiler",
        ge_cloud_id=None,
    )


def test_get_profiler_in_cloud_mode():
    pass


def test_get_profiler_with_non_existent_profiler_raises_error():
    pass


def test_get_profiler_with_invalid_configuration_raises_error():
    pass


@mock.patch("great_expectations.data_context.store.ProfilerStore")
def test_list_profilers(mock_profiler_store: mock.MagicMock):
    store = mock_profiler_store()
    keys = ["a", "b", "c"]
    store.list_keys.return_value = [ConfigurationIdentifier(char) for char in keys]
    res = list_profilers(store, ge_cloud_mode=False)

    assert res == keys
    assert store.list_keys.called


@mock.patch("great_expectations.data_context.store.ProfilerStore")
def test_list_profilers_in_cloud_mode(mock_profiler_store: mock.MagicMock):
    store = mock_profiler_store()
    keys = ["a", "b", "c"]
    store.list_keys.return_value = keys
    res = list_profilers(store, ge_cloud_mode=True)

    assert res == keys
    assert store.list_keys.called
