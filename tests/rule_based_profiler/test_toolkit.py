from unittest import mock

from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.rule_based_profiler.toolkit import list_profilers


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
