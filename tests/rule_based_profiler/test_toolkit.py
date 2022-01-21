from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.rule_based_profiler.toolkit import get_profiler, list_profilers


@pytest.fixture(scope="function")
def profiler_store():
    return ProfilerStore("profiler_store")


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler(mock_data_context: mock.MagicMock):
    pass
    # res = get_profiler(
    #     data_context=empty_data_context,
    #     profiler_store=store,
    #     name="my_profiler",
    #     ge_cloud_id=None,
    # )


def test_get_profiler_in_cloud_mode():
    pass


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_non_existent_profiler_raises_error(
    mock_data_context: mock.MagicMock, profiler_store: ProfilerStore
):
    with pytest.raises(ge_exceptions.ProfilerNotFoundError) as e:
        get_profiler(
            data_context=mock_data_context,
            profiler_store=profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert "Non-existent Profiler" in str(e.value)


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
