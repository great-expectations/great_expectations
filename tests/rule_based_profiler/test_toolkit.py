from unittest import mock

import pytest

import great_expectations.exceptions as ge_exceptions
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.rule_based_profiler import RuleBasedProfiler
from great_expectations.rule_based_profiler.toolkit import (
    delete_profiler,
    get_profiler,
    list_profilers,
)
from great_expectations.rule_based_profiler.types.base import RuleBasedProfilerConfig


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_with_too_many_args_raises_error(
    mock_data_context: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(AssertionError) as e:
        get_profiler(
            data_context=mock_data_context,
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id="my_ge_cloud_id",
        )

    assert "either name or ge_cloud_id" in str(e.value)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler(
    mock_data_context: mock.MagicMock,
    populated_profiler_store: ProfilerStore,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    with mock.patch(
        "great_expectations.data_context.store.profiler_store.ProfilerStore.get",
        return_value=profiler_config_with_placeholder_args,
    ):
        profiler = get_profiler(
            data_context=mock_data_context,
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert isinstance(profiler, RuleBasedProfiler)


@mock.patch("great_expectations.data_context.data_context.DataContext")
def test_get_profiler_non_existent_profiler_raises_error(
    mock_data_context: mock.MagicMock, empty_profiler_store: ProfilerStore
):
    with pytest.raises(ge_exceptions.ProfilerNotFoundError) as e:
        get_profiler(
            data_context=mock_data_context,
            profiler_store=empty_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert "Non-existent Profiler" in str(e.value)


def test_delete_profiler(
    populated_profiler_store: ProfilerStore,
):
    with mock.patch(
        "great_expectations.data_context.store.profiler_store.ProfilerStore.remove_key",
    ) as mock_remove_key:
        delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id=None,
        )

    assert mock_remove_key.call_count == 1
    assert mock_remove_key.call_args == mock.call(
        key=ConfigurationIdentifier("my_profiler")
    )


def test_delete_profiler_with_too_many_args_raises_error(
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(AssertionError) as e:
        delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_profiler",
            ge_cloud_id="my_ge_cloud_id",
        )

    assert "either name or ge_cloud_id" in str(e.value)


def test_delete_profiler_non_existent_profiler_raises_error(
    populated_profiler_store: ProfilerStore,
):
    with pytest.raises(ge_exceptions.ProfilerNotFoundError) as e:
        delete_profiler(
            profiler_store=populated_profiler_store,
            name="my_non_existent_profiler",
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
