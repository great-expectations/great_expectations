import pytest

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.rule_based_profiler.config.base import RuleBasedProfilerConfig


@pytest.fixture(scope="function")
def profiler_name() -> str:
    return "my_first_profiler"


@pytest.fixture(scope="function")
def store_name() -> str:
    return "profiler_store"


@pytest.fixture(scope="function")
def profiler_config(profiler_name: str) -> RuleBasedProfilerConfig:
    return RuleBasedProfilerConfig(name=profiler_name, config_version=1.0, rules={})


@pytest.fixture(scope="function")
def empty_profiler_store(store_name: str) -> ProfilerStore:
    return ProfilerStore(store_name)


@pytest.fixture(scope="function")
def profiler_key(profiler_name: str) -> ConfigurationIdentifier:
    return ConfigurationIdentifier(configuration_key=profiler_name)


@pytest.fixture(scope="function")
def populated_profiler_store(
    empty_profiler_store: ProfilerStore,
    profiler_config: RuleBasedProfilerConfig,
    profiler_key: ConfigurationIdentifier,
) -> ProfilerStore:
    profiler_store = empty_profiler_store
    profiler_store.set(key=profiler_key, value=profiler_config)
    return profiler_store


def test_profiler_store_raises_error_with_invalid_value(
    empty_profiler_store: ProfilerStore,
):
    with pytest.raises(TypeError):
        empty_profiler_store.set(
            key="my_first_profiler", value="this is not a profiler"
        )


def test_profiler_store_set_adds_valid_key(
    empty_profiler_store: ProfilerStore,
    profiler_config: RuleBasedProfilerConfig,
    profiler_name: str,
):
    key = ConfigurationIdentifier(configuration_key=profiler_name)
    assert len(empty_profiler_store.list_keys()) == 0
    empty_profiler_store.set(key=key, value=profiler_config)
    assert len(empty_profiler_store.list_keys()) == 1


def test_profiler_store_remove_key_deletes_value(
    populated_profiler_store: ProfilerStore, profiler_key: ConfigurationIdentifier
):
    assert len(populated_profiler_store.list_keys()) == 1
    populated_profiler_store.remove_key(key=profiler_key)
    assert len(populated_profiler_store.list_keys()) == 0


def test_profiler_store_self_check_report(
    populated_profiler_store: ProfilerStore,
):
    data = populated_profiler_store.self_check()
    self_check_report = convert_to_json_serializable(data=data)
    assert self_check_report == {}


def test_profiler_store_alters_filesystem():
    pass
