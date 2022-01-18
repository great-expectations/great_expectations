from pathlib import Path
from typing import Dict

import pytest

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.rule_based_profiler.config.base import (
    DomainBuilderConfig,
    ExpectationConfigurationBuilderConfig,
    ParameterBuilderConfig,
    RuleBasedProfilerConfig,
    RuleConfig,
)
from tests.test_utils import build_profiler_store_using_filesystem


@pytest.fixture(scope="function")
def profiler_name() -> str:
    return "my_first_profiler"


@pytest.fixture(scope="function")
def store_name() -> str:
    return "profiler_store"


@pytest.fixture(scope="function")
def rules() -> Dict[str, RuleConfig]:
    return {
        "rule_1": RuleConfig(
            name="rule_1",
            domain_builder=DomainBuilderConfig(class_name="DomainBuilder"),
            parameter_builders=[
                ParameterBuilderConfig(
                    class_name="ParameterBuilder", name="my_parameter"
                )
            ],
            expectation_configuration_builders=[
                ExpectationConfigurationBuilderConfig(
                    class_name="ExpectationConfigurationBuilder",
                    expectation_type="expect_column_pair_values_A_to_be_greater_than_B",
                )
            ],
        )
    }


@pytest.fixture(scope="function")
def profiler_config(
    profiler_name: str, rules: Dict[str, RuleConfig]
) -> RuleBasedProfilerConfig:
    return RuleBasedProfilerConfig(name=profiler_name, config_version=1.0, rules=rules)


@pytest.fixture(scope="function")
def empty_profiler_store(store_name: str) -> ProfilerStore:
    return ProfilerStore(store_name)


@pytest.fixture(scope="function")
def populated_profiler_store(
    empty_profiler_store: ProfilerStore,
    profiler_config: RuleBasedProfilerConfig,
    profiler_name: str,
) -> ProfilerStore:
    profiler_store = empty_profiler_store
    key = ConfigurationIdentifier(configuration_key=profiler_name)
    profiler_store.set(key=key, value=profiler_config)
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
    populated_profiler_store: ProfilerStore, profiler_name: str
):
    assert len(populated_profiler_store.list_keys()) == 1
    key = (profiler_name,)
    populated_profiler_store.remove_key(key=key)
    assert len(populated_profiler_store.list_keys()) == 0


def test_profiler_store_self_check_report(
    populated_profiler_store: ProfilerStore, store_name: str, profiler_name: str
):
    data = populated_profiler_store.self_check()
    self_check_report = convert_to_json_serializable(data=data)
    assert self_check_report == {
        "config": {
            "class_name": "ProfilerStore",
            "module_name": "great_expectations.data_context.store.profiler_store",
            "overwrite_existing": False,
            "store_name": store_name,
        },
        "keys": [profiler_name],
        "len_keys": 1,
    }


def test_profiler_store_alters_filesystem(empty_data_context, store_name):
    base_directory: str = str(Path(empty_data_context.root_directory) / "profilers")

    profiler_store = build_profiler_store_using_filesystem(
        store_name=store_name,
        base_directory=base_directory,
        overwrite_existing=True,
    )
