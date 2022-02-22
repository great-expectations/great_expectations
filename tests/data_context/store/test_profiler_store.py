from pathlib import Path

import pytest

from great_expectations.core.util import convert_to_json_serializable
from great_expectations.data_context.data_context import DataContext
from great_expectations.data_context.store.profiler_store import ProfilerStore
from great_expectations.data_context.types.resource_identifiers import (
    ConfigurationIdentifier,
)
from great_expectations.rule_based_profiler.config import RuleBasedProfilerConfig
from great_expectations.util import gen_directory_tree_str
from tests.test_utils import build_profiler_store_using_filesystem


def test_profiler_store_raises_error_with_invalid_value(
    empty_profiler_store: ProfilerStore,
):
    with pytest.raises(TypeError):
        empty_profiler_store.set(
            key="my_first_profiler", value="this is not a profiler"
        )


def test_profiler_store_set_adds_valid_key(
    empty_profiler_store: ProfilerStore,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
    profiler_key: ConfigurationIdentifier,
):
    assert len(empty_profiler_store.list_keys()) == 0
    empty_profiler_store.set(
        key=profiler_key, value=profiler_config_with_placeholder_args
    )
    assert len(empty_profiler_store.list_keys()) == 1


def test_profiler_store_integration(
    empty_data_context: DataContext,
    profiler_store_name: str,
    profiler_name: str,
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
):
    base_directory: str = str(Path(empty_data_context.root_directory) / "profilers")

    profiler_store: ProfilerStore = build_profiler_store_using_filesystem(
        store_name=profiler_store_name,
        base_directory=base_directory,
        overwrite_existing=True,
    )

    dir_tree: str

    dir_tree = gen_directory_tree_str(startpath=base_directory)
    assert (
        dir_tree
        == """profilers/
    .ge_store_backend_id
"""
    )

    key: ConfigurationIdentifier = ConfigurationIdentifier(
        configuration_key=profiler_name
    )
    profiler_store.set(key=key, value=profiler_config_with_placeholder_args)

    dir_tree = gen_directory_tree_str(startpath=base_directory)
    assert (
        dir_tree
        == """profilers/
    .ge_store_backend_id
    my_first_profiler.yml
"""
    )

    assert len(profiler_store.list_keys()) == 1
    profiler_store.remove_key(key=key)
    assert len(profiler_store.list_keys()) == 0

    data: dict = profiler_store.self_check()
    self_check_report: dict = convert_to_json_serializable(data=data)

    # Drop dynamic value to ensure appropriate assert
    self_check_report["config"]["store_backend"].pop("base_directory")

    assert self_check_report == {
        "config": {
            "class_name": "ProfilerStore",
            "module_name": "great_expectations.data_context.store.profiler_store",
            "overwrite_existing": True,
            "store_backend": {
                "class_name": "TupleFilesystemStoreBackend",
                "filepath_suffix": ".yml",
                "fixed_length_key": False,
                "module_name": "great_expectations.data_context.store.tuple_store_backend",
                "platform_specific_separator": True,
                "suppress_store_backend_id": False,
            },
            "store_name": "profiler_store",
        },
        "keys": [],
        "len_keys": 0,
    }
