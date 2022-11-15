from pathlib import Path
from unittest import mock

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


@pytest.mark.unit
def test_profiler_store_raises_error_with_invalid_value(
    empty_profiler_store: ProfilerStore,
):
    with pytest.raises(TypeError):
        empty_profiler_store.set(
            key="my_first_profiler", value="this is not a profiler"
        )


@pytest.mark.unit
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


@pytest.mark.integration
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

    key = ConfigurationIdentifier(configuration_key=profiler_name)
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


@pytest.mark.unit
@pytest.mark.cloud
def test_ge_cloud_response_json_to_object_dict(
    profiler_config_with_placeholder_args: RuleBasedProfilerConfig,
) -> None:
    store = ProfilerStore(store_name="profiler_store")

    profiler_id = "b1445fa5-d034-45d7-a4ae-d6dca19b207b"

    profiler_config = profiler_config_with_placeholder_args.to_dict()
    response_json = {
        "data": {
            "id": profiler_id,
            "attributes": {
                "profiler": profiler_config,
            },
        }
    }

    expected = profiler_config
    expected["id"] = profiler_id

    actual = store.ge_cloud_response_json_to_object_dict(response_json)

    assert actual == expected


@pytest.mark.unit
def test_serialization_self_check(capsys) -> None:
    store = ProfilerStore(store_name="profiler_store")

    with mock.patch("random.choice", lambda _: "0"):
        store.serialization_self_check(pretty_print=True)

    stdout = capsys.readouterr().out

    test_key = "ConfigurationIdentifier::profiler_00000000000000000000"
    messages = [
        f"Attempting to add a new test key {test_key} to Profiler store...",
        f"Test key {test_key} successfully added to Profiler store.",
        f"Attempting to retrieve the test value associated with key {test_key} from Profiler store...",
        "Test value successfully retrieved from Profiler store",
        f"Cleaning up test key {test_key} and value from Profiler store...",
        "Test key and value successfully removed from Profiler store",
    ]

    for message in messages:
        assert message in stdout
