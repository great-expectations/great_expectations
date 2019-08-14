import pytest
import json
import importlib

from great_expectations.data_context.store import (
    Store,
    InMemoryStore,
    FilesystemStore,
    # store_class_registry,
)
from great_expectations.data_context.store.types import (
    StoreMetaConfig,
)

def test_core_store_logic():
    pass


def test_InMemoryStore(empty_data_context):
    with pytest.raises(TypeError):
        my_store = InMemoryStore(
            data_context=None,
            config={},
        )

    with pytest.raises(TypeError):
        my_store = InMemoryStore(
            data_context=empty_data_context,
            config=None,
        )

    my_store = InMemoryStore(
        data_context=empty_data_context,
        config={},
    )

    with pytest.raises(KeyError):
        my_store.get("AAA")
    
    my_store.set("AAA", "aaa")
    assert my_store.get("AAA") == "aaa"

    #??? Should putting a non-string, non-byte object into a store trigger an error?    
    # with pytest.raises(ValueError):
    #     my_store.set("BBB", {"x":1})

def test_InMemoryStore_with_serialization(empty_data_context):
    my_store = InMemoryStore(
        data_context=empty_data_context,
        config={
            "serialization_type": "json"
        }
    )
    
    my_store.set("AAA", {"x":1})
    assert my_store.get("AAA") == {"x":1}

    with pytest.raises(TypeError):
        my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

    my_store = InMemoryStore(
        data_context=empty_data_context,
        config={}
    )

    with pytest.raises(KeyError):
        assert my_store.get("AAA") == {"x":1}

    my_store.set("AAA", {"x":1}, serialization_type="json")
    
    assert my_store.get("AAA") == "{\"x\": 1}"
    assert my_store.get("AAA", serialization_type="json") == {"x":1}

    with pytest.raises(TypeError):
        my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

def test_FilesystemStore(tmp_path_factory, empty_data_context):
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = FilesystemStore(
        data_context=empty_data_context,
        config={
            "base_directory": project_path,
        }
    )

    #??? Should we standardize on KeyValue, or allow each Store to raise its own error types?
    with pytest.raises(FileNotFoundError):
        my_store.get("my_file_AAA")
    
    my_store.set("my_file_AAA", "aaa")
    assert my_store.get("my_file_AAA") == "aaa"

    my_store.set("subdir/my_file_BBB", "bbb")
    assert my_store.get("subdir/my_file_BBB") == "bbb"

def test_store_config(empty_data_context):

    config = {
        "module_name": "great_expectations.data_context.store",
        "class_name": "InMemoryStore",
        "store_config": {
            "serialization_type": "json"
        },
    }
    typed_config = StoreMetaConfig(
        coerce_types=True,
        **config,
    )
    print(typed_config)

    loaded_module = importlib.import_module(typed_config.module_name)
    loaded_class = getattr(loaded_module, typed_config.class_name)

    typed_sub_config = loaded_class.get_config_class()(
        coerce_types=True,
        **typed_config.store_config,
    )

    data_asset_snapshot_store = loaded_class(
        data_context=empty_data_context,
        config=typed_sub_config,
    )