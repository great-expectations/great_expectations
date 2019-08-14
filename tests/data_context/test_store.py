import pytest
import json
import importlib

from great_expectations.data_context.store import (
    # Store,
    ContextAwareStore,
    InMemoryStore,
    FilesystemStore,
    # ContextAwareInMemoryStore,
    # ContextAwareFilesystemStore,
    NameSpacedFilesystemStore,
)
from great_expectations.data_context.store.types import (
    StoreMetaConfig,
)
from great_expectations.data_context.types import (
    NameSpaceDotDict,
)
# def test_core_store_logic():
#     pass

def test_InMemoryStore(empty_data_context):
    #TODO: Typechecking was causing circular imports. Someday, we can implement this with mypy...?
    # with pytest.raises(TypeError):
    #     my_store = InMemoryStore(
    #         data_context=None,
    #         config={},
    #     )

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
            "file_extension" : ".txt",
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

def test__get_namespaced_key(empty_data_context, tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('my_dir'))
    my_store = NameSpacedFilesystemStore(
        data_context=empty_data_context,
        config={
            "base_directory": project_path,
            "file_extension" : ".txt",
        }
    )

    with pytest.raises(KeyError):
        my_store._get_namespaced_key(NameSpaceDotDict(**{}))
    
    assert my_store._get_namespaced_key(NameSpaceDotDict(**{
        "expectation_suite_name" : "AAA",
        "normalized_data_asset_name" : "BBB",
        "run_id" : "CCC",
    }))[-23:] == "my_dir1/CCC/BBB/AAA.txt"

def test_NameSpacedFilesystemStore(empty_data_context, tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = NameSpacedFilesystemStore(
        data_context=empty_data_context,
        config={
            "base_directory": project_path,
            "file_extension" : ".txt",
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_NameSpaceDotDict")

    with pytest.raises(KeyError):
        my_store.get(NameSpaceDotDict(**{}))
    
    ns_1 = NameSpaceDotDict(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : "goodbye",
        "run_id" : "quack",
    })
    my_store.set(ns_1,"aaa")
    assert my_store.get(ns_1) == "aaa"

    ns_2 = NameSpaceDotDict(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : "goodbye",
        "run_id" : "moo",
    })
    my_store.set(ns_2, "bbb")
    assert my_store.get(ns_2) == "bbb"