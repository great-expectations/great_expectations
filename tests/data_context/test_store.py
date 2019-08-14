import pytest
import json

from great_expectations.data_context.store import (
    Store,
    InMemoryStore,
    FilesystemStore,
)

def test_core_store_logic():
    pass


def test_InMemoryStore(empty_data_context):
    with pytest.raises(TypeError):
        my_store = InMemoryStore(
            data_context=None
        )

    my_store = InMemoryStore(
        data_context=empty_data_context
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
        serialization_type="json"
    )
    
    my_store.set("AAA", {"x":1})
    assert my_store.get("AAA") == {"x":1}

    with pytest.raises(TypeError):
        my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

    my_store = InMemoryStore(
        data_context=empty_data_context
    )

    with pytest.raises(KeyError):
        assert my_store.get("AAA") == {"x":1}

    my_store.set("AAA", {"x":1}, serialization_type="json")
    
    assert my_store.get("AAA") == "{\"x\": 1}"
    assert my_store.get("AAA", serialization_type="json") == {"x":1}

    with pytest.raises(TypeError):
        my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

def test_FilesystemStore(tmp_path, empty_data_context):
    my_store = FilesystemStore(**{
        "data_context": empty_data_context,
        "base_directory": tmp_path
    })

    #??? Should we standardize on KeyValue, or allow each Store to raise its own error types?
    with pytest.raises(FileNotFoundError):
        my_store.get("my_file_AAA")
    
    # my_store.set("my_file_AAA", "aaa")
    # assert my_store.get("my_file_AAA") == "aaa"

    # my_store.set("subdir/my_file_BBB", "bbb")
    # assert my_store.get("subdir/my_file_BBB") == "bbb"
