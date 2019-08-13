import pytest
import json

from great_expectations.data_context.store import (
    Store,
    InMemoryStore,
)

def test_core_store_logic():
    pass


def test_integration():
    my_store = InMemoryStore()

    with pytest.raises(KeyError):
        my_store.get("AAA")
    
    my_store.set("AAA", "aaa")
    assert my_store.get("AAA") == "aaa"

def test_InMemoryStore_with_serialization():
    my_store = InMemoryStore(serialization_type="json")
    
    my_store.set("AAA", {"x":1})
    assert my_store.get("AAA") == {"x":1}

    my_store = InMemoryStore()

    #??? Should putting a non-string, non-byte object into a store trigger an error?    
    # with pytest.raises(ValueError):
    #     my_store.set("AAA", {"x":1})
    
    with pytest.raises(KeyError):
        assert my_store.get("AAA") == {"x":1}

    my_store.set("AAA", {"x":1}, serialization_type="json")
    
    assert my_store.get("AAA") == "{\"x\": 1}"
    assert my_store.get("AAA", serialization_type="json") == {"x":1}
