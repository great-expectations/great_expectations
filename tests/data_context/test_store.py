import pytest
import json

from great_expectations.data_context.store import (
    Store,
    InMemoryStore,
)

def test_integration_InMemoryStore():
    my_store = InMemoryStore()

    with pytest.raises(KeyError):
        my_store.get("AAA")
    
    my_store.set("AAA", "aaa")
    assert my_store.get("AAA") == "aaa"


def test_core_store_logic():
    pass


def test_InMemoryStore():
    pass
