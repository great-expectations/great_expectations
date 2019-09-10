import pytest
import os

import pandas as pd

from great_expectations.data_context.store import (
    ValidationResultStore,
)
from great_expectations.data_context.types import (
    DataAssetIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.util import (
    gen_directory_tree_str,
)

def test_ValidationResultStore_with_InMemoryStoreBackend():

    my_store = ValidationResultStore(
        store_backend = {
            "module_name" : "great_expectations.data_context.store",
            "class_name" : "InMemoryStoreBackend",
            "separator" : ".",
        },
        root_directory=None,#"dummy/path/",
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(KeyError):
        my_store.get(ValidationResultIdentifier(**{}))
    
    ns_1 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
    )
    my_store.set(ns_1,{"A": "aaa"})
    assert my_store.get(ns_1) == {"A": "aaa"}

    ns_2 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-200"
    )
    my_store.set(ns_2, "bbb")
    assert my_store.get(ns_2) == "bbb"

    # Verify that internals are working as expected
    assert my_store.store_backend.store == {
        'a.b.c.quarantine.prod-100': '{"A": "aaa"}',
        'a.b.c.quarantine.prod-200': '"bbb"',
    }

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == set([
        ns_1,
        ns_2,
    ])

def test_ValidationResultStore__convert_resource_identifier_to_list():

    my_store = ValidationResultStore(
        store_backend = {
            "module_name" : "great_expectations.data_context.store",
            "class_name" : "InMemoryStoreBackend",
        },
        root_directory=None,
    )

    ns_1 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
    )
    assert my_store._convert_resource_identifier_to_tuple(ns_1) == ('a', 'b', 'c', 'quarantine', 'prod-100')
    
def test_ValidationResultStore_with_FixedLengthTupleFileSystemStoreBackend(tmp_path_factory):
    path = str(tmp_path_factory.mktemp('test_ValidationResultStore_with_FixedLengthTupleFileSystemStoreBackend__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = ValidationResultStore(
        store_backend = {
            "module_name" : "great_expectations.data_context.store",
            "class_name" : "FixedLengthTupleFilesystemStoreBackend",
            "base_directory" : "my_store/",
            "filepath_template" : "{4}/{0}/{1}/{2}/{3}.txt",
        },
        root_directory = path,
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(KeyError):
        my_store.get(ValidationResultIdentifier(**{}))
    
    ns_1 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
    )
    my_store.set(ns_1, {"A": "aaa"})
    assert my_store.get(ns_1) == {"A": "aaa"}

    ns_2 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-20"
    )
    my_store.set(ns_2, {"B": "bbb"})
    assert my_store.get(ns_2) == {"B": "bbb"}

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == set([
        ns_1,
        ns_2,
    ])

    print(gen_directory_tree_str(path))
    assert gen_directory_tree_str(path) == """\
test_ValidationResultStore_with_FixedLengthTupleFileSystemStoreBackend__dir0/
    my_store/
        prod-100/
            a/
                b/
                    c/
                        quarantine.txt
        prod-20/
            a/
                b/
                    c/
                        quarantine.txt
"""
