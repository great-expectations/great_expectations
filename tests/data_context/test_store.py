import pytest
import json
import importlib
import os

import six
if six.PY2: FileNotFoundError = IOError

import pandas as pd


from great_expectations.data_context.store import (
    # Store,
    # ContextAwareStore,
    NamespaceAwareStore,
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
    # NameSpaceDotDict,
    # NormalizedDataAssetName,
    DataAssetIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.util import (
    gen_directory_tree_str,
)

def test_InMemoryStore():#empty_data_context):

    with pytest.raises(TypeError):
        my_store = InMemoryStore(
            config=None,
        )

    my_store = InMemoryStore(
        config={},
    )

    my_key = DataAssetIdentifier("a","b","c")
    with pytest.raises(KeyError):
        my_store.get(my_key)
    
    my_store.set(my_key, "aaa")
    assert my_store.get(my_key) == "aaa"

    #??? Should putting a non-string, non-byte object into a store trigger an error?
    # NOTE : Abe 2019/08/24 : I think this is the same as asking, "Is serializability a core concern of stores?"
    # with pytest.raises(ValueError):
    #     my_store.set("BBB", {"x":1})

    # TODO: Get these working
    assert my_store.has_key(my_key) == True
    assert my_store.has_key(DataAssetIdentifier("A","b","c")) == False
    #Try the same thing twice in a row, just in case...
    assert my_store.has_key(DataAssetIdentifier("A","b","c")) == False
    #Try something similar with deeper nesting
    assert my_store.has_key(DataAssetIdentifier("a","b","C")) == False
    # Try the first object again, but newly instantiated
    assert my_store.has_key(DataAssetIdentifier("a","b","c")) == True
    assert my_store.list_keys() == [my_key]

# NOTE : Abe 2019/08/24 : Freezing this code in carbonite,
# in case it turns out to be useful in a future refactor to separate Store and NamespaceAwareStore
# def test_InMemoryStore_with_serialization(empty_data_context):
#     my_store = InMemoryStore(
#         config={
#             "serialization_type": "json"
#         }
#     )

#     A_key = DataAssetIdentifier("A", "A", "A")
    
#     my_store.set("AAA", {"x":1})
#     assert my_store.get("AAA") == {"x":1}

#     with pytest.raises(TypeError):
#         my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

#     my_store = InMemoryStore(
#         config={}
#     )

#     with pytest.raises(KeyError):
#         assert my_store.get("AAA") == {"x":1}

#     my_store.set("AAA", {"x":1}, serialization_type="json")
    
#     assert my_store.get("AAA") == "{\"x\": 1}"
#     assert my_store.get("AAA", serialization_type="json") == {"x":1}

#     with pytest.raises(TypeError):
#         my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

#     assert set(my_store.list_keys()) == set(["AAA"])

def test_InMemoryStore_with_serialization(empty_data_context):
    my_store = InMemoryStore(
        config={
            "serialization_type": "json"
        }
    )

    A_key = DataAssetIdentifier("A", "A", "A")
    B_key = DataAssetIdentifier("B", "B", "B")
    
    my_store.set(A_key, {"x":1})
    assert my_store.get(A_key) == {"x":1}

    with pytest.raises(TypeError):
        my_store.set(B_key, set(["x", "y", "z"]), serialization_type="json")

    my_store = InMemoryStore(
        config={}
    )

    with pytest.raises(KeyError):
        assert my_store.get(A_key) == {"x":1}

    my_store.set(A_key, {"x":1}, serialization_type="json")
    
    assert my_store.get(A_key) == "{\"x\": 1}"
    assert my_store.get(A_key, serialization_type="json") == {"x":1}

    with pytest.raises(TypeError):
        my_store.set(B_key, set(["x", "y", "z"]), serialization_type="json")

    # FIXME : This should return a list of keys, not strings.
    assert set(my_store.list_keys()) == set(['DataAssetIdentifier.A.A.A'])


def test_FilesystemStore(tmp_path_factory, empty_data_context):
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = FilesystemStore(
        # data_context=empty_data_context,
        root_directory=empty_data_context.root_directory,
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

    my_store.set("subdir/my_file_BBB", "BBB")
    assert my_store.get("subdir/my_file_BBB") == "BBB"

    with pytest.raises(TypeError):
        my_store.set("subdir/my_file_CCC", 123)
        assert my_store.get("subdir/my_file_CCC") == 123

    my_store.set("subdir/my_file_CCC", "ccc")
    assert my_store.get("subdir/my_file_CCC") == "ccc"

    assert set(my_store.list_keys()) == set(["my_file_AAA", "subdir/my_file_BBB", "subdir/my_file_CCC"])

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
        **config
    )
    print(typed_config)

    loaded_module = importlib.import_module(typed_config.module_name)
    loaded_class = getattr(loaded_module, typed_config.class_name)

    typed_sub_config = loaded_class.get_config_class()(
        coerce_types=True,
        **typed_config.store_config
    )

    data_asset_snapshot_store = loaded_class(
        # data_context=empty_data_context,
        root_directory=empty_data_context.root_directory,
        config=typed_sub_config,
    )

def test__get_namespaced_key(empty_data_context, tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('my_dir'))
    my_store = NameSpacedFilesystemStore(
        # data_context=empty_data_context,
        root_directory=empty_data_context.root_directory,
        config={
            "base_directory": project_path,
            "file_extension" : ".txt",
        }
    )

    with pytest.raises(KeyError):
        my_store._get_namespaced_key(ValidationResultIdentifier(**{}))
    
    ns_key = my_store._get_namespaced_key(ValidationResultIdentifier(**{
        "expectation_suite_name" : "AAA",
        "normalized_data_asset_name" : DataAssetIdentifier("B", "B", "B"),
        "run_id" : "CCC",
    }))
    print(ns_key)
    assert ns_key[-25:] == "my_dir1/CCC/B/B/B/AAA.txt"

def test_NameSpacedFilesystemStore(empty_data_context, tmp_path_factory):
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = NameSpacedFilesystemStore(
        # data_context=empty_data_context,
        root_directory=empty_data_context.root_directory,
        config={
            "base_directory": project_path,
            "file_extension" : ".txt",
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(KeyError):
        my_store.get(ValidationResultIdentifier(**{}))
    
    ns_1 = ValidationResultIdentifier(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : DataAssetIdentifier("a", "b", "c"),
        "run_id" : "100",
    })
    my_store.set(ns_1,"aaa")
    assert my_store.get(ns_1) == "aaa"

    ns_2 = ValidationResultIdentifier(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : DataAssetIdentifier("a", "b", "c"),
        "run_id" : "200",
    })
    my_store.set(ns_2, "bbb")
    assert my_store.get(ns_2) == "bbb"

    assert set(my_store.list_keys()) == set([
        "100/a/b/c/hello.txt",
        "200/a/b/c/hello.txt",
    ])

    assert my_store.get_most_recent_run_id() == "200"

def test_NameSpacedFilesystemStore_key_listing(empty_data_context, tmp_path_factory):
    project_path = "some_dir/my_store"

    my_store = NameSpacedFilesystemStore(
        # data_context=empty_data_context,
        root_directory=empty_data_context.root_directory,
        config={
            "base_directory": project_path,
            "file_extension" : ".txt",
        }
    )

    ns_1 = ValidationResultIdentifier(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : DataAssetIdentifier("a", "b", "c"),
        "run_id" : "100",
    })
    my_store.set(ns_1,"aaa")

    assert set(my_store.list_keys()) == set([
        "100/a/b/c/hello.txt",
    ])

    assert my_store.get_most_recent_run_id() == "100"

def test_NameSpacedFilesystemStore_pandas_csv_serialization(tmp_path_factory, empty_data_context):
    #TODO: We should consider using this trick everywhere, as a way to avoid directory name collisions
    path = str(tmp_path_factory.mktemp('test_FilesystemStore_pandas_csv_serialization__dir'))

    my_store = NameSpacedFilesystemStore(
        # data_context=empty_data_context,
        root_directory=empty_data_context.root_directory,
        config={
            "serialization_type": "pandas_csv",
            "base_directory": path,
            "file_extension": ".csv",
        }
    )

    key1 = ValidationResultIdentifier(
        coerce_types=True,
        **{
            "expectation_suite_identifier" : {
                "data_asset_identifier" : ("a", "b", "c"), #DataAssetIdentifier("a", "b", "c"),
                "suite_name": "hello",
                "purpose": "testing",
            },
            # "purpose" : "default",
            "run_id" : ("goodbye", "100"),
        }
    )
    with pytest.raises(AssertionError):
        my_store.set(key1, "hi")

    my_df = pd.DataFrame({"x": [1,2,3], "y": ["a", "b", "c"]})
    my_store.set(key1, my_df)

    assert gen_directory_tree_str(path) == """\
test_FilesystemStore_pandas_csv_serialization__dir0/
    100/
        a/
            b/
                c/
                    hello.csv
"""

    with open(os.path.join(path, "100/a/b/c/hello.csv")) as f_:
        assert f_.read() == """\
x,y
1,a
2,b
3,c
"""

    with pytest.raises(NotImplementedError):
        my_store.get(key1)
    
