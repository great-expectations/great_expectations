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
    NameSpaceDotDict,
    NormalizedDataAssetName,
)
from great_expectations.util import (
    gen_directory_tree_str,
)

# def test_core_store_logic():
#     pass

def test_InMemoryStore(empty_data_context):
    #TODO: Typechecking was causing circular imports. See the note in the Store class.
    # with pytest.raises(TypeError):
    #     my_store = InMemoryStore(
    #         data_context=None,
    #         config={},
    #     )

    with pytest.raises(TypeError):
        my_store = InMemoryStore(
            # data_context=empty_data_context,
            config=None,
        )

    my_store = InMemoryStore(
        # data_context=empty_data_context,
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
        # data_context=empty_data_context,
        config={
            "serialization_type": "json"
        }
    )
    
    my_store.set("AAA", {"x":1})
    assert my_store.get("AAA") == {"x":1}

    with pytest.raises(TypeError):
        my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

    my_store = InMemoryStore(
        # data_context=empty_data_context,
        config={}
    )

    with pytest.raises(KeyError):
        assert my_store.get("AAA") == {"x":1}

    my_store.set("AAA", {"x":1}, serialization_type="json")
    
    assert my_store.get("AAA") == "{\"x\": 1}"
    assert my_store.get("AAA", serialization_type="json") == {"x":1}

    with pytest.raises(TypeError):
        my_store.set("BBB", set(["x", "y", "z"]), serialization_type="json")

    assert set(my_store.list_keys()) == set(["AAA"])


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
        my_store._get_namespaced_key(NameSpaceDotDict(**{}))
    
    ns_key = my_store._get_namespaced_key(NameSpaceDotDict(**{
        "expectation_suite_name" : "AAA",
        "normalized_data_asset_name" : NormalizedDataAssetName("B", "B", "B"),
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
        my_store.get("not_a_NameSpaceDotDict")

    with pytest.raises(KeyError):
        my_store.get(NameSpaceDotDict(**{}))
    
    ns_1 = NameSpaceDotDict(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : NormalizedDataAssetName("a", "b", "c"),
        "run_id" : "100",
    })
    my_store.set(ns_1,"aaa")
    assert my_store.get(ns_1) == "aaa"

    ns_2 = NameSpaceDotDict(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : NormalizedDataAssetName("a", "b", "c"),
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

    ns_1 = NameSpaceDotDict(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : NormalizedDataAssetName("a", "b", "c"),
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

    key1 = NameSpaceDotDict(**{
        "expectation_suite_name" : "hello",
        "normalized_data_asset_name" : NormalizedDataAssetName("a", "b", "c"),
        "run_id" : "100",
    })
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
    
