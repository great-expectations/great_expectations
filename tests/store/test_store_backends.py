import pytest
import json
import importlib
import os

import six
if six.PY2: FileNotFoundError = IOError

import pandas as pd


from great_expectations.data_context.store import (
    StoreBackend,
    InMemoryStoreBackend,
)
from great_expectations.util import (
    gen_directory_tree_str,
)

def test_StoreBackendValidation():
    backend = StoreBackend({})

    backend._validate_key( ("I", "am", "a", "string", "tuple") )

    with pytest.raises(TypeError):
        backend._validate_key("nope")

    with pytest.raises(TypeError):
        backend._validate_key( ("I", "am", "a", "string", 100) )

    with pytest.raises(TypeError):
        backend._validate_key( ("I", "am", "a", "string", None) )

    # I guess this works. Huh.
    backend._validate_key( () )


def test_InMemoryStoreBackend():

    with pytest.raises(TypeError):
        my_store = InMemoryStoreBackend(
            config=None,
        )

    my_store = InMemoryStoreBackend(
        config={},
    )

    my_key = ("A",)
    with pytest.raises(KeyError):
        my_store.get(my_key)
    
    print(my_store.store)
    my_store.set(my_key, "aaa")
    print(my_store.store)
    assert my_store.get(my_key) == "aaa"

    #??? Putting a non-string object into a store triggers an error.
    # TODO: Allow bytes as well.
    with pytest.raises(TypeError):
        my_store.set(("B",), {"x":1})

    assert my_store.has_key(my_key) == True
    assert my_store.has_key(("B",)) == False
    assert my_store.has_key(("A",)) == True
    assert my_store.list_keys() == [("A",)]


# def test_FilesystemStore(tmp_path_factory):
#     path = str(tmp_path_factory.mktemp('test_FilesystemStore__dir'))
#     project_path = str(tmp_path_factory.mktemp('my_dir'))

#     my_store = FilesystemStore(
#         root_directory=os.path.abspath(path),
#         config={
#             "base_directory": project_path,
#             "file_extension" : ".txt",
#         }
#     )

#     #??? Should we standardize on KeyValue, or allow each Store to raise its own error types?
#     with pytest.raises(FileNotFoundError):
#         my_store.get("my_file_AAA")
    
#     my_store.set("my_file_AAA", "aaa")
#     assert my_store.get("my_file_AAA") == "aaa"

#     my_store.set("subdir/my_file_BBB", "bbb")
#     assert my_store.get("subdir/my_file_BBB") == "bbb"

#     my_store.set("subdir/my_file_BBB", "BBB")
#     assert my_store.get("subdir/my_file_BBB") == "BBB"

#     with pytest.raises(TypeError):
#         my_store.set("subdir/my_file_CCC", 123)
#         assert my_store.get("subdir/my_file_CCC") == 123

#     my_store.set("subdir/my_file_CCC", "ccc")
#     assert my_store.get("subdir/my_file_CCC") == "ccc"

#     print(my_store.list_keys())
#     # FIXME: Abe 2019/08/24 :
#     # Note that list_keys returns different keys than get and set.
#     # This is almost certainly not the right long-term behavior for FilesystemStore.
#     # However, it works with current methods in DataContext.
#     # I recommend
#     # 1. Overwriting this behavior with the correct behavior in NamespacedFilesystemStore
#     # 2. Migrating most/all of the Stores in DataContext to this new pattern.
#     # 3. Finally refactoring this behavior in FilesystemStore.
#     assert set(my_store.list_keys()) == set(["my_file_AAA.txt", "subdir/my_file_BBB.txt", "subdir/my_file_CCC.txt"])

# def test_store_config(tmp_path_factory):
#     path = str(tmp_path_factory.mktemp('test_store_config__dir'))

#     config = {
#         "module_name": "great_expectations.data_context.store",
#         "class_name": "InMemoryStore",
#         "store_config": {
#             "serialization_type": "json"
#         },
#     }
#     typed_config = StoreMetaConfig(
#         coerce_types=True,
#         **config
#     )
#     print(typed_config)

#     loaded_module = importlib.import_module(typed_config.module_name)
#     loaded_class = getattr(loaded_module, typed_config.class_name)

#     typed_sub_config = loaded_class.get_config_class()(
#         coerce_types=True,
#         **typed_config.store_config
#     )

#     data_asset_snapshot_store = loaded_class(
#         root_directory=os.path.abspath(path),
#         config=typed_sub_config,
#     )

# def test_NamespacedFilesystemStore(tmp_path_factory):
#     path = str(tmp_path_factory.mktemp('test_NamespacedFilesystemStore__dir'))
#     project_path = str(tmp_path_factory.mktemp('my_dir'))

#     my_store = NamespacedFilesystemStore(
#         # root_directory=empty_data_context.root_directory,
#         root_directory=os.path.abspath(path),
#         config={
#             "resource_identifier_class_name": "ValidationResultIdentifier",
#             "base_directory": project_path,
#             "file_extension" : ".txt",
#         }
#     )

#     with pytest.raises(TypeError):
#         my_store.get("not_a_ValidationResultIdentifier")

#     with pytest.raises(KeyError):
#         my_store.get(ValidationResultIdentifier(**{}))
    
#     ns_1 = ValidationResultIdentifier(
#         from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
#     )
#     my_store.set(ns_1,"aaa")
#     assert my_store.get(ns_1) == "aaa"

#     ns_2 = ValidationResultIdentifier(
#         from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-200"
#     )
#     my_store.set(ns_2, "bbb")
#     assert my_store.get(ns_2) == "bbb"

#     print(my_store.list_keys())
#     assert set(my_store.list_keys()) == set([
#         ns_1,
#         ns_2,
#     ])

#     # TODO : Reactivate this
#     # assert my_store.get_most_recent_run_id() == "200"


# def test_NamespacedFilesystemStore__validate_key(tmp_path_factory):
#     path = str(tmp_path_factory.mktemp('test_NamespacedFilesystemStore__dir'))
#     project_path = str(tmp_path_factory.mktemp('my_dir'))
 
#     my_store = NamespacedFilesystemStore(
#         root_directory=os.path.abspath(path),
#         config={
#             "resource_identifier_class_name": "ValidationResultIdentifier",
#             "base_directory": project_path,
#             "file_extension" : ".txt",
#         }
#     )

#     my_store._validate_key(ValidationResultIdentifier(
#         from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
#     ))

#     with pytest.raises(TypeError):
#         my_store._validate_key("I am string like")


# def test_NamespacedFilesystemStore_key_listing(tmp_path_factory):
#     path = str(tmp_path_factory.mktemp('test_NamespacedFilesystemStore_key_listing__dir'))
#     project_path = "some_dir/my_store"

#     my_store = NamespacedFilesystemStore(
#         root_directory=os.path.abspath(path),
#         config={
#             "resource_identifier_class_name": "ValidationResultIdentifier",
#             "base_directory": project_path,
#             "file_extension" : ".txt",
#         }
#     )

#     ns_1 = ValidationResultIdentifier(**{
#         "expectation_suite_identifier" : {
#             "data_asset_name" : DataAssetIdentifier("a", "b", "c"),
#             "expectation_suite_name" : "quarantine",
#         },
#         "run_id" : "prod-100",
#     })
#     my_store.set(ns_1,"aaa")

#     print(my_store.list_keys())
#     assert set(my_store.list_keys()) == set([
#         ValidationResultIdentifier(from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100")
#     ])

#     # TODO : Reactivate this
#     # assert my_store.get_most_recent_run_id() == "100"

# def test_NamespacedFilesystemStore_pandas_csv_serialization(tmp_path_factory):#, empty_data_context):
#     #TODO: We should consider using this trick everywhere, as a way to avoid directory name collisions
#     path = str(tmp_path_factory.mktemp('test_FilesystemStore_pandas_csv_serialization__dir'))

#     my_store = NamespacedFilesystemStore(
#         root_directory=os.path.abspath(path),
#         config={
#             "resource_identifier_class_name": "ValidationResultIdentifier",
#             "serialization_type": "pandas_csv",
#             "base_directory": path,
#             "file_extension": ".csv",
#             "file_prefix": "quarantined-rows-",
#         }
#     )

#     key1 = ValidationResultIdentifier(
#         from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-20190801"
#     )
#     with pytest.raises(AssertionError):
#         my_store.set(key1, "hi")

#     my_df = pd.DataFrame({"x": [1,2,3], "y": ["a", "b", "c"]})
#     my_store.set(key1, my_df)

#     print(gen_directory_tree_str(path))
#     assert gen_directory_tree_str(path) == """\
# test_FilesystemStore_pandas_csv_serialization__dir0/
#     prod-20190801/
#         a/
#             b/
#                 c/
#                     quarantined-rows-quarantine.csv
# """

#     with open(os.path.join(path, "prod-20190801/a/b/c/quarantined-rows-quarantine.csv")) as f_:
#         assert f_.read() == """\
# x,y
# 1,a
# 2,b
# 3,c
# """

#     with pytest.raises(NotImplementedError):
#         my_store.get(key1)
    
