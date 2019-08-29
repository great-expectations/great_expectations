import pytest
import os

import pandas as pd

from great_expectations.data_context.store import (
    NamespacedReadWriteStore,
    NamespacedReadWriteStoreConfig,
    BasicInMemoryStore,
    BasicInMemoryStoreConfig,
)
from great_expectations.data_context.types import (
    DataAssetIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.util import (
    gen_directory_tree_str,
)

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

def test_NamespacedReadWriteStore_with_InMemoryStoreBackend():

    my_store = NamespacedReadWriteStore(
        config=NamespacedReadWriteStoreConfig(**{
            "resource_identifier_class_name": "ValidationResultIdentifier",
            "serialization_type" : None,
            "store_backend" : {
                "module_name" : "great_expectations.data_context.store",
                "class_name" : "InMemoryStoreBackend",
                "separator" : ".",
            }
        }),
        root_directory=None,#"dummy/path/",
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(KeyError):
        my_store.get(ValidationResultIdentifier(**{}))
    
    ns_1 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
    )
    my_store.set(ns_1,"aaa")
    assert my_store.get(ns_1) == "aaa"

    ns_2 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-200"
    )
    my_store.set(ns_2, "bbb")
    assert my_store.get(ns_2) == "bbb"

    # Verify that internals are working as expected
    assert my_store.store_backend.store == {
        'ValidationResultIdentifier.a.b.c.quarantine.prod-100': 'aaa',
        'ValidationResultIdentifier.a.b.c.quarantine.prod-200': 'bbb',
    }

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == set([
        ns_1,
        ns_2,
    ])

    # TODO : Reactivate this
    # assert my_store.get_most_recent_run_id() == "200"

def test_NamespacedReadWriteStore_with_FileSystemStoreBackend(tmp_path_factory):
    path = str(tmp_path_factory.mktemp('test_NamespacedFilesystemStore__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = NamespacedReadWriteStore(
        config=NamespacedReadWriteStoreConfig(**{
            "resource_identifier_class_name": "ValidationResultIdentifier",
            "serialization_type" : None,
            "store_backend" : {
                "module_name" : "great_expectations.data_context.store",
                "class_name" : "FilesystemStoreBackend",
                "base_directory" : "my_store/",
                "file_extension" : "txt",
                "filepath_template" : "{5}/{0}/{1}/{2}/{3}/{4}/validation-results-{3}-{4}.{file_extension}",
                "replaced_substring" : "/",
                "replacement_string" : "__",
            }
        }),
        root_directory = path,
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(KeyError):
        my_store.get(ValidationResultIdentifier(**{}))
    
    ns_1 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-100"
    )
    my_store.set(ns_1,"aaa")
    assert my_store.get(ns_1) == "aaa"

    ns_2 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-200"
    )
    my_store.set(ns_2, "bbb")
    assert my_store.get(ns_2) == "bbb"

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == set([
        ns_1,
        ns_2,
    ])

    print(gen_directory_tree_str(path))
    assert gen_directory_tree_str(path) == """\
test_NamespacedFilesystemStore__dir0/
    my_store/
        prod-100/
            ValidationResultIdentifier/
                a/
                    b/
                        c/
                            quarantine/
                                validation-results-c-quarantine.txt
        prod-200/
            ValidationResultIdentifier/
                a/
                    b/
                        c/
                            quarantine/
                                validation-results-c-quarantine.txt
"""

#     # TODO : Reactivate this
#     # assert my_store.get_most_recent_run_id() == "200"

# NOTE: Abe 2019/08/29 : AFIACT, this test doesn't add anything new after refactoring.
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

# NOTE: Abe 2019/08/29 : AFIACT, this test doesn't add anything new after refactoring.
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

def test_NamespacedFilesystemStore_pandas_csv_serialization(tmp_path_factory):#, empty_data_context):
    #TODO: We should consider using this trick everywhere, as a way to avoid directory name collisions
    path = str(tmp_path_factory.mktemp('test_test_NamespacedFilesystemStore_pandas_csv_serialization__dir'))
    # project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = NamespacedReadWriteStore(
        config=NamespacedReadWriteStoreConfig(**{
            "resource_identifier_class_name": "ValidationResultIdentifier",
            "serialization_type" : "pandas_csv",
            "store_backend" : {
                "module_name" : "great_expectations.data_context.store",
                "class_name" : "FilesystemStoreBackend",
                "base_directory" : "my_store/",
                "file_extension" : "csv",
                "filepath_template" : "{5}/{0}/{1}/{2}/{3}/quarantined-rows-{3}-{4}.{file_extension}",
                "replaced_substring" : "/",
                "replacement_string" : "__",
            }
        }),
        root_directory = path,
    )

    key1 = ValidationResultIdentifier(
        from_string="ValidationResultIdentifier.a.b.c.quarantine.prod-20190801"
    )
    with pytest.raises(AssertionError):
        my_store.set(key1, "hi")

    my_df = pd.DataFrame({"x": [1,2,3], "y": ["a", "b", "c"]})
    my_store.set(key1, my_df)

    print(gen_directory_tree_str(path))
    assert gen_directory_tree_str(path) == """\
test_test_NamespacedFilesystemStore_pandas_csv_serialization__dir0/
    my_store/
        prod-20190801/
            ValidationResultIdentifier/
                a/
                    b/
                        c/
                            quarantined-rows-c-quarantine.csv
"""

    with open(os.path.join(path, "my_store/prod-20190801/ValidationResultIdentifier/a/b/c/quarantined-rows-c-quarantine.csv")) as f_:
        assert f_.read() == """\
x,y
1,a
2,b
3,c
"""

    with pytest.raises(NotImplementedError):
        my_store.get(key1)


def test_BasicInMemoryStore():
    my_store = BasicInMemoryStore()

    my_key = "A"
    with pytest.raises(KeyError):
        my_store.get(my_key)
    
    my_store.set(my_key, "aaa")
    assert my_store.get(my_key) == "aaa"

    #??? Putting a non-string object into a store triggers an error.
    # TODO: Allow bytes as well.
    with pytest.raises(TypeError):
        my_store.set("B", {"x":1})

    assert my_store.has_key(my_key) == True
    assert my_store.has_key("B") == False
    assert my_store.has_key("A") == True
    assert my_store.list_keys() == ["A"]