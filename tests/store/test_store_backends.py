import pytest
import json
import importlib
import os
import re

import six
if six.PY2: FileNotFoundError = IOError

import pandas as pd


from great_expectations.data_context.store import (
    StoreBackend,
    InMemoryStoreBackend,
    FixedLengthTupleFilesystemStoreBackend,
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

    my_store = InMemoryStoreBackend()

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

def test_FilesystemStoreBackend_two_way_string_conversion(tmp_path_factory):
    path = str(tmp_path_factory.mktemp('test_FilesystemStoreBackend_two_way_string_conversion__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = FixedLengthTupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        file_extension= "txt",
        key_length=3,
        filepath_template= "{0}/{1}/{2}/foo-{2}-expectations.{file_extension}",
    )

    tuple_ = ("A__a", "B-b", "C")
    converted_string = my_store._convert_key_to_filepath(tuple_)
    print(converted_string)
    assert converted_string == "A__a/B-b/C/foo-C-expectations.txt"

    recovered_key = my_store._convert_filepath_to_key("A__a/B-b/C/foo-C-expectations.txt")
    print(recovered_key)
    assert recovered_key == tuple_
    
    with pytest.raises(ValueError):
        tuple_ = ("A/a", "B-b", "C")
        converted_string = my_store._convert_key_to_filepath(tuple_)
        print(converted_string)


def test_FixedLengthTupleFilesystemStoreBackend_verify_that_key_to_filepath_operation_is_reversible(tmp_path_factory):
    path = str(tmp_path_factory.mktemp('test_FixedLengthTupleFilesystemStoreBackend_verify_that_key_to_filepath_operation_is_reversible__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = FixedLengthTupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        file_extension= "txt",
        key_length=3,
        filepath_template= "{0}/{1}/{2}/foo-{2}-expectations.{file_extension}",
    )
    #This should pass silently
    my_store.verify_that_key_to_filepath_operation_is_reversible()

    my_store = FixedLengthTupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        file_extension= "txt",
        key_length=3,
        filepath_template= "{0}/{1}/foo-{2}-expectations.{file_extension}",
    )
    #This also should pass silently
    my_store.verify_that_key_to_filepath_operation_is_reversible()

    my_store = FixedLengthTupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        file_extension= "txt",
        key_length=3,
        filepath_template= "{0}/{1}/foo-expectations.{file_extension}",
    )
    with pytest.raises(AssertionError):
        #This should fail
        my_store.verify_that_key_to_filepath_operation_is_reversible()


def test_FixedLengthTupleFilesystemStoreBackend(tmp_path_factory):
    path = "dummy_str"
    project_path = str(tmp_path_factory.mktemp('test_FixedLengthTupleFilesystemStoreBackend__dir'))

    my_store = FixedLengthTupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        key_length=1,
        filepath_template= "my_file_{0}",
    )

    #??? Should we standardize on KeyValue, or allow each BackendStore to raise its own error types?
    with pytest.raises(FileNotFoundError):
        my_store.get(("AAA",))
    
    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    # NOTE: variable key lengths are not supported in this class
    # I suspect the best option is to differentiate between stores meant for reading AND writing,
    # vs Stores that only need to support writing. If a store only needs to support writing,
    # we don't need to guarantee reversibility of keys, which makes the internals **much** simpler.

    # my_store.set("subdir/my_file_BBB", "bbb")
    # assert my_store.get("subdir/my_file_BBB") == "bbb"

    # my_store.set("subdir/my_file_BBB", "BBB")
    # assert my_store.get("subdir/my_file_BBB") == "BBB"

    # with pytest.raises(TypeError):
    #     my_store.set("subdir/my_file_CCC", 123)
    #     assert my_store.get("subdir/my_file_CCC") == 123

    # my_store.set("subdir/my_file_CCC", "ccc")
    # assert my_store.get("subdir/my_file_CCC") == "ccc"

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == set([("AAA",), ("BBB",)])

    print(gen_directory_tree_str(project_path))
    assert gen_directory_tree_str(project_path) == """\
test_FixedLengthTupleFilesystemStoreBackend__dir0/
    my_file_AAA
    my_file_BBB
"""
