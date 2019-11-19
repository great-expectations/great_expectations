import pytest
import os
import boto3
from moto import mock_s3
from mock import patch

import six
if six.PY2:
    FileNotFoundError = IOError


from great_expectations.data_context.store import (
    StoreBackend,
    InMemoryStoreBackend,
    FixedLengthTupleFilesystemStoreBackend,
    FixedLengthTupleS3StoreBackend,
    FixedLengthTupleGCSStoreBackend,
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

    my_store.set(("B",), {"x":1})

    assert my_store.has_key(my_key) == True
    assert my_store.has_key(("B",)) == True
    assert my_store.has_key(("A",)) == True
    assert my_store.has_key(("C",)) == False
    assert my_store.list_keys() == [("A",), ("B",)]

def test_FilesystemStoreBackend_two_way_string_conversion(tmp_path_factory):
    path = str(tmp_path_factory.mktemp('test_FilesystemStoreBackend_two_way_string_conversion__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = FixedLengthTupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        key_length=3,
        filepath_template="{0}/{1}/{2}/foo-{2}-expectations.txt",
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
        key_length=3,
        filepath_template= "{0}/{1}/{2}/foo-{2}-expectations.txt",
    )
    #This should pass silently
    my_store.verify_that_key_to_filepath_operation_is_reversible()

    my_store = FixedLengthTupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        key_length=3,
        filepath_template= "{0}/{1}/foo-{2}-expectations.txt",
    )
    #This also should pass silently
    my_store.verify_that_key_to_filepath_operation_is_reversible()


    with pytest.raises(ValueError):
        my_store = FixedLengthTupleFilesystemStoreBackend(
            root_directory=os.path.abspath(path),
            base_directory=project_path,
            key_length=3,
            filepath_template= "{0}/{1}/foo-expectations.txt",
        )


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
    assert set(my_store.list_keys()) == {("AAA",), ("BBB",)}

    print(gen_directory_tree_str(project_path))
    assert gen_directory_tree_str(project_path) == """\
test_FixedLengthTupleFilesystemStoreBackend__dir0/
    my_file_AAA
    my_file_BBB
"""


@mock_s3
def test_FixedLengthTupleS3StoreBackend():
    """
    What does this test test and why?

    We will exercise the store backend's set method twice and then verify
    that the we calling get and list methods will return the expected keys.

    We will also check that the objects are stored on S3 at the expected location.

    """
    path = "dummy_str"
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket)

    my_store = FixedLengthTupleS3StoreBackend(
        # NOTE: Eugene: 2019-09-06: root_directory should be removed from the base class
        root_directory=os.path.abspath(path),
        key_length=1,
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )

    my_store.set(("AAA",), "aaa", content_type='text/html; charset=utf-8')
    assert my_store.get(("AAA",)) == 'aaa'


    # We should have set the raw data content type since we encode as utf-8
    object = boto3.client('s3').get_object(Bucket=bucket, Key=prefix + "/my_file_AAA")
    assert object["ContentType"] == 'text/html; charset=utf-8'
    assert object["ContentEncoding"] == 'utf-8'

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == 'bbb'

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {("AAA",), ("BBB",)}
    assert set([s3_object_info['Key'] for s3_object_info in boto3.client('s3').list_objects(Bucket=bucket, Prefix=prefix)['Contents']])\
           == set(['this_is_a_test_prefix/my_file_AAA', 'this_is_a_test_prefix/my_file_BBB'])


def test_FixedLengthTupleGCSStoreBackend():

    """
    What does this test test and why?

    Since no package like moto exists for GCP services, we mock the GCS client
    and assert that the store backend makes the right calls for set, get, and list.

    """

    path = "dummy_str"
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"
    project = "dummy-project"

    my_store = FixedLengthTupleGCSStoreBackend(
        root_directory=os.path.abspath(path), # NOTE: Eugene: 2019-09-06: root_directory should be removed from the base class
        key_length=1,
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
        project=project
    )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        
        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store.set(("AAA",), "aaa", content_type='text/html')

        mock_gcs_client.assert_called_once_with('dummy-project')
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.blob.assert_called_once_with("this_is_a_test_prefix/my_file_AAA")
        mock_blob.upload_from_string.assert_called_once_with(b"aaa", content_type="text/html")

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.get_blob.return_value
        mock_str = mock_blob.download_as_string.return_value

        my_store.get(("BBB",))

        mock_gcs_client.assert_called_once_with('dummy-project')
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.get_blob.assert_called_once_with("this_is_a_test_prefix/my_file_BBB")
        mock_blob.download_as_string.assert_called_once()
        mock_str.decode.assert_called_once_with("utf-8")

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value

        my_store.list_keys()

        mock_client.list_blobs.assert_called_once_with("leakybucket", prefix="this_is_a_test_prefix")
