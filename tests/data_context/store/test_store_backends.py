import datetime
import os
import uuid
from unittest.mock import patch

import boto3
import pyparsing as pp
import pytest
from moto import mock_s3

import tests.test_utils as test_utils
from great_expectations.core.run_identifier import RunIdentifier
from great_expectations.data_context.store import (
    InMemoryStoreBackend,
    StoreBackend,
    TupleAzureBlobStoreBackend,
    TupleFilesystemStoreBackend,
    TupleGCSStoreBackend,
    TupleS3StoreBackend,
)
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.exceptions import InvalidKeyError, StoreBackendError, StoreError
from great_expectations.util import gen_directory_tree_str


def test_StoreBackendValidation():
    backend = InMemoryStoreBackend()

    backend._validate_key(("I", "am", "a", "string", "tuple"))

    with pytest.raises(TypeError):
        backend._validate_key("nope")

    with pytest.raises(TypeError):
        backend._validate_key(("I", "am", "a", "string", 100))

    with pytest.raises(TypeError):
        backend._validate_key(("I", "am", "a", "string", None))

    # zero-length tuple is allowed
    backend._validate_key(())


def check_store_backend_store_backend_id_functionality(
    store_backend: StoreBackend, store_backend_id: str = None
) -> None:
    """
    Assertions to check if a store backend is handling reading and writing a store_backend_id appropriately.
    Args:
        store_backend: Instance of subclass of StoreBackend to test e.g. TupleFilesystemStoreBackend
        store_backend_id: Manually input store_backend_id
    Returns:
        None
    """
    # Check that store_backend_id exists can be read
    assert store_backend.store_backend_id is not None
    store_error_uuid = "00000000-0000-0000-0000-00000000e003"
    assert store_backend.store_backend_id != store_error_uuid
    if store_backend_id:
        assert store_backend.store_backend_id == store_backend_id
    # Check that store_backend_id is a valid UUID
    assert test_utils.validate_uuid4(store_backend.store_backend_id)
    # Check in file stores that the actual file exists
    assert store_backend.has_key(key=(".ge_store_backend_id",))

    # Check file stores for the file in the correct format
    store_backend_id_from_file = store_backend.get(key=(".ge_store_backend_id",))
    store_backend_id_file_parser = "store_backend_id = " + pp.Word(pp.hexnums + "-")
    parsed_store_backend_id = store_backend_id_file_parser.parseString(
        store_backend_id_from_file
    )
    assert test_utils.validate_uuid4(parsed_store_backend_id[1])


@mock_s3
def test_StoreBackend_id_initialization(tmp_path_factory):
    """
    What does this test and why?

    A StoreBackend should have a store_backend_id property. That store_backend_id should be read and initialized
    from an existing persistent store_backend_id during instantiation, or a new store_backend_id should be generated
    and persisted. The store_backend_id should be a valid UUIDv4
    If a new store_backend_id cannot be persisted, use an ephemeral store_backend_id.
    Persistence should be in a .ge_store_backend_id file for for filesystem and blob-stores.

    Note: StoreBackend & TupleStoreBackend are abstract classes, so we will test the
    concrete classes that inherit from them.
    See also test_database_store_backend::test_database_store_backend_id_initialization
    """

    # InMemoryStoreBackend
    # Initialize without store_backend_id and check that it is generated correctly
    in_memory_store_backend = InMemoryStoreBackend()
    check_store_backend_store_backend_id_functionality(
        store_backend=in_memory_store_backend
    )

    # Create a new store with the same config and make sure it reports the same store_backend_id
    # in_memory_store_backend_duplicate = InMemoryStoreBackend()
    # assert in_memory_store_backend.store_backend_id == in_memory_store_backend_duplicate.store_backend_id
    # This is not currently implemented for the InMemoryStoreBackend, the store_backend_id is ephemeral since
    # there is no place to persist it.

    # TupleFilesystemStoreBackend
    # Initialize without store_backend_id and check that it is generated correctly
    path = "dummy_str"
    project_path = str(
        tmp_path_factory.mktemp("test_StoreBackend_id_initialization__dir")
    )

    tuple_filesystem_store_backend = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),
    )
    # Check that store_backend_id is created on instantiation, before being accessed
    desired_directory_tree_str = """\
test_StoreBackend_id_initialization__dir0/
    dummy_str/
        .ge_store_backend_id
"""
    assert gen_directory_tree_str(project_path) == desired_directory_tree_str
    check_store_backend_store_backend_id_functionality(
        store_backend=tuple_filesystem_store_backend
    )
    assert gen_directory_tree_str(project_path) == desired_directory_tree_str

    # Repeat the above with a filepath template
    project_path_with_filepath_template = str(
        tmp_path_factory.mktemp("test_StoreBackend_id_initialization__dir")
    )
    tuple_filesystem_store_backend_with_filepath_template = TupleFilesystemStoreBackend(
        root_directory=os.path.join(project_path, path),
        base_directory=project_path_with_filepath_template,
        filepath_template="my_file_{0}",
    )
    check_store_backend_store_backend_id_functionality(
        store_backend=tuple_filesystem_store_backend_with_filepath_template
    )
    assert (
        gen_directory_tree_str(project_path_with_filepath_template)
        == """\
test_StoreBackend_id_initialization__dir1/
    .ge_store_backend_id
"""
    )

    # Create a new store with the same config and make sure it reports the same store_backend_id
    tuple_filesystem_store_backend_duplicate = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),
        # filepath_template="my_file_{0}",
    )
    check_store_backend_store_backend_id_functionality(
        store_backend=tuple_filesystem_store_backend_duplicate
    )
    assert (
        tuple_filesystem_store_backend.store_backend_id
        == tuple_filesystem_store_backend_duplicate.store_backend_id
    )

    # TupleS3StoreBackend
    # Initialize without store_backend_id and check that it is generated correctly
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    s3_store_backend = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )

    check_store_backend_store_backend_id_functionality(store_backend=s3_store_backend)

    # Create a new store with the same config and make sure it reports the same store_backend_id
    s3_store_backend_duplicate = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )
    check_store_backend_store_backend_id_functionality(
        store_backend=s3_store_backend_duplicate
    )
    assert (
        s3_store_backend.store_backend_id == s3_store_backend_duplicate.store_backend_id
    )

    # TODO: Fix GCS Testing
    # TupleGCSStoreBackend
    # Initialize without store_backend_id and check that it is generated correctly
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"
    project = "dummy-project"
    base_public_path = "http://www.test.com/"

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        gcs_store_backend_with_base_public_path = TupleGCSStoreBackend(
            filepath_template=None,
            bucket=bucket,
            prefix=prefix,
            project=project,
            base_public_path=base_public_path,
        )

        gcs_store_backend_with_base_public_path_duplicate = TupleGCSStoreBackend(
            filepath_template=None,
            bucket=bucket,
            prefix=prefix,
            project=project,
            base_public_path=base_public_path,
        )

        assert gcs_store_backend_with_base_public_path.store_backend_id is not None
        # Currently we don't have a good way to mock GCS functionality
        # check_store_backend_store_backend_id_functionality(store_backend=gcs_store_backend_with_base_public_path)

        # Create a new store with the same config and make sure it reports the same store_backend_id
        assert (
            gcs_store_backend_with_base_public_path.store_backend_id
            == gcs_store_backend_with_base_public_path_duplicate.store_backend_id
        )
        store_error_uuid = "00000000-0000-0000-0000-00000000e003"
        assert (
            gcs_store_backend_with_base_public_path.store_backend_id != store_error_uuid
        )
        assert (
            gcs_store_backend_with_base_public_path_duplicate.store_backend_id
            != store_error_uuid
        )


@mock_s3
def test_TupleS3StoreBackend_store_backend_id():
    # TupleS3StoreBackend
    # Initialize without store_backend_id and check that it is generated correctly
    bucket = "leakybucket2"
    prefix = "this_is_a_test_prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    s3_store_backend = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )

    check_store_backend_store_backend_id_functionality(store_backend=s3_store_backend)

    # Create a new store with the same config and make sure it reports the same store_backend_id
    s3_store_backend_duplicate = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )

    store_error_uuid = "00000000-0000-0000-0000-00000000e003"
    assert s3_store_backend.store_backend_id != store_error_uuid
    assert s3_store_backend_duplicate.store_backend_id != store_error_uuid

    assert (
        s3_store_backend.store_backend_id == s3_store_backend_duplicate.store_backend_id
    )


def test_InMemoryStoreBackend():

    my_store = InMemoryStoreBackend()

    my_key = ("A",)
    with pytest.raises(InvalidKeyError):
        my_store.get(my_key)

    my_store.set(my_key, "aaa")
    assert my_store.get(my_key) == "aaa"

    my_store.set(("B",), {"x": 1})

    assert my_store.has_key(my_key) is True
    assert my_store.has_key(("B",)) is True
    assert my_store.has_key(("A",)) is True
    assert my_store.has_key(("C",)) is False
    assert my_store.list_keys() == [(".ge_store_backend_id",), ("A",), ("B",)]

    with pytest.raises(StoreError):
        my_store.get_url_for_key(my_key)


def test_tuple_filesystem_store_filepath_prefix_error(tmp_path_factory):
    path = str(
        tmp_path_factory.mktemp("test_tuple_filesystem_store_filepath_prefix_error")
    )
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=project_path,
            base_directory=os.path.join(project_path, path),
            filepath_prefix="invalid_prefix_ends_with/",
        )
    assert "filepath_prefix may not end with" in e.value.message

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=project_path,
            base_directory=os.path.join(project_path, path),
            filepath_prefix="invalid_prefix_ends_with\\",
        )
    assert "filepath_prefix may not end with" in e.value.message


def test_FilesystemStoreBackend_two_way_string_conversion(tmp_path_factory):
    path = str(
        tmp_path_factory.mktemp(
            "test_FilesystemStoreBackend_two_way_string_conversion__dir"
        )
    )
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    my_store = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),
        filepath_template="{0}/{1}/{2}/foo-{2}-expectations.txt",
    )

    tuple_ = ("A__a", "B-b", "C")
    converted_string = my_store._convert_key_to_filepath(tuple_)
    assert converted_string == "A__a/B-b/C/foo-C-expectations.txt"

    recovered_key = my_store._convert_filepath_to_key(
        "A__a/B-b/C/foo-C-expectations.txt"
    )
    assert recovered_key == tuple_

    with pytest.raises(ValueError):
        tuple_ = ("A/a", "B-b", "C")
        converted_string = my_store._convert_key_to_filepath(tuple_)


def test_TupleFilesystemStoreBackend(tmp_path_factory):
    path = "dummy_str"
    project_path = str(tmp_path_factory.mktemp("test_TupleFilesystemStoreBackend__dir"))
    base_public_path = "http://www.test.com/"

    my_store = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),
        filepath_template="my_file_{0}",
    )

    with pytest.raises(InvalidKeyError):
        my_store.get(("AAA",))

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {(".ge_store_backend_id",), ("AAA",), ("BBB",)}
    assert (
        gen_directory_tree_str(project_path)
        == """\
test_TupleFilesystemStoreBackend__dir0/
    dummy_str/
        .ge_store_backend_id
        my_file_AAA
        my_file_BBB
"""
    )
    my_store.remove_key(("BBB",))
    with pytest.raises(InvalidKeyError):
        assert my_store.get(("BBB",)) == ""

    my_store_with_base_public_path = TupleFilesystemStoreBackend(
        root_directory=project_path,
        base_directory=os.path.join(project_path, path),
        filepath_template="my_file_{0}",
        base_public_path=base_public_path,
    )
    my_store_with_base_public_path.set(("CCC",), "ccc")
    url = my_store_with_base_public_path.get_public_url_for_key(("CCC",))
    assert url == "http://www.test.com/my_file_CCC"


def test_TupleFilesystemStoreBackend_ignores_jupyter_notebook_checkpoints(
    tmp_path_factory,
):
    project_path = str(tmp_path_factory.mktemp("things"))

    checkpoint_dir = os.path.join(project_path, ".ipynb_checkpoints")
    os.mkdir(checkpoint_dir)
    assert os.path.isdir(checkpoint_dir)
    nb_file = os.path.join(checkpoint_dir, "foo.json")

    with open(nb_file, "w") as f:
        f.write("")
    assert os.path.isfile(nb_file)
    my_store = TupleFilesystemStoreBackend(
        root_directory=os.path.join(project_path, "dummy_str"),
        base_directory=project_path,
    )

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    assert (
        gen_directory_tree_str(project_path)
        == """\
things0/
    .ge_store_backend_id
    AAA
    .ipynb_checkpoints/
        foo.json
"""
    )

    assert set(my_store.list_keys()) == {(".ge_store_backend_id",), ("AAA",)}


@mock_s3
def test_TupleS3StoreBackend_with_prefix():
    """
    What does this test test and why?

    We will exercise the store backend's set method twice and then verify
    that the we calling get and list methods will return the expected keys.

    We will also check that the objects are stored on S3 at the expected location,
    and that the correct S3 URL for the object can be retrieved.

    """
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"
    base_public_path = "http://www.test.com/"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    my_store = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )

    # We should be able to list keys, even when empty
    keys = my_store.list_keys()
    assert len(keys) == 1

    my_store.set(("AAA",), "aaa", content_type="text/html; charset=utf-8")
    assert my_store.get(("AAA",)) == "aaa"

    obj = boto3.client("s3").get_object(Bucket=bucket, Key=prefix + "/my_file_AAA")
    assert obj["ContentType"] == "text/html; charset=utf-8"
    assert obj["ContentEncoding"] == "utf-8"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {("AAA",), ("BBB",), (".ge_store_backend_id",)}
    assert {
        s3_object_info["Key"]
        for s3_object_info in boto3.client("s3").list_objects_v2(
            Bucket=bucket, Prefix=prefix
        )["Contents"]
    } == {
        "this_is_a_test_prefix/.ge_store_backend_id",
        "this_is_a_test_prefix/my_file_AAA",
        "this_is_a_test_prefix/my_file_BBB",
    }

    assert my_store.get_url_for_key(
        ("AAA",)
    ) == "https://s3.amazonaws.com/{}/{}/my_file_AAA".format(bucket, prefix)
    assert my_store.get_url_for_key(
        ("BBB",)
    ) == "https://s3.amazonaws.com/{}/{}/my_file_BBB".format(bucket, prefix)

    my_store.remove_key(("BBB",))
    with pytest.raises(InvalidKeyError):
        my_store.get(("BBB",))

    # testing base_public_path
    my_new_store = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
        base_public_path=base_public_path,
    )

    my_new_store.set(("BBB",), "bbb", content_type="text/html; charset=utf-8")

    assert (
        my_new_store.get_public_url_for_key(("BBB",))
        == "http://www.test.com/my_file_BBB"
    )


@mock_s3
def test_tuple_s3_store_backend_slash_conditions():
    bucket = "my_bucket"
    prefix = None
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    client = boto3.client("s3")

    my_store = TupleS3StoreBackend(
        bucket=bucket,
        prefix=prefix,
        platform_specific_separator=False,
        filepath_prefix="foo__",
        filepath_suffix="__bar.json",
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo__/.ge_store_backend_id", "foo__/my_suite__bar.json"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo__/my_suite__bar.json"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    my_store = TupleS3StoreBackend(
        bucket=bucket,
        prefix=prefix,
        platform_specific_separator=False,
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = [".ge_store_backend_id", "my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    my_store = TupleS3StoreBackend(
        bucket=bucket,
        prefix=prefix,
        platform_specific_separator=True,
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = [".ge_store_backend_id", "my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    prefix = "/foo/"
    my_store = TupleS3StoreBackend(
        bucket=bucket, prefix=prefix, platform_specific_separator=True
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo/.ge_store_backend_id", "foo/my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    prefix = "foo"
    my_store = TupleS3StoreBackend(
        bucket=bucket, prefix=prefix, platform_specific_separator=True
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo/.ge_store_backend_id", "foo/my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    my_store = TupleS3StoreBackend(
        bucket=bucket, prefix=prefix, platform_specific_separator=False
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo/.ge_store_backend_id", "foo/my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    prefix = "foo/"
    my_store = TupleS3StoreBackend(
        bucket=bucket, prefix=prefix, platform_specific_separator=True
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo/.ge_store_backend_id", "foo/my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    my_store = TupleS3StoreBackend(
        bucket=bucket, prefix=prefix, platform_specific_separator=False
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo/.ge_store_backend_id", "foo/my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    prefix = "/foo"
    my_store = TupleS3StoreBackend(
        bucket=bucket, prefix=prefix, platform_specific_separator=True
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo/.ge_store_backend_id", "foo/my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo/my_suite"
    )

    client.delete_objects(
        Bucket=bucket, Delete={"Objects": [{"Key": key} for key in expected_s3_keys]}
    )
    assert len(client.list_objects_v2(Bucket=bucket).get("Contents", [])) == 0
    my_store = TupleS3StoreBackend(
        bucket=bucket, prefix=prefix, platform_specific_separator=False
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["foo/.ge_store_backend_id", "foo/my_suite"]
    assert [
        obj["Key"] for obj in client.list_objects_v2(Bucket=bucket)["Contents"]
    ] == expected_s3_keys
    assert (
        my_store.get_url_for_key(("my_suite",))
        == "https://s3.amazonaws.com/my_bucket/foo/my_suite"
    )


@mock_s3
def test_TupleS3StoreBackend_with_empty_prefixes():
    """
    What does this test test and why?

    We will exercise the store backend's set method twice and then verify
    that the we calling get and list methods will return the expected keys.

    We will also check that the objects are stored on S3 at the expected location,
    and that the correct S3 URL for the object can be retrieved.
    """
    bucket = "leakybucket"
    prefix = ""

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    my_store = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )

    # We should be able to list keys, even when empty
    keys = my_store.list_keys()
    assert len(keys) == 1

    my_store.set(("AAA",), "aaa", content_type="text/html; charset=utf-8")
    assert my_store.get(("AAA",)) == "aaa"

    obj = boto3.client("s3").get_object(Bucket=bucket, Key=prefix + "my_file_AAA")
    assert my_store._build_s3_object_key(("AAA",)) == "my_file_AAA"
    assert obj["ContentType"] == "text/html; charset=utf-8"
    assert obj["ContentEncoding"] == "utf-8"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {("AAA",), ("BBB",), (".ge_store_backend_id",)}
    assert {
        s3_object_info["Key"]
        for s3_object_info in boto3.client("s3").list_objects_v2(
            Bucket=bucket, Prefix=prefix
        )["Contents"]
    } == {"my_file_AAA", "my_file_BBB", ".ge_store_backend_id"}

    assert (
        my_store.get_url_for_key(("AAA",))
        == "https://s3.amazonaws.com/leakybucket/my_file_AAA"
    )
    assert (
        my_store.get_url_for_key(("BBB",))
        == "https://s3.amazonaws.com/leakybucket/my_file_BBB"
    )


def test_TupleGCSStoreBackend_base_public_path():
    """
    What does this test and why?

    the base_public_path parameter allows users to point to a custom DNS when hosting Data docs.

    This test will exercise the get_url_for_key method twice to see that we are getting the expected url,
    with or without base_public_path
    """
    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"
    project = "dummy-project"
    base_public_path = "http://www.test.com/"

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store_with_base_public_path = TupleGCSStoreBackend(
            filepath_template=None,
            bucket=bucket,
            prefix=prefix,
            project=project,
            base_public_path=base_public_path,
        )

        my_store_with_base_public_path.set(
            ("BBB",), b"bbb", content_encoding=None, content_type="image/png"
        )

    run_id = RunIdentifier("my_run_id", datetime.datetime.utcnow())
    key = ValidationResultIdentifier(
        ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
        run_id,
        "my_batch_id",
    )
    run_time_string = run_id.to_tuple()[1]

    url = my_store_with_base_public_path.get_public_url_for_key(key.to_tuple())
    assert (
        url
        == "http://www.test.com/leakybucket"
        + f"/this_is_a_test_prefix/my_suite_name/my_run_id/{run_time_string}/my_batch_id"
    )


def test_TupleGCSStoreBackend():
    # pytest.importorskip("google-cloud-storage")
    """
    What does this test test and why?

    Since no package like moto exists for GCP services, we mock the GCS client
    and assert that the store backend makes the right calls for set, get, and list.

    TODO : One option may be to have a GCS Store in Docker, which can be use to "actually" run these tests.
    """

    bucket = "leakybucket"
    prefix = "this_is_a_test_prefix"
    project = "dummy-project"
    base_public_path = "http://www.test.com/"

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store = TupleGCSStoreBackend(
            filepath_template="my_file_{0}",
            bucket=bucket,
            prefix=prefix,
            project=project,
        )

        my_store.set(("AAA",), "aaa", content_type="text/html")

        mock_gcs_client.assert_called_with("dummy-project")
        mock_client.get_bucket.assert_called_with("leakybucket")
        mock_bucket.blob.assert_called_with("this_is_a_test_prefix/my_file_AAA")
        # mock_bucket.blob.assert_any_call("this_is_a_test_prefix/.ge_store_backend_id")
        mock_blob.upload_from_string.assert_called_with(
            b"aaa", content_type="text/html"
        )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store_with_no_filepath_template = TupleGCSStoreBackend(
            filepath_template=None, bucket=bucket, prefix=prefix, project=project
        )

        my_store_with_no_filepath_template.set(
            ("AAA",), b"aaa", content_encoding=None, content_type="image/png"
        )

        mock_gcs_client.assert_called_with("dummy-project")
        mock_client.get_bucket.assert_called_with("leakybucket")
        mock_bucket.blob.assert_called_with("this_is_a_test_prefix/AAA")
        # mock_bucket.blob.assert_any_call("this_is_a_test_prefix/.ge_store_backend_id")
        mock_blob.upload_from_string.assert_called_with(
            b"aaa", content_type="image/png"
        )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.get_blob.return_value
        mock_str = mock_blob.download_as_string.return_value

        my_store.get(("BBB",))

        mock_gcs_client.assert_called_once_with("dummy-project")
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.get_blob.assert_called_once_with(
            "this_is_a_test_prefix/my_file_BBB"
        )
        mock_blob.download_as_string.assert_called_once()
        mock_str.decode.assert_called_once_with("utf-8")

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value

        my_store.list_keys()

        mock_client.list_blobs.assert_called_once_with(
            "leakybucket", prefix="this_is_a_test_prefix"
        )

        my_store.remove_key("leakybucket")

        from google.cloud.exceptions import NotFound

        try:
            mock_client.get_bucket.assert_called_once_with("leakybucket")
        except NotFound:
            pass

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        mock_gcs_client.side_effect = InvalidKeyError("Hi I am an InvalidKeyError")
        with pytest.raises(InvalidKeyError):
            my_store.get(("non_existent_key",))

    run_id = RunIdentifier("my_run_id", datetime.datetime.utcnow())
    key = ValidationResultIdentifier(
        ExpectationSuiteIdentifier(expectation_suite_name="my_suite_name"),
        run_id,
        "my_batch_id",
    )
    run_time_string = run_id.to_tuple()[1]

    url = my_store_with_no_filepath_template.get_url_for_key(key.to_tuple())
    assert (
        url
        == "https://storage.googleapis.com/leakybucket"
        + f"/this_is_a_test_prefix/my_suite_name/my_run_id/{run_time_string}/my_batch_id"
    )


def test_TupleAzureBlobStoreBackend():
    pytest.importorskip("azure-storage-blob")
    """
    What does this test test and why?
    Since no package like moto exists for Azure-Blob services, we mock the Azure-blob client
    and assert that the store backend makes the right calls for set, get, and list.
    """
    connection_string = "this_is_a_test_conn_string"
    prefix = "this_is_a_test_prefix"
    container = "dummy-container"

    my_store = TupleAzureBlobStoreBackend(
        connection_string=connection_string, prefix=prefix, container=container
    )

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.set(("AAA",), "aaa")

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.upload_blob.assert_called_once_with(
            name="AAA", data=b"aaa", encoding="utf-8"
        )

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.set(("BBB",), b"bbb")

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.upload_blob.assert_called_once_with(
            name="AAA", data=b"aaa"
        )

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.get(("BBB",))

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.download_blob.assert_called_once_with("BBB")

    with patch(
        "azure.storage.blob.BlobServiceClient", autospec=True
    ) as mock_azure_blob_client:

        mock_container_client = mock_azure_blob_client.get_container_client.return_value

        my_store.list_keys()

        mock_azure_blob_client.from_connection_string.assert_called_once()
        mock_container_client.assert_called_once()
        mock_container_client.list_blobs.assert_called_once_with("this_is_a_prefix")


@mock_s3
def test_TupleS3StoreBackend_list_over_1000_keys():
    """
    What does this test test and why?

    TupleS3StoreBackend.list_keys() should be able to list over 1000 keys
    which is the current limit for boto3.list_objects and boto3.list_objects_v2 methods.
    See https://boto3.amazonaws.com/v1/documentation/api/latest/guide/paginators.html

    We will create a bucket with over 1000 keys, list them with TupleS3StoreBackend.list_keys()
    and make sure all are accounted for.
    """
    bucket = "leakybucket"
    prefix = "my_prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    # Assert that the bucket is empty before creating store
    assert (
        boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents")
        is None
    )

    my_store = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=prefix,
    )

    # We should be able to list keys, even when empty
    # len(keys) == 1 because of the .ge_store_backend_id
    keys = my_store.list_keys()
    assert len(keys) == 1

    # Add more than 1000 keys
    max_keys_in_a_single_call = 1000
    num_keys_to_add = int(1.2 * max_keys_in_a_single_call)

    for key_num in range(num_keys_to_add):
        my_store.set(
            (f"AAA_{key_num}",),
            f"aaa_{key_num}",
            content_type="text/html; charset=utf-8",
        )
    assert my_store.get(("AAA_0",)) == "aaa_0"
    assert my_store.get((f"AAA_{num_keys_to_add-1}",)) == f"aaa_{num_keys_to_add-1}"

    # Without pagination only list max_keys_in_a_single_call
    # This is belt and suspenders to make sure mocking s3 list_objects_v2 implements
    # the same limit as the actual s3 api
    assert (
        len(
            boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix)["Contents"]
        )
        == max_keys_in_a_single_call
    )

    # With pagination list all keys
    keys = my_store.list_keys()
    # len(keys) == num_keys_to_add + 1 because of the .ge_store_backend_id
    assert len(keys) == num_keys_to_add + 1
