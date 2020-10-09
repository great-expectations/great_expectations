import datetime
import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3

from great_expectations.core import RunIdentifier
from great_expectations.data_context.store import (
    InMemoryStoreBackend,
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


def test_InMemoryStoreBackend():

    my_store = InMemoryStoreBackend()

    my_key = ("A",)
    with pytest.raises(KeyError):
        my_store.get(my_key)

    my_store.set(my_key, "aaa")
    assert my_store.get(my_key) == "aaa"

    my_store.set(("B",), {"x": 1})

    assert my_store.has_key(my_key) is True
    assert my_store.has_key(("B",)) is True
    assert my_store.has_key(("A",)) is True
    assert my_store.has_key(("C",)) is False
    assert my_store.list_keys() == [("A",), ("B",)]

    with pytest.raises(StoreError):
        my_store.get_url_for_key(my_key)


def test_tuple_filesystem_store_filepath_prefix_error(tmp_path_factory):
    path = str(
        tmp_path_factory.mktemp("test_tuple_filesystem_store_filepath_prefix_error")
    )
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=os.path.abspath(path),
            base_directory=project_path,
            filepath_prefix="invalid_prefix_ends_with/",
        )
    assert "filepath_prefix may not end with" in e.value.message

    with pytest.raises(StoreBackendError) as e:
        TupleFilesystemStoreBackend(
            root_directory=os.path.abspath(path),
            base_directory=project_path,
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
        root_directory=os.path.abspath(path),
        base_directory=project_path,
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
        root_directory=os.path.abspath(path),
        base_directory=project_path,
        filepath_template="my_file_{0}",
    )

    with pytest.raises(InvalidKeyError):
        my_store.get(("AAA",))

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {("AAA",), ("BBB",)}

    assert (
        gen_directory_tree_str(project_path)
        == """\
test_TupleFilesystemStoreBackend__dir0/
    my_file_AAA
    my_file_BBB
"""
    )
    my_store.remove_key(("BBB",))
    with pytest.raises(InvalidKeyError):
        assert my_store.get(("BBB",)) == ""

    my_store_with_base_public_path = TupleFilesystemStoreBackend(
        root_directory=os.path.abspath(path),
        base_directory=project_path,
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
        root_directory=os.path.abspath("dummy_str"), base_directory=project_path,
    )

    my_store.set(("AAA",), "aaa")
    assert my_store.get(("AAA",)) == "aaa"

    assert (
        gen_directory_tree_str(project_path)
        == """\
things0/
    AAA
    .ipynb_checkpoints/
        foo.json
"""
    )

    assert set(my_store.list_keys()) == {("AAA",)}


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
        filepath_template="my_file_{0}", bucket=bucket, prefix=prefix,
    )

    # We should be able to list keys, even when empty
    keys = my_store.list_keys()
    assert len(keys) == 0

    my_store.set(("AAA",), "aaa", content_type="text/html; charset=utf-8")
    assert my_store.get(("AAA",)) == "aaa"

    obj = boto3.client("s3").get_object(Bucket=bucket, Key=prefix + "/my_file_AAA")
    assert obj["ContentType"] == "text/html; charset=utf-8"
    assert obj["ContentEncoding"] == "utf-8"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {("AAA",), ("BBB",)}
    assert {
        s3_object_info["Key"]
        for s3_object_info in boto3.client("s3").list_objects_v2(
            Bucket=bucket, Prefix=prefix
        )["Contents"]
    } == {"this_is_a_test_prefix/my_file_AAA", "this_is_a_test_prefix/my_file_BBB"}

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
    expected_s3_keys = ["foo__/my_suite__bar.json"]
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
        bucket=bucket, prefix=prefix, platform_specific_separator=False,
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["my_suite"]
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
        bucket=bucket, prefix=prefix, platform_specific_separator=True,
    )
    my_store.set(("my_suite",), '{"foo": "bar"}')
    expected_s3_keys = ["my_suite"]
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
    expected_s3_keys = ["foo/my_suite"]
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
    expected_s3_keys = ["foo/my_suite"]
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
    expected_s3_keys = ["foo/my_suite"]
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
    expected_s3_keys = ["foo/my_suite"]
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
    expected_s3_keys = ["foo/my_suite"]
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
    expected_s3_keys = ["foo/my_suite"]
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
    expected_s3_keys = ["foo/my_suite"]
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
        filepath_template="my_file_{0}", bucket=bucket, prefix=prefix,
    )

    # We should be able to list keys, even when empty
    keys = my_store.list_keys()
    assert len(keys) == 0

    my_store.set(("AAA",), "aaa", content_type="text/html; charset=utf-8")
    assert my_store.get(("AAA",)) == "aaa"

    obj = boto3.client("s3").get_object(Bucket=bucket, Key=prefix + "my_file_AAA")
    assert my_store._build_s3_object_key(("AAA",)) == "my_file_AAA"
    assert obj["ContentType"] == "text/html; charset=utf-8"
    assert obj["ContentEncoding"] == "utf-8"

    my_store.set(("BBB",), "bbb")
    assert my_store.get(("BBB",)) == "bbb"

    assert set(my_store.list_keys()) == {("AAA",), ("BBB",)}
    assert {
        s3_object_info["Key"]
        for s3_object_info in boto3.client("s3").list_objects_v2(
            Bucket=bucket, Prefix=prefix
        )["Contents"]
    } == {"my_file_AAA", "my_file_BBB"}

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

    my_store_with_base_public_path = TupleGCSStoreBackend(
        filepath_template=None,
        bucket=bucket,
        prefix=prefix,
        project=project,
        base_public_path=base_public_path,
    )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

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

    my_store = TupleGCSStoreBackend(
        filepath_template="my_file_{0}", bucket=bucket, prefix=prefix, project=project
    )

    my_store_with_no_filepath_template = TupleGCSStoreBackend(
        filepath_template=None, bucket=bucket, prefix=prefix, project=project
    )

    my_store_with_base_public_path = TupleGCSStoreBackend(
        filepath_template=None,
        bucket=bucket,
        prefix=prefix,
        project=project,
        base_public_path=base_public_path,
    )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:

        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store.set(("AAA",), "aaa", content_type="text/html")

        mock_gcs_client.assert_called_once_with("dummy-project")
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.blob.assert_called_once_with("this_is_a_test_prefix/my_file_AAA")
        mock_blob.upload_from_string.assert_called_once_with(
            b"aaa", content_type="text/html"
        )

    with patch("google.cloud.storage.Client", autospec=True) as mock_gcs_client:
        mock_client = mock_gcs_client.return_value
        mock_bucket = mock_client.get_bucket.return_value
        mock_blob = mock_bucket.blob.return_value

        my_store_with_no_filepath_template.set(
            ("AAA",), b"aaa", content_encoding=None, content_type="image/png"
        )

        mock_gcs_client.assert_called_once_with("dummy-project")
        mock_client.get_bucket.assert_called_once_with("leakybucket")
        mock_bucket.blob.assert_called_once_with("this_is_a_test_prefix/AAA")
        mock_blob.upload_from_string.assert_called_once_with(
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
