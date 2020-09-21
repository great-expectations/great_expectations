import datetime

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_s3

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.store import ValidationsStore
from great_expectations.data_context.types.resource_identifiers import (
    ExpectationSuiteIdentifier,
    ValidationResultIdentifier,
)
from great_expectations.util import gen_directory_tree_str


@freeze_time("09/26/2019 13:42:41")
@mock_s3
def test_ValidationsStore_with_TupleS3StoreBackend():
    bucket = "test_validation_store_bucket"
    prefix = "test/prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    # First, demonstrate that we pick up default configuration including from an S3TupleS3StoreBackend
    my_store = ValidationsStore(
        store_backend={
            "class_name": "TupleS3StoreBackend",
            "bucket": bucket,
            "prefix": prefix,
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="asset.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_100",
        batch_identifier="batch_id",
    )
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(
        success=True, statistics={}, results=[]
    )

    ns_2 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="asset.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_200",
        batch_identifier="batch_id",
    )

    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(
        success=False, statistics={}, results=[]
    )

    # Verify that internals are working as expected, including the default filepath
    assert {
        s3_object_info["Key"]
        for s3_object_info in boto3.client("s3").list_objects_v2(
            Bucket=bucket, Prefix=prefix
        )["Contents"]
    } == {
        "test/prefix/asset/quarantine/20191007T151224.1234Z_prod_100/20190926T134241.000000Z/batch_id.json",
        "test/prefix/asset/quarantine/20191007T151224.1234Z_prod_200/20190926T134241.000000Z/batch_id.json",
    }

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


@freeze_time("09/26/2019 13:42:41")
def test_ValidationsStore_with_InMemoryStoreBackend():
    my_store = ValidationsStore(
        store_backend={
            "module_name": "great_expectations.data_context.store",
            "class_name": "InMemoryStoreBackend",
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier.from_tuple(
        (
            "a",
            "b",
            "c",
            "quarantine",
            datetime.datetime.now(datetime.timezone.utc),
            "prod-100",
        )
    )
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(
        success=True, statistics={}, results=[]
    )

    ns_2 = ValidationResultIdentifier.from_tuple(
        (
            "a",
            "b",
            "c",
            "quarantine",
            datetime.datetime.now(datetime.timezone.utc),
            "prod-200",
        )
    )
    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(
        success=False, statistics={}, results=[]
    )

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


@freeze_time("09/26/2019 13:42:41")
def test_ValidationsStore_with_TupleFileSystemStoreBackend(tmp_path_factory):
    path = str(
        tmp_path_factory.mktemp(
            "test_ValidationResultStore_with_TupleFileSystemStoreBackend__dir"
        )
    )
    project_path = str(tmp_path_factory.mktemp("my_dir"))

    my_store = ValidationsStore(
        store_backend={
            "module_name": "great_expectations.data_context.store",
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": "my_store/",
        },
        runtime_environment={"root_directory": path},
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier("asset.quarantine"),
        run_id="prod-100",
        batch_identifier="batch_id",
    )
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(
        success=True, statistics={}, results=[]
    )

    ns_2 = ValidationResultIdentifier.from_tuple(
        (
            "asset",
            "quarantine",
            "prod-20",
            datetime.datetime.now(datetime.timezone.utc),
            "batch_id",
        )
    )
    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(
        success=False, statistics={}, results=[]
    )

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }

    print(gen_directory_tree_str(path))
    assert (
        gen_directory_tree_str(path)
        == """\
test_ValidationResultStore_with_TupleFileSystemStoreBackend__dir0/
    my_store/
        asset/
            quarantine/
                prod-100/
                    20190926T134241.000000Z/
                        batch_id.json
                prod-20/
                    20190926T134241.000000Z/
                        batch_id.json
"""
    )


def test_ValidationsStore_with_DatabaseStoreBackend(sa):
    # Use sqlite so we don't require postgres for this test.
    connection_kwargs = {"drivername": "sqlite"}

    # First, demonstrate that we pick up default configuration
    my_store = ValidationsStore(
        store_backend={
            "class_name": "DatabaseStoreBackend",
            "credentials": connection_kwargs,
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="asset.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_100",
        batch_identifier="batch_id",
    )
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(
        success=True, statistics={}, results=[]
    )

    ns_2 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="asset.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_200",
        batch_identifier="batch_id",
    )

    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(
        success=False, statistics={}, results=[]
    )

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }
