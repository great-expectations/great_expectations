import json

import pytest
from moto import mock_s3
import boto3

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.store import (
    ValidationsStore,
)
from great_expectations.data_context.types.resource_identifiers import ValidationResultIdentifier, \
    ExpectationSuiteIdentifier

from great_expectations.util import (
    gen_directory_tree_str,
)


@mock_s3
def test_ValidationsStore_with_TupleS3StoreBackend():
    bucket = "test_validation_store_bucket"
    prefix = "test/prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket)

    # First, demonstrate that we pick up default configuration including from an S3TupleS3StoreBackend
    my_store = ValidationsStore(
        store_backend={
            "class_name": "TupleS3StoreBackend",
            "bucket": bucket,
            "prefix": prefix
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="a.b.c.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_100"
    )
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(success=True, statistics={}, results=[])

    ns_2 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="a.b.c.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_200"
    )

    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(success=False, statistics={}, results=[])

    # Verify that internals are working as expected, including the default filepath
    assert set(
        [s3_object_info['Key'] for s3_object_info in
         boto3.client('s3').list_objects(Bucket=bucket, Prefix=prefix)['Contents']
         ]
    ) == {'test/prefix/20191007T151224.1234Z_prod_100/a/b/c/quarantine.json',
          'test/prefix/20191007T151224.1234Z_prod_200/a/b/c/quarantine.json'}

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


def test_ValidationsStore_with_InMemoryStoreBackend():
    my_store = ValidationsStore(
        store_backend={
            "module_name": "great_expectations.data_context.store",
            "class_name": "InMemoryStoreBackend",
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier.from_tuple(("a", "b", "c", "quarantine", "prod-100"))
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(success=True, statistics={}, results=[])

    ns_2 = ValidationResultIdentifier.from_tuple(("a", "b", "c", "quarantine", "prod-200"))
    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(success=False, statistics={}, results=[])

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }


def test_ValidationsStore_with_TupleFileSystemStoreBackend(tmp_path_factory):
    path = str(tmp_path_factory.mktemp('test_ValidationResultStore_with_TupleFileSystemStoreBackend__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = ValidationsStore(
        store_backend={
            "module_name": "great_expectations.data_context.store",
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": "my_store/",
            "filepath_template": "{4}/{0}/{1}/{2}/{3}.txt",
        },
        runtime_environment={
            "root_directory": path
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier.from_tuple(("a", "b", "c", "quarantine", "prod-100"))
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(success=True, statistics={}, results=[])

    ns_2 = ValidationResultIdentifier.from_tuple(("a", "b", "c", "quarantine", "prod-20"))
    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(success=False, statistics={}, results=[])

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }

    print(gen_directory_tree_str(path))
    assert gen_directory_tree_str(path) == """\
test_ValidationResultStore_with_TupleFileSystemStoreBackend__dir0/
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


def test_ValidationsStore_with_DatabaseStoreBackend():
    # Use sqlite so we don't require postgres for this test.
    connection_kwargs = {
        "drivername": "sqlite"
    }

    # First, demonstrate that we pick up default configuration
    my_store = ValidationsStore(
        store_backend={
            "class_name": "DatabaseStoreBackend",
            "credentials": connection_kwargs
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="a.b.c.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_100"
    )
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(success=True, statistics={}, results=[])

    ns_2 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            expectation_suite_name="a.b.c.quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_200"
    )

    my_store.set(ns_2, ExpectationSuiteValidationResult(success=False))
    assert my_store.get(ns_2) == ExpectationSuiteValidationResult(success=False, statistics={}, results=[])

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }
