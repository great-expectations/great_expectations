import pytest
from moto import mock_s3
import boto3

from great_expectations.core import ExpectationSuiteValidationResult
from great_expectations.data_context.store import (
    ValidationsStore,
)
from great_expectations.data_context.types import (
    ExpectationSuiteIdentifier,
    DataAssetIdentifier,
    ValidationResultIdentifier,
)

from great_expectations.util import (
    gen_directory_tree_str,
)


@mock_s3
def test_ValidationsStore_with_FixedLengthTupleS3StoreBackend():
    bucket = "test_validation_store_bucket"
    prefix = "test/prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket)

    # First, demonstrate that we pick up default configuration including from an S3FixedLengthTupleS3StoreBackend
    my_store = ValidationsStore(
        store_backend={
            "class_name": "FixedLengthTupleS3StoreBackend",
            "bucket": bucket,
            "prefix": prefix
        },
        root_directory=None
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier(
                datasource="a",
                generator="b",
                generator_asset="c"
            ),
            expectation_suite_name="quarantine",
        ),
        run_id="20191007T151224.1234Z_prod_100"
    )
    my_store.set(ns_1, ExpectationSuiteValidationResult(success=True))
    assert my_store.get(ns_1) == ExpectationSuiteValidationResult(success=True, statistics={}, results=[])

    ns_2 = ValidationResultIdentifier(
        expectation_suite_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier(
                datasource="a",
                generator="b",
                generator_asset="c"
            ),
            expectation_suite_name="quarantine",
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
            "separator": ".",
        },
        root_directory=None,
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


def test_ValidationsStore__convert_resource_identifier_to_list():

    my_store = ValidationsStore(
        store_backend={
            "module_name": "great_expectations.data_context.store",
            "class_name": "InMemoryStoreBackend",
        },
        root_directory=None,
    )

    ns_1 = ValidationResultIdentifier.from_tuple(("a", "b", "c", "quarantine", "prod-100"))
    assert my_store._convert_resource_identifier_to_tuple(ns_1) == ('a', 'b', 'c', 'quarantine', 'prod-100')


def test_ValidationsStore_with_FixedLengthTupleFileSystemStoreBackend(tmp_path_factory):
    path = str(tmp_path_factory.mktemp('test_ValidationResultStore_with_FixedLengthTupleFileSystemStoreBackend__dir'))
    project_path = str(tmp_path_factory.mktemp('my_dir'))

    my_store = ValidationsStore(
        store_backend={
            "module_name": "great_expectations.data_context.store",
            "class_name": "FixedLengthTupleFilesystemStoreBackend",
            "base_directory": "my_store/",
            "filepath_template": "{4}/{0}/{1}/{2}/{3}.txt",
        },
        root_directory=path,
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
test_ValidationResultStore_with_FixedLengthTupleFileSystemStoreBackend__dir0/
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
