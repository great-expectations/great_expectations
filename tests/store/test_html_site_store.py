import pytest
from marshmallow import ValidationError

from moto import mock_s3
import boto3

from great_expectations.data_context.types.resource_identifiers import validationResultIdentifierSchema
from great_expectations.exceptions import MissingTopLevelConfigKeyError
from great_expectations.util import (
    gen_directory_tree_str,
)
from great_expectations.data_context.types import (
    ValidationResultIdentifier,
    SiteSectionIdentifier,
    DataAssetIdentifier,
    ExpectationSuiteIdentifier
)
from great_expectations.data_context.store import (
    HtmlSiteStore
)


def test_HtmlSiteStore_filesystem_backend(tmp_path_factory):

    path = str(tmp_path_factory.mktemp('test_HtmlSiteStore_with_FixedLengthTupleFileSystemStoreBackend__dir'))

    my_store = HtmlSiteStore(
        root_directory=path,
        store_backend={
            "class_name": "FixedLengthTupleFilesystemStoreBackend",
            "base_directory": "my_store"
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    with pytest.raises(ValidationError):
        my_store.get(validationResultIdentifierSchema.load({}).data)
    
    ns_1 = SiteSectionIdentifier(
        site_section_name="validations",
        resource_identifier=ValidationResultIdentifier.from_tuple(('a', 'b', 'c', 'quarantine', 'prod-100'))
    )
    my_store.set(ns_1, "aaa")
    # assert my_store.get(ns_1) == "aaa"

    ns_2 = SiteSectionIdentifier(
        site_section_name="validations",
        resource_identifier=ValidationResultIdentifier.from_tuple(('a', 'b', 'c', 'quarantine', 'prod-20'))
    )
    my_store.set(ns_2, "bbb")
    # assert my_store.get(ns_2) == {"B": "bbb"}

    print(my_store.list_keys())
    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }

    print(gen_directory_tree_str(path))
    assert gen_directory_tree_str(path) == """\
test_HtmlSiteStore_with_FixedLengthTupleFileSystemStoreBackend__dir0/
    my_store/
        validations/
            prod-100/
                a/
                    b/
                        c/
                            quarantine.html
            prod-20/
                a/
                    b/
                        c/
                            quarantine.html
"""


@mock_s3
def test_HtmlSiteStore_S3_backend():
    bucket = "test_validation_store_bucket"
    prefix = "test/prefix"

    # create a bucket in Moto's mock AWS environment
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=bucket)

    my_store = HtmlSiteStore(
        root_directory='NOT_USED_WITH_S3',
        store_backend={
            "class_name": "FixedLengthTupleS3StoreBackend",
            "bucket": bucket,
            "prefix": prefix
        }
    )

    with pytest.raises(TypeError):
        my_store.get("not_a_ValidationResultIdentifier")

    ns_1 = SiteSectionIdentifier(
        site_section_name="validations",
        resource_identifier=ValidationResultIdentifier(
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
    )
    my_store.set(ns_1, "aaa")

    ns_2 = SiteSectionIdentifier(
        site_section_name="expectations",
        resource_identifier=ExpectationSuiteIdentifier(
            data_asset_name=DataAssetIdentifier(
                datasource="a",
                generator="b",
                generator_asset="c"
            ),
            expectation_suite_name="quarantine",
        )
    )
    my_store.set(ns_2, "bbb")

    assert set(my_store.list_keys()) == {
        ns_1,
        ns_2,
    }

    # This is a special un-store-like method exposed by the HtmlSiteStore
    my_store.write_index_page("index_html_string_content")

    # Verify that internals are working as expected, including the default filepath
    assert set(
        [s3_object_info['Key'] for s3_object_info in
         boto3.client('s3').list_objects(Bucket=bucket, Prefix=prefix)['Contents']
         ]
    ) == {
        'test/prefix/index.html',
        'test/prefix/expectations/a/b/c/quarantine.html',
        'test/prefix/validations/20191007T151224.1234Z_prod_100/a/b/c/quarantine.html'
    }

    index_content = boto3.client('s3').get_object(Bucket=bucket, Key='test/prefix/index.html')["Body"]\
        .read().decode('utf-8')
    assert index_content == "index_html_string_content"
