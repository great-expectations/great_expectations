import uuid
from typing import Dict, Optional, Set

import boto3
import pyparsing as pp
import pytest
from moto import mock_s3

from great_expectations.data_context import get_context
from great_expectations.data_context.store import StoreBackend, TupleS3StoreBackend
from great_expectations.data_context.types.base import DataContextConfig


def build_in_code_data_context_project_config(
    bucket: str = "leakybucket",
    expectations_store_prefix: str = "expectations_store_prefix",
    checkpoint_store_prefix: str = "checkpoint_store_prefix",
    validation_results_store_prefix: str = "validation_results_store_prefix",
    data_docs_store_prefix: str = "data_docs_store_prefix",
    stores: Optional[Dict] = None,
) -> DataContextConfig:
    """
    Create a project config for an in-code data context.
    Not a fixture because we want to control when this is built (after the expectation store).
    Args:
        expectations_store_prefix: prefix for expectations store
        checkpoint_store_prefix: prefix for checkpoint store
        validation_results_store_prefix: prefix for validations store
        data_docs_store_prefix: prefix for data docs
        bucket: name of the s3 bucket
        stores: optional overwrite of the default stores

    Returns:
        DataContextConfig using s3 for all stores.
    """
    if stores is None:
        stores = {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket,
                    "prefix": expectations_store_prefix,
                },
            },
            "validation_results_store": {
                "class_name": "ValidationResultsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket,
                    "prefix": validation_results_store_prefix,
                },
            },
            "suite_parameter_store": {"class_name": "SuiteParameterStore"},
            "checkpoint_store": {"class_name": "CheckpointStore"},
        }
    project_config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,
        stores=stores,
        data_docs_sites={
            "s3_site": {
                "class_name": "SiteBuilder",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket,
                    "prefix": data_docs_store_prefix,
                },
                "site_index_builder": {
                    "class_name": "DefaultSiteIndexBuilder",
                },
            }
        },
    )
    return project_config


def get_store_backend_id_from_s3(bucket: str, prefix: str, key: str) -> uuid.UUID:
    """
    Return the UUID store_backend_id from a given s3 file
    Args:
        bucket: s3 bucket
        prefix: prefix for s3 bucket
        key: filename in s3 bucket

    Returns:

    """
    s3_response_object = boto3.client("s3").get_object(Bucket=bucket, Key=f"{prefix}/{key}")
    ge_store_backend_id_file_contents = (
        s3_response_object["Body"].read().decode(s3_response_object.get("ContentEncoding", "utf-8"))
    )

    store_backend_id_file_parser = StoreBackend.STORE_BACKEND_ID_PREFIX + pp.Word(pp.hexnums + "-")
    parsed_store_backend_id = store_backend_id_file_parser.parseString(
        ge_store_backend_id_file_contents
    )
    return uuid.UUID(parsed_store_backend_id[1])


def list_s3_bucket_contents(bucket: str, prefix: str) -> Set[str]:
    """
    List the contents of an s3 bucket as a set of strings given bucket name and prefix
    Args:
        bucket: s3 bucket
        prefix: prefix for s3 bucket

    Returns:
        set of filepath strings
    """
    return {
        s3_object_info["Key"]
        for s3_object_info in boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix)[
            "Contents"
        ]
    }


@pytest.mark.aws_deps
@mock_s3
def test_DataContext_construct_data_context_id_uses_id_of_currently_configured_expectations_store(
    aws_credentials,
):
    """
    What does this test and why?

    A DataContext should have an id. This ID should come from either:
    1. configured expectations store store_backend_id
    2. great_expectations.yml
    3. new generated id from DataContextConfig
    This test verifies that DataContext._construct_data_context_id
    uses the store_backend_id from the currently configured expectations store
    when instantiating the DataContext
    """

    store_backend_id_filename = StoreBackend.STORE_BACKEND_ID_KEY[0]
    bucket = "leakybucket"
    expectations_store_prefix = "expectations_store_prefix"
    validation_results_store_prefix = "validation_results_store_prefix"
    data_docs_store_prefix = "data_docs_store_prefix"
    data_context_prefix = ""

    # Create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    # Create a TupleS3StoreBackend
    # Initialize without store_backend_id and check that the store_backend_id is generated correctly
    s3_expectations_store_backend = TupleS3StoreBackend(
        filepath_template="my_file_{0}",
        bucket=bucket,
        prefix=expectations_store_prefix,
    )
    # Make sure store_backend_id is not the error string
    store_error_uuid = uuid.UUID("00000000-0000-0000-0000-00000000e003")
    s3_expectations_store_backend_id = s3_expectations_store_backend.store_backend_id
    assert s3_expectations_store_backend_id != store_error_uuid

    # Make sure the bucket contents are as expected
    bucket_contents_after_creating_expectation_store = list_s3_bucket_contents(
        bucket=bucket, prefix=data_context_prefix
    )
    assert bucket_contents_after_creating_expectation_store == {
        f"{expectations_store_prefix}/{store_backend_id_filename}"
    }

    # Make sure the store_backend_id from the file is equal to reading from the property
    expectations_store_backend_id_from_s3_file = get_store_backend_id_from_s3(
        bucket=bucket,
        prefix=expectations_store_prefix,
        key=store_backend_id_filename,
    )
    assert expectations_store_backend_id_from_s3_file == s3_expectations_store_backend_id

    # Create a DataContext (note existing expectations store already set up)
    in_code_data_context_project_config = build_in_code_data_context_project_config(
        bucket="leakybucket",
        expectations_store_prefix=expectations_store_prefix,
        validation_results_store_prefix=validation_results_store_prefix,
        data_docs_store_prefix=data_docs_store_prefix,
    )
    in_code_data_context = get_context(project_config=in_code_data_context_project_config)
    bucket_contents_after_instantiating_get_context = list_s3_bucket_contents(
        bucket=bucket, prefix=data_context_prefix
    )
    assert bucket_contents_after_instantiating_get_context == {
        f"{expectations_store_prefix}/{store_backend_id_filename}",
        f"{validation_results_store_prefix}/{store_backend_id_filename}",
    }

    # Make sure ids are consistent
    in_code_data_context_expectations_store_store_backend_id = in_code_data_context.stores[
        "expectations_store"
    ].store_backend_id
    in_code_data_context_data_context_id = in_code_data_context.data_context_id
    constructed_data_context_id = in_code_data_context._construct_data_context_id()
    assert (
        in_code_data_context_expectations_store_store_backend_id
        == in_code_data_context_data_context_id
        == expectations_store_backend_id_from_s3_file
        == s3_expectations_store_backend_id
        == constructed_data_context_id
    )


@pytest.mark.aws_deps
@mock_s3
def test_DataContext_construct_data_context_id_uses_id_stored_in_DataContextConfig_if_no_configured_expectations_store(  # noqa: E501
    monkeypatch, aws_credentials
):
    """
    What does this test and why?

    A DataContext should have an id. This ID should come from either:
    1. configured expectations store store_backend_id
    2. great_expectations.yml
    3. new generated id from DataContextConfig
    This test verifies that DataContext._construct_data_context_id
    uses the data_context_id from DataContextConfig when there is no configured expectations store
    when instantiating the DataContext,
    and also that this data_context_id is used to configure the expectations_store.store_backend_id
    """
    bucket = "leakybucket"
    expectations_store_prefix = "expectations_store_prefix"
    validation_results_store_prefix = "validation_results_store_prefix"
    data_docs_store_prefix = "data_docs_store_prefix"
    manually_created_uuid = uuid.UUID("00000000-0000-0000-0000-000000000eee")

    # Create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    # Create a DataContext (note NO existing expectations store already set up)
    in_code_data_context_project_config = build_in_code_data_context_project_config(
        bucket="leakybucket",
        expectations_store_prefix=expectations_store_prefix,
        validation_results_store_prefix=validation_results_store_prefix,
        data_docs_store_prefix=data_docs_store_prefix,
    )
    # Manually set the data_context_id in the project_config
    in_code_data_context_project_config.data_context_id = manually_created_uuid
    in_code_data_context = get_context(project_config=in_code_data_context_project_config)

    # Make sure the manually set data_context_id is propagated to all the appropriate places
    assert (
        manually_created_uuid
        == in_code_data_context.data_context_id
        == in_code_data_context.stores["expectations_store"].store_backend_id
    )
