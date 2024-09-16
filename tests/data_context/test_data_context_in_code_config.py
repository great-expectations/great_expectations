import uuid
from typing import Dict, Optional, Set

import boto3
import pyparsing as pp
import pytest
from moto import mock_s3

from great_expectations.data_context import get_context
from great_expectations.data_context.store import StoreBackend
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
            "expectations_S3_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket,
                    "prefix": expectations_store_prefix,
                },
            },
            "validation_results_S3_store": {
                "class_name": "ValidationResultsStore",
                "store_backend": {
                    "class_name": "TupleS3StoreBackend",
                    "bucket": bucket,
                    "prefix": validation_results_store_prefix,
                },
            },
            "checkpoint_store": {"class_name": "CheckpointStore"},
        }
    project_config = DataContextConfig(
        config_version=2,
        plugins_directory=None,
        config_variables_file_path=None,
        stores=stores,
        checkpoint_store_name="checkpoint_store",
        expectations_store_name="expectations_S3_store",
        validation_results_store_name="validation_results_S3_store",
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
def test_inaccessible_active_bucket_warning_messages(caplog, aws_credentials):
    """
    What does this test do and why?

    Trying to create a data context with unreachable ACTIVE stores should show a warning message once per store
    e.g. Invalid store configuration: Please check the configuration of your TupleS3StoreBackend named expectations_S3_store
    Active stores are those named in:
    "expectations_store_name", "validation_results_store_name"
    """  # noqa: E501

    bucket = "leakybucket"
    expectations_store_prefix = "expectations_store_prefix"
    validation_results_store_prefix = "validation_results_store_prefix"
    data_docs_store_prefix = "data_docs_store_prefix"

    # Create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    # Create a DataContext
    # Add inactive stores
    inactive_bucket = "inactive_leakybucket"
    stores = {
        "expectations_S3_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": inactive_bucket,
                "prefix": expectations_store_prefix,
            },
        },
        "validation_results_S3_store": {
            "class_name": "ValidationResultsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": inactive_bucket,
                "prefix": validation_results_store_prefix,
            },
        },
    }
    in_code_data_context_project_config = build_in_code_data_context_project_config(
        bucket="leakybucket",
        expectations_store_prefix=expectations_store_prefix,
        validation_results_store_prefix=validation_results_store_prefix,
        data_docs_store_prefix=data_docs_store_prefix,
        stores=stores,
    )
    _ = get_context(project_config=in_code_data_context_project_config)
    assert (
        caplog.messages.count(
            "Invalid store configuration: Please check the configuration of your TupleS3StoreBackend named expectations_S3_store. Exception was: \n Unable to set object in s3."  # noqa: E501
        )
        == 1
    )
    assert (
        caplog.messages.count(
            "Invalid store configuration: Please check the configuration of your TupleS3StoreBackend named validation_results_S3_store. Exception was: \n Unable to set object in s3."  # noqa: E501
        )
        == 1
    )


@pytest.mark.big
@mock_s3
def test_inaccessible_inactive_bucket_no_warning_messages(caplog):
    """
    What does this test do and why?

    Trying to create a data context with unreachable INACTIVE stores should show no warning messages
    Inactive stores are those NOT named in:
    "expectations_store_name", "validation_results_store_name"
    """

    bucket = "leakybucket"
    expectations_store_prefix = "expectations_store_prefix"
    validation_results_store_prefix = "validation_results_store_prefix"
    data_docs_store_prefix = "data_docs_store_prefix"

    # Create a bucket in Moto's mock AWS environment
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket=bucket)

    # Create a DataContext
    # Add inactive stores
    inactive_bucket = "inactive_leakybucket"
    stores = {
        "expectations_S3_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": bucket,
                "prefix": expectations_store_prefix,
            },
        },
        "validation_results_S3_store": {
            "class_name": "ValidationResultsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": bucket,
                "prefix": validation_results_store_prefix,
            },
        },
        "inactive_expectations_S3_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": inactive_bucket,
                "prefix": expectations_store_prefix,
            },
        },
        "inactive_validation_results_S3_store": {
            "class_name": "ValidationResultsStore",
            "store_backend": {
                "class_name": "TupleS3StoreBackend",
                "bucket": inactive_bucket,
                "prefix": validation_results_store_prefix,
            },
        },
    }
    in_code_data_context_project_config = build_in_code_data_context_project_config(
        bucket="leakybucket",
        expectations_store_prefix=expectations_store_prefix,
        validation_results_store_prefix=validation_results_store_prefix,
        data_docs_store_prefix=data_docs_store_prefix,
        stores=stores,
    )
    _ = get_context(project_config=in_code_data_context_project_config)
    assert (
        caplog.messages.count(
            "Invalid store configuration: Please check the configuration of your TupleS3StoreBackend named expectations_S3_store"  # noqa: E501
        )
        == 0
    )
    assert (
        caplog.messages.count(
            "Invalid store configuration: Please check the configuration of your TupleS3StoreBackend named validation_results_S3_store"  # noqa: E501
        )
        == 0
    )
