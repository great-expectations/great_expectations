import pytest
import yaml
import json

from typing import List

import boto3
from moto import mock_s3

import pandas as pd

from great_expectations.execution_environment.data_connector import ConfiguredAssetS3DataConnector
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
    PartitionDefinition,
)
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions.exceptions as ge_exceptions


@mock_s3
def test_basic_instantiation():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key
        )

    my_data_connector = ConfiguredAssetS3DataConnector(
        name="my_data_connector",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        bucket=bucket,
        prefix="",
        assets={
            "alpha": {}
        }
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetS3DataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
                "batch_definition_count": 3
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []

    # Illegal execution environment name
    with pytest.raises(ValueError):
        print(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
            execution_environment_name="something",
            data_connector_name="my_data_connector",
            data_asset_name="something",
        )))


@mock_s3
def test_instantiation_from_a_config(empty_data_context):
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key
        )

    return_object = empty_data_context.test_yaml_config(f"""
module_name: great_expectations.execution_environment.data_connector
class_name: ConfiguredAssetS3DataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR

default_regex:
    pattern: alpha-(.*)\\.csv
    group_names:
        - index

bucket: {bucket}
prefix: ""

assets:
    alpha:
    """, return_mode="return_object")

    assert return_object == {
        "class_name": "ConfiguredAssetS3DataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
                "batch_definition_count": 3
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }

