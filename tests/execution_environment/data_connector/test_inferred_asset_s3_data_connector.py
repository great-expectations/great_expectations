import pytest
import yaml

from typing import List

import boto3
from moto import mock_s3

import pandas as pd

from great_expectations.execution_environment.data_connector import InferredAssetS3DataConnector
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
)
from great_expectations.data_context.util import instantiate_class_from_config
import great_expectations.exceptions.exceptions as ge_exceptions


@mock_s3
def test_test_yaml_config(empty_data_context):
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key
        )

    return_object = empty_data_context.test_yaml_config(f"""
module_name: great_expectations.execution_environment.data_connector
class_name: InferredAssetS3DataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR
bucket: {bucket}
prefix: ""
default_regex:
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """, return_mode="return_object")

    assert return_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "alpha",
            "beta"
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["2020/01/alpha-*.csv", "2020/02/alpha-*.csv", "2020/03/alpha-*.csv"],
                "batch_definition_count": 3
            },
            "beta": {
                "example_data_references": ["2020/01/beta-*.csv", "2020/02/beta-*.csv", "2020/03/beta-*.csv"],
                "batch_definition_count": 4
            }
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }


@mock_s3
def test_yaml_config_excluding_non_regex_matching_files(empty_data_context):
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "2020/01/alpha-1001.csv",
        "2020/01/beta-1002.csv",
        "2020/02/alpha-1003.csv",
        "2020/02/beta-1004.csv",
        "2020/03/alpha-1005.csv",
        "2020/03/beta-1006.csv",
        "2020/04/beta-1007.csv",
        "gamma-202001.csv",
        "gamma-202002.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket,
            Body=test_df.to_csv(index=False).encode("utf-8"),
            Key=key
        )

    return_object = empty_data_context.test_yaml_config(
        f"""
module_name: great_expectations.execution_environment.data_connector
class_name: InferredAssetS3DataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR

bucket: {bucket}
prefix: ""

default_regex:
    pattern: (\\d{{4}})/(\\d{{2}})/(.*)-.*\\.csv
    group_names:
        - year_dir
        - month_dir
        - data_asset_name
    """,
        return_mode="return_object",
    )

    assert return_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": [
            "alpha",
            "beta"
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": ["2020/01/alpha-*.csv", "2020/02/alpha-*.csv", "2020/03/alpha-*.csv"],
                "batch_definition_count": 3
            },
            "beta": {
                "example_data_references": ["2020/01/beta-*.csv", "2020/02/beta-*.csv", "2020/03/beta-*.csv"],
                "batch_definition_count": 4
            }
        },
        "example_unmatched_data_references": ['gamma-202001.csv', 'gamma-202002.csv'],
        "unmatched_data_reference_count": 2,
    }
