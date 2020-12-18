from typing import List

import boto3
import pandas as pd
import pytest
from moto import mock_s3
from ruamel.yaml import YAML

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import InferredAssetS3DataConnector

yaml = YAML()


@mock_s3
def test_basic_instantiation():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "path/A-100.csv",
        "path/A-101.csv",
        "directory/B-1.csv",
        "directory/B-2.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: InferredAssetS3DataConnector = InferredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": r"(.+)/(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "letter", "number"],
        },
        bucket=bucket,
        prefix="",
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    assert my_data_connector.get_data_reference_list_count() == 4
    assert my_data_connector.get_unmatched_data_references() == []

    # Illegal execution environment name
    with pytest.raises(ValueError):
        print(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="something",
                    data_connector_name="my_data_connector",
                    data_asset_name="something",
                )
            )
        )


@mock_s3
def test_simple_regex_example_with_implicit_data_asset_names_self_check():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "A-100.csv",
        "A-101.csv",
        "B-1.csv",
        "B-2.csv",
        "CCC.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: InferredAssetS3DataConnector = InferredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": r"(.+)-(\d+)\.csv",
            "group_names": [
                "data_asset_name",
                "number",
            ],
        },
        bucket=bucket,
        prefix="",
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    self_check_report_object = my_data_connector.self_check()

    assert self_check_report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["A", "B"],
        "data_assets": {
            "A": {
                "example_data_references": ["A-100.csv", "A-101.csv"],
                "batch_definition_count": 2,
            },
            "B": {
                "example_data_references": ["B-1.csv", "B-2.csv"],
                "batch_definition_count": 2,
            },
        },
        "example_unmatched_data_references": ["CCC.csv"],
        "unmatched_data_reference_count": 1,
        "example_data_reference": {},
    }


@mock_s3
def test_complex_regex_example_with_implicit_data_asset_names():
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
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: InferredAssetS3DataConnector = InferredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": r"(\d{4})/(\d{2})/(.+)-\d+\.csv",
            "group_names": ["year_dir", "month_dir", "data_asset_name"],
        },
        bucket=bucket,
        prefix="",
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    # Test for an unknown execution environment
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="non_existent_datasource",
                data_connector_name="my_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    # Test for an unknown data_connector
    with pytest.raises(ValueError):
        # noinspection PyUnusedLocal
        batch_definition_list: List[
            BatchDefinition
        ] = my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=BatchRequest(
                datasource_name="FAKE_DATASOURCE_NAME",
                data_connector_name="non_existent_data_connector",
                data_asset_name="my_data_asset",
            )
        )

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="FAKE_DATASOURCE_NAME",
                    data_connector_name="my_data_connector",
                    data_asset_name="alpha",
                )
            )
        )
        == 3
    )

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    data_connector_name="my_data_connector",
                    data_asset_name="alpha",
                )
            )
        )
        == 3
    )

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    data_connector_name="my_data_connector",
                    data_asset_name="beta",
                )
            )
        )
        == 4
    )

    assert my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=BatchRequest(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            partition_request={
                "partition_identifiers": {
                    "year_dir": "2020",
                    "month_dir": "03",
                }
            },
        )
    ) == [
        BatchDefinition(
            datasource_name="FAKE_DATASOURCE_NAME",
            data_connector_name="my_data_connector",
            data_asset_name="alpha",
            partition_definition=PartitionDefinition(
                year_dir="2020",
                month_dir="03",
            ),
        )
    ]


@mock_s3
def test_self_check():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "A-100.csv",
        "A-101.csv",
        "B-1.csv",
        "B-2.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: InferredAssetS3DataConnector = InferredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": r"(.+)-(\d+)\.csv",
            "group_names": ["data_asset_name", "number"],
        },
        bucket=bucket,
        prefix="",
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    self_check_report_object = my_data_connector.self_check()

    assert self_check_report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["A", "B"],
        "data_assets": {
            "A": {
                "example_data_references": ["A-100.csv", "A-101.csv"],
                "batch_definition_count": 2,
            },
            "B": {
                "example_data_references": ["B-1.csv", "B-2.csv"],
                "batch_definition_count": 2,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
        "example_data_reference": {},
    }


@mock_s3
def test_test_yaml_config(empty_data_context):
    context = empty_data_context

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
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    report_object = context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: InferredAssetS3DataConnector
datasource_name: FAKE_DATASOURCE
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
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["alpha", "beta"],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "2020/01/alpha-*.csv",
                    "2020/02/alpha-*.csv",
                    "2020/03/alpha-*.csv",
                ],
                "batch_definition_count": 3,
            },
            "beta": {
                "example_data_references": [
                    "2020/01/beta-*.csv",
                    "2020/02/beta-*.csv",
                    "2020/03/beta-*.csv",
                ],
                "batch_definition_count": 4,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
        "example_data_reference": {},
    }


@mock_s3
def test_yaml_config_excluding_non_regex_matching_files(empty_data_context):
    context = empty_data_context

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
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    report_object = context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: InferredAssetS3DataConnector
datasource_name: FAKE_DATASOURCE
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
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 2,
        "example_data_asset_names": ["alpha", "beta"],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "2020/01/alpha-*.csv",
                    "2020/02/alpha-*.csv",
                    "2020/03/alpha-*.csv",
                ],
                "batch_definition_count": 3,
            },
            "beta": {
                "example_data_references": [
                    "2020/01/beta-*.csv",
                    "2020/02/beta-*.csv",
                    "2020/03/beta-*.csv",
                ],
                "batch_definition_count": 4,
            },
        },
        "example_unmatched_data_references": ["gamma-202001.csv", "gamma-202002.csv"],
        "unmatched_data_reference_count": 2,
        "example_data_reference": {},
    }


@mock_s3
def test_nested_directory_data_asset_name_in_folder(empty_data_context):
    context = empty_data_context

    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "A/A-1.csv",
        "A/A-2.csv",
        "A/A-3.csv",
        "B/B-1.csv",
        "B/B-2.csv",
        "B/B-3.csv",
        "C/C-1.csv",
        "C/C-2.csv",
        "C/C-3.csv",
        "D/D-1.csv",
        "D/D-2.csv",
        "D/D-3.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    report_object = context.test_yaml_config(
        f"""
    module_name: great_expectations.datasource.data_connector
    class_name: InferredAssetS3DataConnector
    datasource_name: FAKE_DATASOURCE
    name: TEST_DATA_CONNECTOR
    bucket: {bucket}
    prefix: ""
    default_regex:
        group_names:
            - data_asset_name
            - letter
            - number
        pattern: (\\w{{1}})\\/(\\w{{1}})-(\\d{{1}})\\.csv
        """,
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 4,
        "example_data_asset_names": ["A", "B", "C"],
        "data_assets": {
            "A": {
                "batch_definition_count": 3,
                "example_data_references": ["A/A-1.csv", "A/A-2.csv", "A/A-3.csv"],
            },
            "B": {
                "batch_definition_count": 3,
                "example_data_references": ["B/B-1.csv", "B/B-2.csv", "B/B-3.csv"],
            },
            "C": {
                "batch_definition_count": 3,
                "example_data_references": ["C/C-1.csv", "C/C-2.csv", "C/C-3.csv"],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {},
    }


@mock_s3
def test_redundant_information_in_naming_convention_random_hash(empty_data_context):
    context = empty_data_context

    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "2021/01/01/log_file-2f1e94b40f310274b485e72050daf591.txt.gz",
        "2021/01/02/log_file-7f5d35d4f90bce5bf1fad680daac48a2.txt.gz",
        "2021/01/03/log_file-99d5ed1123f877c714bbe9a2cfdffc4b.txt.gz",
        "2021/01/04/log_file-885d40a5661bbbea053b2405face042f.txt.gz",
        "2021/01/05/log_file-d8e478f817b608729cfc8fb750ebfc84.txt.gz",
        "2021/01/06/log_file-b1ca8d1079c00fd4e210f7ef31549162.txt.gz",
        "2021/01/07/log_file-d34b4818c52e74b7827504920af19a5c.txt.gz",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    report_object = context.test_yaml_config(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetS3DataConnector
          datasource_name: FAKE_DATASOURCE
          name: TEST_DATA_CONNECTOR
          bucket: {bucket}
          prefix: ""
          default_regex:
              group_names:
                - year
                - month
                - day
                - data_asset_name
              pattern: (\\d{{4}})/(\\d{{2}})/(\\d{{2}})/(log_file)-.*\\.txt\\.gz

              """,
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["log_file"],
        "data_assets": {
            "log_file": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "2021/01/01/log_file-*.txt.gz",
                    "2021/01/02/log_file-*.txt.gz",
                    "2021/01/03/log_file-*.txt.gz",
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {},
    }


@mock_s3
def test_redundant_information_in_naming_convention_timestamp(empty_data_context):
    context = empty_data_context

    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "log_file-2021-01-01-035419.163324.txt.gz",
        "log_file-2021-01-02-035513.905752.txt.gz",
        "log_file-2021-01-03-035455.848839.txt.gz",
        "log_file-2021-01-04-035251.47582.txt.gz",
        "log_file-2021-01-05-033034.289789.txt.gz",
        "log_file-2021-01-06-034958.505688.txt.gz",
        "log_file-2021-01-07-033545.600898.txt.gz",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    report_object = context.test_yaml_config(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetS3DataConnector
          datasource_name: FAKE_DATASOURCE
          name: TEST_DATA_CONNECTOR
          bucket: {bucket}
          prefix: ""
          default_regex:
              group_names:
                - data_asset_name
                - year
                - month
                - day
              pattern: (log_file)-(\\d{{4}})-(\\d{{2}})-(\\d{{2}})-.*\\.*\\.txt\\.gz
      """,
        return_mode="report_object",
    )
    assert report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["log_file"],
        "data_assets": {
            "log_file": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "log_file-2021-01-01-*.txt.gz",
                    "log_file-2021-01-02-*.txt.gz",
                    "log_file-2021-01-03-*.txt.gz",
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {},
    }


@mock_s3
def test_redundant_information_in_naming_convention_bucket(empty_data_context):
    context = empty_data_context

    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    report_object = context.test_yaml_config(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetS3DataConnector
          datasource_name: FAKE_DATASOURCE
          name: TEST_DATA_CONNECTOR
          bucket: {bucket}
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-.*\\.txt\\.gz
              """,
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "InferredAssetS3DataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["some_bucket"],
        "data_assets": {
            "some_bucket": {
                "batch_definition_count": 7,
                "example_data_references": [
                    "some_bucket/2021/01/01/log_file-*.txt.gz",
                    "some_bucket/2021/01/02/log_file-*.txt.gz",
                    "some_bucket/2021/01/03/log_file-*.txt.gz",
                ],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {},
    }


@mock_s3
def test_redundant_information_in_naming_convention_bucket_sorted():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetS3DataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket: {bucket}
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: full_date

          """,
    )

    my_data_connector: InferredAssetS3DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "my_inferred_asset_filesystem_data_connector",
            "datasource_name": "test_environment",
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    sorted_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="test_environment",
                data_connector_name="my_inferred_asset_filesystem_data_connector",
                data_asset_name="some_bucket",
            )
        )
    )

    expected = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            partition_definition=PartitionDefinition(
                {"year": "2021", "month": "01", "day": "07", "full_date": "20210107"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            partition_definition=PartitionDefinition(
                {"year": "2021", "month": "01", "day": "06", "full_date": "20210106"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            partition_definition=PartitionDefinition(
                {"year": "2021", "month": "01", "day": "05", "full_date": "20210105"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            partition_definition=PartitionDefinition(
                {"year": "2021", "month": "01", "day": "04", "full_date": "20210104"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            partition_definition=PartitionDefinition(
                {"year": "2021", "month": "01", "day": "03", "full_date": "20210103"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            partition_definition=PartitionDefinition(
                {"year": "2021", "month": "01", "day": "02", "full_date": "20210102"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="my_inferred_asset_filesystem_data_connector",
            data_asset_name="some_bucket",
            partition_definition=PartitionDefinition(
                {"year": "2021", "month": "01", "day": "01", "full_date": "20210101"}
            ),
        ),
    ]
    assert expected == sorted_batch_definition_list


@mock_s3
def test_redundant_information_in_naming_convention_bucket_sorter_does_not_match_group():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector_yaml = yaml.load(
        f"""
          module_name: great_expectations.datasource.data_connector
          class_name: InferredAssetS3DataConnector
          datasource_name: test_environment
          name: my_inferred_asset_filesystem_data_connector
          bucket: {bucket}
          prefix: ""
          default_regex:
              group_names:
                  - data_asset_name
                  - year
                  - month
                  - day
                  - full_date
              pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
          sorters:
              - orderby: desc
                class_name: DateTimeSorter
                name: not_matching_anything

          """,
    )

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: InferredAssetS3DataConnector = instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_inferred_asset_filesystem_data_connector",
                "datasource_name": "test_environment",
                "execution_engine": "BASE_ENGINE",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )


@mock_s3
def test_redundant_information_in_naming_convention_bucket_too_many_sorters():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "some_bucket/2021/01/01/log_file-20210101.txt.gz",
        "some_bucket/2021/01/02/log_file-20210102.txt.gz",
        "some_bucket/2021/01/03/log_file-20210103.txt.gz",
        "some_bucket/2021/01/04/log_file-20210104.txt.gz",
        "some_bucket/2021/01/05/log_file-20210105.txt.gz",
        "some_bucket/2021/01/06/log_file-20210106.txt.gz",
        "some_bucket/2021/01/07/log_file-20210107.txt.gz",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector_yaml = yaml.load(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: InferredAssetS3DataConnector
        datasource_name: test_environment
        name: my_inferred_asset_filesystem_data_connector
        bucket: {bucket}
        prefix: ""
        default_regex:
            group_names:
                - data_asset_name
                - year
                - month
                - day
                - full_date
            pattern: (\\w{{11}})/(\\d{{4}})/(\\d{{2}})/(\\d{{2}})/log_file-(.*)\\.txt\\.gz
        sorters:
            - datetime_format: "%Y%m%d"
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: price
          """,
    )

    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: InferredAssetS3DataConnector = instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_inferred_asset_filesystem_data_connector",
                "datasource_name": "test_environment",
                "execution_engine": "BASE_ENGINE",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
