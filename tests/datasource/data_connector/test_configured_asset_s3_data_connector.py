import os
from typing import List

import boto3
import pandas as pd
import pytest
from moto import mock_s3

import great_expectations.exceptions.exceptions as gx_exceptions
from great_expectations.core.batch import (
    BatchRequest,
    BatchRequestBase,
    IDDict,
    LegacyBatchDefinition,
)
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import ConfiguredAssetS3DataConnector
from great_expectations.datasource.data_connector.util import (
    sanitize_prefix,
    sanitize_prefix_for_gcs_and_s3,
)
from great_expectations.execution_engine import PandasExecutionEngine

yaml = YAMLHandler()

# module level markers
pytestmark = pytest.mark.big


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
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector = ConfiguredAssetS3DataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        execution_engine=PandasExecutionEngine(),
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        bucket=bucket,
        prefix="",
        assets={"alpha": {}},
    )

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []

    # Illegal execution environment name
    with pytest.raises(ValueError):
        print(
            my_data_connector.get_batch_definition_list_from_batch_request(
                BatchRequest(
                    datasource_name="something",
                    data_connector_name="my_data_connector",
                    data_asset_name="something",
                )
            )
        )


@mock_s3
def test_return_all_batch_definitions_unsorted():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
            class_name: ConfiguredAssetS3DataConnector
            datasource_name: test_environment
            bucket: {bucket}
            prefix: ""
            assets:
                TestFiles:
            default_regex:
                pattern: (.+)_(.+)_(.+)\\.csv
                group_names:
                    - name
                    - timestamp
                    - price
        """,
    )

    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_s3_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list_from_batch_request()

    # with unnamed data_asset_name
    with pytest.raises(TypeError):
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_s3_data_connector",
                data_asset_name="",
            )
        )

    # with unnamed data_asset_name
    unsorted_batch_definition_list = (
        my_data_connector._get_batch_definition_list_from_batch_request(
            BatchRequestBase(
                datasource_name="test_environment",
                data_connector_name="general_s3_data_connector",
                data_asset_name="",
            )
        )
    )
    expected = [
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "abe", "timestamp": "20200809", "price": "1040"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200809", "price": "1000"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200819", "price": "1300"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20200809", "price": "1500"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20201129", "price": "1900"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200713", "price": "1567"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200810", "price": "1003"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200811", "price": "1009"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200809", "price": "1002"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200810", "price": "1001"}),
        ),
    ]
    assert expected == unsorted_batch_definition_list

    # with named data_asset_name
    unsorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
        )
    )
    assert expected == unsorted_batch_definition_list


@mock_s3
def test_return_all_batch_definitions_sorted():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
        class_name: ConfiguredAssetS3DataConnector
        datasource_name: test_environment
        bucket: {bucket}
        prefix: ""
        assets:
            TestFiles:
        default_regex:
            pattern: (.+)_(.+)_(.+)\\.csv
            group_names:
                - name
                - timestamp
                - price
        sorters:
            - orderby: asc
              class_name: LexicographicSorter
              name: name
            - datetime_format: "%Y%m%d"
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: price

    """,
    )

    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_s3_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        BatchRequest(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
        )
    )

    expected = [
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "abe", "timestamp": "20200809", "price": "1040"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200819", "price": "1300"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "alex", "timestamp": "20200809", "price": "1000"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20201129", "price": "1900"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "eugene", "timestamp": "20200809", "price": "1500"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200811", "price": "1009"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200810", "price": "1003"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "james", "timestamp": "20200713", "price": "1567"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200810", "price": "1001"}),
        ),
        LegacyBatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_s3_data_connector",
            data_asset_name="TestFiles",
            batch_identifiers=IDDict({"name": "will", "timestamp": "20200809", "price": "1002"}),
        ),
    ]

    # TEST 1: Sorting works
    assert expected == sorted_batch_definition_list

    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_s3_data_connector",
        data_asset_name="TestFiles",
        data_connector_query=IDDict(
            **{
                "batch_filter_parameters": {
                    "name": "james",
                    "timestamp": "20200713",
                    "price": "1567",
                }
            }
        ),
    )

    my_batch_definition_list: List[LegacyBatchDefinition]
    my_batch_definition: LegacyBatchDefinition

    # TEST 2: Should only return the specified partition
    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )

    assert len(my_batch_definition_list) == 1
    my_batch_definition = my_batch_definition_list[0]
    expected_batch_definition = LegacyBatchDefinition(
        datasource_name="test_environment",
        data_connector_name="general_s3_data_connector",
        data_asset_name="TestFiles",
        batch_identifiers=IDDict(
            **{
                "name": "james",
                "timestamp": "20200713",
                "price": "1567",
            }
        ),
    )
    assert my_batch_definition == expected_batch_definition

    # TEST 3: Without data_connector_query, should return all 10
    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_s3_data_connector",
        data_asset_name="TestFiles",
        data_connector_query=None,
    )
    # should return 10
    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 10


@mock_s3
def test_alpha():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "test_dir_alpha/A.csv",
        "test_dir_alpha/B.csv",
        "test_dir_alpha/C.csv",
        "test_dir_alpha/D.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
                module_name: great_expectations.datasource.data_connector
                class_name: ConfiguredAssetS3DataConnector
                datasource_name: BASE
                bucket: {bucket}
                prefix: test_dir_alpha
                assets:
                  A:
                default_regex:
                    pattern: .*(.+)\\.csv
                    group_names:
                    - part_1
            """,
    )

    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_s3_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )

    my_batch_definition_list: List[LegacyBatchDefinition]
    my_batch_definition: LegacyBatchDefinition

    # Try to fetch a batch from a nonexistent asset
    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="BASE",
        data_connector_name="general_s3_data_connector",
        data_asset_name="B",
        data_connector_query=None,
    )

    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 0

    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="BASE",
        data_connector_name="general_s3_data_connector",
        data_asset_name="A",
        data_connector_query=IDDict(**{"batch_filter_parameters": {"part_1": "B"}}),
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 1


@mock_s3
def test_foxtrot():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "test_dir_foxtrot/A/A-1.csv",
        "test_dir_foxtrot/A/A-2.csv",
        "test_dir_foxtrot/A/A-3.csv",
        "test_dir_foxtrot/B/B-1.txt",
        "test_dir_foxtrot/B/B-2.txt",
        "test_dir_foxtrot/B/B-3.txt",
        "test_dir_foxtrot/C/C-2017.csv",
        "test_dir_foxtrot/C/C-2018.csv",
        "test_dir_foxtrot/C/C-2019.csv",
        "test_dir_foxtrot/D/D-aaa.csv",
        "test_dir_foxtrot/D/D-bbb.csv",
        "test_dir_foxtrot/D/D-ccc.csv",
        "test_dir_foxtrot/D/D-ddd.csv",
        "test_dir_foxtrot/D/D-eee.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
            module_name: great_expectations.datasource.data_connector
            class_name: ConfiguredAssetS3DataConnector
            datasource_name: BASE
            bucket: {bucket}
            prefix: test_dir_foxtrot
            assets:
              A:
                prefix: test_dir_foxtrot/A/
              B:
                prefix: test_dir_foxtrot/B/
                pattern: (.+)-(.+)\\.txt
                group_names:
                - part_1
                - part_2
              C:
                prefix: test_dir_foxtrot/C/
              D:
                prefix: test_dir_foxtrot/D/
            default_regex:
                pattern: (.+)-(.+)\\.csv
                group_names:
                - part_1
                - part_2
        """,
    )

    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_s3_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
    )
    my_batch_definition_list: List[LegacyBatchDefinition]
    my_batch_definition: LegacyBatchDefinition
    my_batch_request = BatchRequest(
        datasource_name="BASE",
        data_connector_name="general_s3_data_connector",
        data_asset_name="A",
        data_connector_query=None,
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 3


@mock_s3
def test_return_all_batch_definitions_raises_error_due_to_sorter_that_does_not_match_group():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        """
        class_name: ConfiguredAssetS3DataConnector
        datasource_name: test_environment
        bucket: bucket
        assets:
            TestFiles:
                pattern: (.+)_(.+)_(.+)\\.csv
                group_names:
                    - name
                    - timestamp
                    - price
        default_regex:
            pattern: (.+)_.+_.+\\.csv
            group_names:
                - name
        sorters:
            - orderby: asc
              class_name: LexicographicSorter
              name: name
            - datetime_format: "%Y%m%d"
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: for_me_Me_Me
    """,
    )
    with pytest.raises(gx_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: ConfiguredAssetS3DataConnector = (  # noqa: F841
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "general_s3_data_connector",
                    "execution_engine": PandasExecutionEngine(),
                },
                config_defaults={"module_name": "great_expectations.datasource.data_connector"},
            )
        )


@mock_s3
def test_return_all_batch_definitions_too_many_sorters():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alex_20200809_1000.csv",
        "eugene_20200809_1500.csv",
        "james_20200811_1009.csv",
        "abe_20200809_1040.csv",
        "will_20200809_1002.csv",
        "james_20200713_1567.csv",
        "eugene_20201129_1900.csv",
        "will_20200810_1001.csv",
        "james_20200810_1003.csv",
        "alex_20200819_1300.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    my_data_connector_yaml = yaml.load(
        f"""
        class_name: ConfiguredAssetS3DataConnector
        datasource_name: test_environment
        bucket: {bucket}
        prefix: ""
        assets:
            TestFiles:
        default_regex:
            pattern: (.+)_.+_.+\\.csv
            group_names:
                - name
        sorters:
            - orderby: asc
              class_name: LexicographicSorter
              name: name
            - datetime_format: "%Y%m%d"
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: price

    """,
    )
    with pytest.raises(gx_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: ConfiguredAssetS3DataConnector = (  # noqa: F841
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "general_s3_data_connector",
                    "execution_engine": PandasExecutionEngine(),
                },
                config_defaults={"module_name": "great_expectations.datasource.data_connector"},
            )
        )


@mock_s3
def test_example_with_explicit_data_asset_names():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "my_base_directory/alpha/files/go/here/alpha-202001.csv",
        "my_base_directory/alpha/files/go/here/alpha-202002.csv",
        "my_base_directory/alpha/files/go/here/alpha-202003.csv",
        "my_base_directory/beta_here/beta-202001.txt",
        "my_base_directory/beta_here/beta-202002.txt",
        "my_base_directory/beta_here/beta-202003.txt",
        "my_base_directory/beta_here/beta-202004.txt",
        "my_base_directory/gamma-202001.csv",
        "my_base_directory/gamma-202002.csv",
        "my_base_directory/gamma-202003.csv",
        "my_base_directory/gamma-202004.csv",
        "my_base_directory/gamma-202005.csv",
    ]
    for key in keys:
        client.put_object(Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key)

    yaml_string = f"""
class_name: ConfiguredAssetS3DataConnector
datasource_name: FAKE_DATASOURCE_NAME
bucket: {bucket}
prefix: my_base_directory/
default_regex:
    pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
    group_names:
        - data_asset_name
        - year_dir
        - month_dir
assets:
    alpha:
        prefix: my_base_directory/alpha/files/go/here/
        pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.csv$
    beta:
        prefix: my_base_directory/beta_here/
        pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.txt$
    gamma:
        pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.csv$

    """
    config = yaml.load(yaml_string)
    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        runtime_environment={
            "name": "my_data_connector",
            "execution_engine": PandasExecutionEngine(),
        },
    )
    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    assert len(my_data_connector.get_unmatched_data_references()) == 0

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
                    datasource_name="FAKE_DATASOURCE_NAME",
                    data_connector_name="my_data_connector",
                    data_asset_name="beta",
                )
            )
        )
        == 4
    )

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    datasource_name="FAKE_DATASOURCE_NAME",
                    data_connector_name="my_data_connector",
                    data_asset_name="gamma",
                )
            )
        )
        == 5
    )


def test_sanitize_prefix_behaves_the_same_as_local_files():
    def check_sameness(prefix, expected_output):
        s3_sanitized = sanitize_prefix_for_gcs_and_s3(text=prefix)
        file_system_sanitized = sanitize_prefix(prefix)
        if os.sep == "\\":  # Fix to ensure tests work on Windows
            file_system_sanitized = file_system_sanitized.replace("\\", "/")

        assert file_system_sanitized == expected_output, (
            f"Expected output does not match original sanitization behavior, got "
            f"{file_system_sanitized} instead of {expected_output}"
        )
        assert (
            s3_sanitized == expected_output == file_system_sanitized
        ), f'S3 sanitized result is incorrect, "{s3_sanitized} instead of {expected_output}'

    # Copy of all samples from tests/datasource/data_connector/test_file_path_data_connector.py
    check_sameness("foo/", "foo/")
    check_sameness("bar", "bar/")
    check_sameness("baz.txt", "baz.txt")
    check_sameness("a/b/c/baz.txt", "a/b/c/baz.txt")

    # A couple additional checks
    check_sameness("a/b/c", "a/b/c/")
    check_sameness("a.x/b/c", "a.x/b/c/")
    check_sameness("path/to/folder.something/", "path/to/folder.something/")
    check_sameness("path/to/folder.something", "path/to/folder.something")
