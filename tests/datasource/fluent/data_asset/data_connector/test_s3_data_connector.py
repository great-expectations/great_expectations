import logging
import os
import re
from typing import TYPE_CHECKING, List

import pandas as pd
import pytest
from moto import mock_s3

from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.util import S3Url
from great_expectations.datasource.data_connector.util import (
    sanitize_prefix,
    sanitize_prefix_for_gcs_and_s3,
)
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.data_asset.data_connector import (
    S3DataConnector,
)

if TYPE_CHECKING:
    from botocore.client import BaseClient

    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )

logger = logging.getLogger(__name__)

try:
    import boto3  # : disable=E0602
except ImportError:
    logger.debug("Unable to load boto3; install optional boto3 dependency for support.")


@pytest.mark.integration
@mock_s3
def test_basic_instantiation():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client: BaseClient = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: DataConnector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"alpha-(.*)\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0


@pytest.mark.integration
@mock_s3
def test_instantiation_batching_regex_does_not_match_paths():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client: BaseClient = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: DataConnector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"beta-(.*)\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 0
    assert my_data_connector.get_matched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_references()[:3] == [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]
    assert my_data_connector.get_unmatched_data_reference_count() == 3


@pytest.mark.integration
@mock_s3
def test_return_all_batch_definitions_unsorted():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client: BaseClient = boto3.client("s3", region_name=region_name)

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
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: DataConnector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.*)\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    # with missing BatchRequest arguments
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list()

    # with empty options
    unsorted_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(
        BatchRequest(
            datasource_name="my_file_path_datasource",
            data_asset_name="my_s3_data_asset",
            options={},
        )
    )
    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "abe_20200809_1040.csv",
                    "name": "abe",
                    "timestamp": "20200809",
                    "price": "1040",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "alex_20200809_1000.csv",
                    "name": "alex",
                    "timestamp": "20200809",
                    "price": "1000",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "alex_20200819_1300.csv",
                    "name": "alex",
                    "timestamp": "20200819",
                    "price": "1300",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "eugene_20200809_1500.csv",
                    "name": "eugene",
                    "timestamp": "20200809",
                    "price": "1500",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "eugene_20201129_1900.csv",
                    "name": "eugene",
                    "timestamp": "20201129",
                    "price": "1900",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "james_20200713_1567.csv",
                    "name": "james",
                    "timestamp": "20200713",
                    "price": "1567",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "james_20200810_1003.csv",
                    "name": "james",
                    "timestamp": "20200810",
                    "price": "1003",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "james_20200811_1009.csv",
                    "name": "james",
                    "timestamp": "20200811",
                    "price": "1009",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "will_20200809_1002.csv",
                    "name": "will",
                    "timestamp": "20200809",
                    "price": "1002",
                }
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict(
                {
                    "path": "will_20200810_1001.csv",
                    "name": "will",
                    "timestamp": "20200810",
                    "price": "1001",
                }
            ),
        ),
    ]
    assert expected == unsorted_batch_definition_list

    # with specified Batch query options
    unsorted_batch_definition_list = my_data_connector.get_batch_definition_list(
        BatchRequest(
            datasource_name="my_file_path_datasource",
            data_asset_name="my_s3_data_asset",
            options={"name": "alex", "timestamp": "20200819", "price": "1300"},
        )
    )
    assert expected[2:3] == unsorted_batch_definition_list


# TODO: <Alex>ALEX-UNCOMMENT_WHEN_SORTERS_ARE_INCLUDED_AND_TEST_SORTED_BATCH_DEFINITION_LIST</Alex>
# TODO: <Alex>ALEX</Alex>
# @pytest.mark.integration
# @mock_s3
# def test_return_all_batch_definitions_sorted():
#     region_name: str = "us-east-1"
#     bucket: str = "test_bucket"
#     conn = boto3.resource("s3", region_name=region_name)
#     conn.create_bucket(Bucket=bucket)
#     client: BaseClient = boto3.client("s3", region_name=region_name)
#
#     test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
#
#     keys: List[str] = [
#         "alex_20200809_1000.csv",
#         "eugene_20200809_1500.csv",
#         "james_20200811_1009.csv",
#         "abe_20200809_1040.csv",
#         "will_20200809_1002.csv",
#         "james_20200713_1567.csv",
#         "eugene_20201129_1900.csv",
#         "will_20200810_1001.csv",
#         "james_20200810_1003.csv",
#         "alex_20200819_1300.csv",
#     ]
#     for key in keys:
#         client.put_object(
#             Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#         )
#
#     my_data_connector: DataConnector = S3DataConnector(
#         datasource_name="my_file_path_datasource",
#         data_asset_name="my_s3_data_asset",
#         batching_regex=re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.*)\.csv"),
#         s3_client=client,
#         bucket=bucket,
#         prefix="",
#         file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
#     )
#     # noinspection PyProtectedMember
#     my_data_connector._get_data_references_cache()
#
#     assert my_data_connector.get_data_reference_count() == 3
#     assert my_data_connector._get_data_reference_list()[:3] == [
#         "alpha-1.csv",
#         "alpha-2.csv",
#         "alpha-3.csv",
#     ]
#     assert my_data_connector.get_unmatched_data_references()[:3] == [
#         "alpha-1.csv",
#         "alpha-2.csv",
#         "alpha-3.csv",
#     ]
#     assert len(my_data_connector.get_unmatched_data_references()) == 3
#
#     sorted_batch_definition_list: List[BatchDefinition] = (
#         my_data_connector.get_batch_definition_list(
#             BatchRequest(
#                 datasource_name="my_file_path_datasource",
#                 data_asset_name="my_s3_data_asset",
#                 options={},
#             )
#         )
#     )
#
#     expected: List[BatchDefinition] = [
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "abe", "timestamp": "20200809", "price": "1040"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "alex", "timestamp": "20200819", "price": "1300"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "alex", "timestamp": "20200809", "price": "1000"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "eugene", "timestamp": "20201129", "price": "1900"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "eugene", "timestamp": "20200809", "price": "1500"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "james", "timestamp": "20200811", "price": "1009"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "james", "timestamp": "20200810", "price": "1003"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "james", "timestamp": "20200713", "price": "1567"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "will", "timestamp": "20200810", "price": "1001"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_s3_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "will", "timestamp": "20200809", "price": "1002"}
#             ),
#         ),
#     ]
#
#     # TEST 1: Sorting works
#     assert expected == sorted_batch_definition_list
#
#     my_batch_request: BatchRequest = BatchRequest(
#         datasource_name="my_file_path_datasource",
#         data_asset_name="my_s3_data_asset",
#         options={
#             "name": "james",
#             "timestamp": "20200713",
#             "price": "1567",
#         },
#     )
#
#     my_batch_definition_list: List[BatchDefinition]
#     my_batch_definition: BatchDefinition
#
#     # TEST 2: Should only return the specified partition
#     my_batch_definition_list = (
#         my_data_connector.get_batch_definition_list(
#             batch_request=my_batch_request
#         )
#     )
#     assert len(my_batch_definition_list) == 1
#     my_batch_definition = my_batch_definition_list[0]
#
#     expected_batch_definition = BatchDefinition(
#         datasource_name="my_file_path_datasource",
#         data_asset_name="my_s3_data_asset",
#         batch_identifiers={
#             "name": "james",
#             "timestamp": "20200713",
#             "price": "1567",
#         },
#     )
#     assert my_batch_definition == expected_batch_definition
#
#     # TEST 3: Without BatchRequest (query) options, should return all 10
#     my_batch_request: BatchRequest = BatchRequest(
#         datasource_name="my_file_path_datasource",
#         data_asset_name="my_s3_data_asset",
#         options={},
#     )
#     # should return 10
#     my_batch_definition_list = (
#         my_data_connector.get_batch_definition_list(
#             batch_request=my_batch_request
#         )
#     )
#     assert len(my_batch_definition_list) == 10
# TODO: <Alex>ALEX</Alex>


@pytest.mark.integration
@mock_s3
def test_return_only_unique_batch_definitions():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client: BaseClient = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "A/file_1.csv",
        "A/file_2.csv",
        "A/file_3.csv",
        "B/file_1.csv",
        "B/file_2.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: DataConnector

    my_data_connector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<name>.+).*\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="A",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "A/file_1.csv",
        "A/file_2.csv",
        "A/file_3.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "A/file_1.csv",
        "A/file_2.csv",
        "A/file_3.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict({"path": "B/file_1.csv", "filename": "file_1"}),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_s3_data_asset",
            batch_identifiers=IDDict({"path": "B/file_2.csv", "filename": "file_2"}),
        ),
    ]

    my_data_connector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<filename>.+).*\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="B",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )

    unsorted_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(
        BatchRequest(
            datasource_name="my_file_path_datasource",
            data_asset_name="my_s3_data_asset",
            options={},
        )
    )
    assert expected == unsorted_batch_definition_list


@pytest.mark.integration
@mock_s3
def test_alpha():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client: BaseClient = boto3.client("s3", region_name=region_name)

    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    keys: List[str] = [
        "test_dir_alpha/A.csv",
        "test_dir_alpha/B.csv",
        "test_dir_alpha/C.csv",
        "test_dir_alpha/D.csv",
    ]
    for key in keys:
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: DataConnector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.*)\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="test_dir_alpha",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_data_references()[:3] == [
        "test_dir_alpha/A.csv",
        "test_dir_alpha/B.csv",
        "test_dir_alpha/C.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 4
    assert my_data_connector.get_matched_data_references()[:3] == [
        "test_dir_alpha/A.csv",
        "test_dir_alpha/B.csv",
        "test_dir_alpha/C.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition

    my_batch_request: BatchRequest

    # Try to fetch a batch from a nonexistent asset
    my_batch_request = BatchRequest(
        datasource_name="BASE", data_asset_name="A", options={}
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 0

    my_batch_request = BatchRequest(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        options={"part_1": "B"},
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 1


@pytest.mark.integration
@mock_s3
def test_foxtrot():
    region_name: str = "us-east-1"
    bucket: str = "test_bucket"
    conn = boto3.resource("s3", region_name=region_name)
    conn.create_bucket(Bucket=bucket)
    client: BaseClient = boto3.client("s3", region_name=region_name)

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
        client.put_object(
            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
        )

    my_data_connector: DataConnector

    my_data_connector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 0
    assert my_data_connector.get_data_references()[:3] == []
    assert my_data_connector.get_matched_data_reference_count() == 0
    assert my_data_connector.get_matched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_data_connector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="test_dir_foxtrot/A",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "test_dir_foxtrot/A/A-1.csv",
        "test_dir_foxtrot/A/A-2.csv",
        "test_dir_foxtrot/A/A-3.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "test_dir_foxtrot/A/A-1.csv",
        "test_dir_foxtrot/A/A-2.csv",
        "test_dir_foxtrot/A/A-3.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_data_connector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.txt"),
        s3_client=client,
        bucket=bucket,
        prefix="test_dir_foxtrot/B",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "test_dir_foxtrot/B/B-1.txt",
        "test_dir_foxtrot/B/B-2.txt",
        "test_dir_foxtrot/B/B-3.txt",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "test_dir_foxtrot/B/B-1.txt",
        "test_dir_foxtrot/B/B-2.txt",
        "test_dir_foxtrot/B/B-3.txt",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_data_connector = S3DataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.csv"),
        s3_client=client,
        bucket=bucket,
        prefix="test_dir_foxtrot/C",
        file_path_template_map_fn=S3Url.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "test_dir_foxtrot/C/C-2017.csv",
        "test_dir_foxtrot/C/C-2018.csv",
        "test_dir_foxtrot/C/C-2019.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "test_dir_foxtrot/C/C-2017.csv",
        "test_dir_foxtrot/C/C-2018.csv",
        "test_dir_foxtrot/C/C-2019.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_batch_request = BatchRequest(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_s3_data_asset",
        options={},
    )
    my_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(batch_request=my_batch_request)
    assert len(my_batch_definition_list) == 3


# TODO: <Alex>ALEX-UNCOMMENT_WHEN_SORTERS_ARE_INCLUDED_AND_TEST_SORTED_BATCH_DEFINITION_LIST</Alex>
# TODO: <Alex>ALEX</Alex>
# @pytest.mark.integration
# @mock_s3
# def test_return_all_batch_definitions_sorted_sorter_named_that_does_not_match_group(
#     tmp_path_factory,
# ):
#     region_name: str = "us-east-1"
#     bucket: str = "test_bucket"
#     conn = boto3.resource("s3", region_name=region_name)
#     conn.create_bucket(Bucket=bucket)
#     client: BaseClient = boto3.client("s3", region_name=region_name)
#
#     test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
#
#     keys: List[str] = [
#         "alex_20200809_1000.csv",
#         "eugene_20200809_1500.csv",
#         "james_20200811_1009.csv",
#         "abe_20200809_1040.csv",
#         "will_20200809_1002.csv",
#         "james_20200713_1567.csv",
#         "eugene_20201129_1900.csv",
#         "will_20200810_1001.csv",
#         "james_20200810_1003.csv",
#         "alex_20200819_1300.csv",
#     ]
#     for key in keys:
#         client.put_object(
#             Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#         )
#
#     my_data_connector_yaml = yaml.load(
#         f"""
#         class_name: S3DataConnector
#         datasource_name: test_environment
#         base_directory: {base_directory}
#         glob_directive: "*.csv"
#         assets:
#             my_s3_data_asset:
#                 pattern: (.+)_(.+)_(.+)\\.csv
#                 group_names:
#                     - name
#                     - timestamp
#                     - price
#         default_regex:
#             pattern: (.+)_.+_.+\\.csv
#             group_names:
#                 - name
#         sorters:
#             - orderby: asc
#               class_name: LexicographicSorter
#               name: name
#             - datetime_format: "%Y%m%d"
#               orderby: desc
#               class_name: DateTimeSorter
#               name: timestamp
#             - orderby: desc
#               class_name: NumericSorter
#               name: for_me_Me_Me
#     """,
#     )
#     with pytest.raises(gx_exceptions.DataConnectorError):
#         # noinspection PyUnusedLocal
#         my_data_connector: S3DataConnector = (
#             instantiate_class_from_config(
#                 config=my_data_connector_yaml,
#                 runtime_environment={
#                     "name": "fluent",
#                     "execution_engine": PandasExecutionEngine(),
#                 },
#                 config_defaults={
#                     "module_name": "great_expectations.datasource.data_connector"
#                 },
#             )
#         )
# TODO: <Alex>ALEX</Alex>


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
