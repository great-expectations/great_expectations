from unittest import mock

import pytest
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.core.batch import BatchRequest
from great_expectations.datasource.data_connector import (
    ConfiguredAssetAzureDataConnector,
)

yaml = YAML()


# # TODO(cdkini): Move to data_context/types/base.py test file
# def test_schema_validation_raises_invalid_config_error_with_multiple_auth_methods():
#     raise NotImplementedError()


# # TODO(cdkini): Move to data_connector/util.py test file
# def test_list_azure_keys_basic(blob_service_client: BlobServiceClient):
#     keys = list_azure_keys(
#         azure=blob_service_client,
#         query_options={"container": CONTAINER_NAME},
#         recursive=False,
#     )
#     assert keys == [
#         "yellow_trip_data_sample_2018-01.csv",
#         "yellow_trip_data_sample_2018-02.csv",
#         "yellow_trip_data_sample_2018-03.csv",
#     ]


# # TODO(cdkini): Move to data_connector/util.py test file
# def test_list_azure_keys_with_name_starts_with(blob_service_client: BlobServiceClient):
#     keys = list_azure_keys(
#         azure=blob_service_client,
#         query_options={"name_starts_with": "2018/", "container": CONTAINER_NAME},
#         recursive=False,
#     )
#     assert keys == [
#         "2018/yellow_trip_data_sample_2018-01.csv",
#         "2018/yellow_trip_data_sample_2018-02.csv",
#         "2018/yellow_trip_data_sample_2018-03.csv",
#     ]


# # TODO(cdkini): Move to data_connector/util.py test file
# def test_list_azure_keys_with_name_starts_with_and_recursive(
#     blob_service_client: BlobServiceClient,
# ):
#     keys = list_azure_keys(
#         azure=blob_service_client,
#         query_options={"name_starts_with": "2018/", "container": CONTAINER_NAME},
#         recursive=True,
#     )
#     assert keys == [
#         "2018/2018-04/yellow_trip_data_sample_2018-04.csv",
#         "2018/2018-05/yellow_trip_data_sample_2018-05.csv",
#         "2018/2018-06/yellow_trip_data_sample_2018-06.csv",
#         "2018/yellow_trip_data_sample_2018-01.csv",
#         "2018/yellow_trip_data_sample_2018-02.csv",
#         "2018/yellow_trip_data_sample_2018-03.csv",
#     ]


@mock.patch(
    "great_expectations.datasource.data_connector.configured_asset_azure_data_connector.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.configured_asset_azure_data_connector.list_azure_keys",
    return_value=["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
)
def test_instantiation_with_account_url(mock_service_client, mock_list_keys):
    my_data_connector = ConfiguredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        container="my_container",
        name_starts_with="",
        assets={"alpha": {}},
        azure_options={"account_url": "my_account_url", "credential": "my_credential"},
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetAzureDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "alpha-1.csv",
                    "alpha-2.csv",
                    "alpha-3.csv",
                ],
                "batch_definition_count": 3,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []


@mock.patch(
    "great_expectations.datasource.data_connector.configured_asset_azure_data_connector.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.configured_asset_azure_data_connector.list_azure_keys",
    return_value=["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
)
def test_instantiation_with_conn_str(mock_service_client, mock_list_keys):
    my_data_connector = ConfiguredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        container="my_container",
        name_starts_with="",
        assets={"alpha": {}},
        azure_options={
            "conn_str": "DefaultEndpointsProtocol=https;AccountName=storagesample;AccountKey=my_account_key"
        },
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetAzureDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "alpha-1.csv",
                    "alpha-2.csv",
                    "alpha-3.csv",
                ],
                "batch_definition_count": 3,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }

    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 3
    assert my_data_connector.get_unmatched_data_references() == []


def test_get_batch_definition_list_from_batch_request_with_illegal_execution_env_name_raises_error():
    my_data_connector = ConfiguredAssetAzureDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        container="my_container",
        name_starts_with="",
        assets={"alpha": {}},
        azure_options={"account_url": "my_account_url"},
    )

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


@mock.patch(
    "great_expectations.datasource.data_connector.configured_asset_azure_data_connector.BlobServiceClient"
)
@mock.patch(
    "great_expectations.datasource.data_connector.configured_asset_azure_data_connector.list_azure_keys",
    return_value=["alpha-1.csv", "alpha-2.csv", "alpha-3.csv"],
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_instantiation_from_a_config(
    mock_service_client, mock_list_keys, mock_emit, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    report_object = context.test_yaml_config(
        f"""
        module_name: great_expectations.datasource.data_connector
        class_name: ConfiguredAssetAzureDataConnector
        datasource_name: FAKE_DATASOURCE
        name: TEST_DATA_CONNECTOR
        default_regex:
            pattern: alpha-(.*)\\.csv
            group_names:
                - index
        azure_options:
            account_url: my_account_url
            credential: my_credential
        container: my_container
        name_starts_with: ""
        assets:
            alpha:
    """,
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "ConfiguredAssetAzureDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "alpha-1.csv",
                    "alpha-2.csv",
                    "alpha-3.csv",
                ],
                "batch_definition_count": 3,
            },
        },
        "example_unmatched_data_references": [],
        "unmatched_data_reference_count": 0,
    }
    assert mock_emit.call_count == 1

    # FIXME(cdkini): Currently broken due to indexing errors?
    # anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
    #     "anonymized_name"
    # ]
    # expected_call_args_list = [
    #     mock.call(
    #         {
    #             "event": "data_context.test_yaml_config",
    #             "event_payload": {
    #                 "anonymized_name": anonymized_name,
    #                 "parent_class": "ConfiguredAssetAzureDataConnector",
    #             },
    #             "success": True,
    #         }
    #     ),
    # ]
    # assert mock_emit.call_args_list == expected_call_args_list


# @mock.patch(
#    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
# )
# @mock_s3
# def test_instantiation_from_a_config_regex_does_not_match_paths(
#    mock_emit, empty_data_context_stats_enabled
# ):
#    context: DataContext = empty_data_context_stats_enabled

#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "alpha-1.csv",
#        "alpha-2.csv",
#        "alpha-3.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    report_object = context.test_yaml_config(
#        f"""
# module_name: great_expectations.datasource.data_connector
# class_name: ConfiguredAssetS3DataConnector
# datasource_name: FAKE_DATASOURCE
# name: TEST_DATA_CONNECTOR

# bucket: {bucket}
# prefix: ""

# default_regex:
#    pattern: beta-(.*)\\.csv
#    group_names:
#        - index

# assets:
#    alpha:

#    """,
#        return_mode="report_object",
#    )

#    assert report_object == {
#        "class_name": "ConfiguredAssetS3DataConnector",
#        "data_asset_count": 1,
#        "example_data_asset_names": [
#            "alpha",
#        ],
#        "data_assets": {
#            "alpha": {"example_data_references": [], "batch_definition_count": 0},
#        },
#        "example_unmatched_data_references": [
#            "alpha-1.csv",
#            "alpha-2.csv",
#            "alpha-3.csv",
#        ],
#        "unmatched_data_reference_count": 3,
#        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
#        # "example_data_reference": {},
#    }
#    assert mock_emit.call_count == 1
#    anonymized_name = mock_emit.call_args_list[0][0][0]["event_payload"][
#        "anonymized_name"
#    ]
#    expected_call_args_list = [
#        mock.call(
#            {
#                "event": "data_context.test_yaml_config",
#                "event_payload": {
#                    "anonymized_name": anonymized_name,
#                    "parent_class": "ConfiguredAssetS3DataConnector",
#                },
#                "success": True,
#            }
#        ),
#    ]
#    assert mock_emit.call_args_list == expected_call_args_list


# @mock_s3
# def test_return_all_batch_definitions_unsorted():
#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "alex_20200809_1000.csv",
#        "eugene_20200809_1500.csv",
#        "james_20200811_1009.csv",
#        "abe_20200809_1040.csv",
#        "will_20200809_1002.csv",
#        "james_20200713_1567.csv",
#        "eugene_20201129_1900.csv",
#        "will_20200810_1001.csv",
#        "james_20200810_1003.csv",
#        "alex_20200819_1300.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    my_data_connector_yaml = yaml.load(
#        f"""
#            class_name: ConfiguredAssetS3DataConnector
#            datasource_name: test_environment
#            #execution_engine:
#            #    class_name: PandasExecutionEngine
#            bucket: {bucket}
#            prefix: ""
#            assets:
#                TestFiles:
#            default_regex:
#                pattern: (.+)_(.+)_(.+)\\.csv
#                group_names:
#                    - name
#                    - timestamp
#                    - price
#        """,
#    )

#    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
#        config=my_data_connector_yaml,
#        runtime_environment={
#            "name": "general_s3_data_connector",
#            "datasource_name": "test_environment",
#        },
#        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
#    )

#    with pytest.raises(TypeError):
#        my_data_connector.get_batch_definition_list_from_batch_request()

#    # with unnamed data_asset_name
#    with pytest.raises(TypeError):
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            BatchRequest(
#                datasource_name="test_environment",
#                data_connector_name="general_s3_data_connector",
#                data_asset_name=None,
#            )
#        )

#    # with unnamed data_asset_name
#    unsorted_batch_definition_list = (
#        my_data_connector._get_batch_definition_list_from_batch_request(
#            BatchRequestBase(
#                datasource_name="test_environment",
#                data_connector_name="general_s3_data_connector",
#                data_asset_name=None,
#            )
#        )
#    )
#    expected = [
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "abe", "timestamp": "20200809", "price": "1040"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "alex", "timestamp": "20200809", "price": "1000"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "alex", "timestamp": "20200819", "price": "1300"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "james", "timestamp": "20200713", "price": "1567"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "james", "timestamp": "20200810", "price": "1003"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "james", "timestamp": "20200811", "price": "1009"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "will", "timestamp": "20200809", "price": "1002"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "will", "timestamp": "20200810", "price": "1001"}
#            ),
#        ),
#    ]
#    assert expected == unsorted_batch_definition_list

#    # with named data_asset_name
#    unsorted_batch_definition_list = (
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            BatchRequest(
#                datasource_name="test_environment",
#                data_connector_name="general_s3_data_connector",
#                data_asset_name="TestFiles",
#            )
#        )
#    )
#    assert expected == unsorted_batch_definition_list


# @mock_s3
# def test_return_all_batch_definitions_sorted():
#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "alex_20200809_1000.csv",
#        "eugene_20200809_1500.csv",
#        "james_20200811_1009.csv",
#        "abe_20200809_1040.csv",
#        "will_20200809_1002.csv",
#        "james_20200713_1567.csv",
#        "eugene_20201129_1900.csv",
#        "will_20200810_1001.csv",
#        "james_20200810_1003.csv",
#        "alex_20200819_1300.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    my_data_connector_yaml = yaml.load(
#        f"""
#        class_name: ConfiguredAssetS3DataConnector
#        datasource_name: test_environment
#        #execution_engine:
#        #    class_name: PandasExecutionEngine
#        bucket: {bucket}
#        prefix: ""
#        assets:
#            TestFiles:
#        default_regex:
#            pattern: (.+)_(.+)_(.+)\\.csv
#            group_names:
#                - name
#                - timestamp
#                - price
#        sorters:
#            - orderby: asc
#              class_name: LexicographicSorter
#              name: name
#            - datetime_format: "%Y%m%d"
#              orderby: desc
#              class_name: DateTimeSorter
#              name: timestamp
#            - orderby: desc
#              class_name: NumericSorter
#              name: price

#    """,
#    )

#    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
#        config=my_data_connector_yaml,
#        runtime_environment={
#            "name": "general_s3_data_connector",
#            "datasource_name": "test_environment",
#        },
#        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
#    )

#    self_check_report = my_data_connector.self_check()

#    assert self_check_report["class_name"] == "ConfiguredAssetS3DataConnector"
#    assert self_check_report["data_asset_count"] == 1
#    assert self_check_report["data_assets"]["TestFiles"]["batch_definition_count"] == 10
#    assert self_check_report["unmatched_data_reference_count"] == 0

#    sorted_batch_definition_list = (
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            BatchRequest(
#                datasource_name="test_environment",
#                data_connector_name="general_s3_data_connector",
#                data_asset_name="TestFiles",
#            )
#        )
#    )

#    expected = [
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "abe", "timestamp": "20200809", "price": "1040"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "alex", "timestamp": "20200819", "price": "1300"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "alex", "timestamp": "20200809", "price": "1000"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "james", "timestamp": "20200811", "price": "1009"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "james", "timestamp": "20200810", "price": "1003"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "james", "timestamp": "20200713", "price": "1567"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "will", "timestamp": "20200810", "price": "1001"}
#            ),
#        ),
#        BatchDefinition(
#            datasource_name="test_environment",
#            data_connector_name="general_s3_data_connector",
#            data_asset_name="TestFiles",
#            batch_identifiers=IDDict(
#                {"name": "will", "timestamp": "20200809", "price": "1002"}
#            ),
#        ),
#    ]

#    # TEST 1: Sorting works
#    assert expected == sorted_batch_definition_list

#    my_batch_request: BatchRequest = BatchRequest(
#        datasource_name="test_environment",
#        data_connector_name="general_s3_data_connector",
#        data_asset_name="TestFiles",
#        data_connector_query=IDDict(
#            **{
#                "batch_filter_parameters": {
#                    "name": "james",
#                    "timestamp": "20200713",
#                    "price": "1567",
#                }
#            }
#        ),
#    )

#    my_batch_definition_list: List[BatchDefinition]
#    my_batch_definition: BatchDefinition

#    # TEST 2: Should only return the specified partition
#    my_batch_definition_list = (
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            batch_request=my_batch_request
#        )
#    )

#    assert len(my_batch_definition_list) == 1
#    my_batch_definition = my_batch_definition_list[0]
#    expected_batch_definition: BatchDefinition = BatchDefinition(
#        datasource_name="test_environment",
#        data_connector_name="general_s3_data_connector",
#        data_asset_name="TestFiles",
#        batch_identifiers=IDDict(
#            **{
#                "name": "james",
#                "timestamp": "20200713",
#                "price": "1567",
#            }
#        ),
#    )
#    assert my_batch_definition == expected_batch_definition

#    # TEST 3: Without data_connector_query, should return all 10
#    my_batch_request: BatchRequest = BatchRequest(
#        datasource_name="test_environment",
#        data_connector_name="general_s3_data_connector",
#        data_asset_name="TestFiles",
#        data_connector_query=None,
#    )
#    # should return 10
#    my_batch_definition_list = (
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            batch_request=my_batch_request
#        )
#    )
#    assert len(my_batch_definition_list) == 10


# @mock_s3
# def test_alpha():
#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "test_dir_alpha/A.csv",
#        "test_dir_alpha/B.csv",
#        "test_dir_alpha/C.csv",
#        "test_dir_alpha/D.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    my_data_connector_yaml = yaml.load(
#        f"""
#                module_name: great_expectations.datasource.data_connector
#                class_name: ConfiguredAssetS3DataConnector
#                bucket: {bucket}
#                prefix: test_dir_alpha
#                assets:
#                  A:
#                default_regex:
#                    pattern: .*(.+)\\.csv
#                    group_names:
#                    - part_1
#            """,
#    )

#    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
#        config=my_data_connector_yaml,
#        runtime_environment={
#            "name": "general_s3_data_connector",
#            "datasource_name": "BASE",
#        },
#        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
#    )
#    self_check_report = my_data_connector.self_check()
#    print(json.dumps(self_check_report, indent=2))

#    assert self_check_report["class_name"] == "ConfiguredAssetS3DataConnector"
#    assert self_check_report["data_asset_count"] == 1
#    assert set(list(self_check_report["data_assets"].keys())) == {"A"}
#    assert self_check_report["unmatched_data_reference_count"] == 0

#    my_batch_definition_list: List[BatchDefinition]
#    my_batch_definition: BatchDefinition

#    # Try to fetch a batch from a nonexistent asset
#    my_batch_request: BatchRequest = BatchRequest(
#        datasource_name="BASE",
#        data_connector_name="general_s3_data_connector",
#        data_asset_name="B",
#        data_connector_query=None,
#    )

#    my_batch_definition_list = (
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            batch_request=my_batch_request
#        )
#    )
#    assert len(my_batch_definition_list) == 0

#    my_batch_request: BatchRequest = BatchRequest(
#        datasource_name="BASE",
#        data_connector_name="general_s3_data_connector",
#        data_asset_name="A",
#        data_connector_query=IDDict(**{"batch_filter_parameters": {"part_1": "B"}}),
#    )
#    my_batch_definition_list = (
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            batch_request=my_batch_request
#        )
#    )
#    assert len(my_batch_definition_list) == 1


# @mock_s3
# def test_foxtrot():
#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "test_dir_foxtrot/A/A-1.csv",
#        "test_dir_foxtrot/A/A-2.csv",
#        "test_dir_foxtrot/A/A-3.csv",
#        "test_dir_foxtrot/B/B-1.txt",
#        "test_dir_foxtrot/B/B-2.txt",
#        "test_dir_foxtrot/B/B-3.txt",
#        "test_dir_foxtrot/C/C-2017.csv",
#        "test_dir_foxtrot/C/C-2018.csv",
#        "test_dir_foxtrot/C/C-2019.csv",
#        "test_dir_foxtrot/D/D-aaa.csv",
#        "test_dir_foxtrot/D/D-bbb.csv",
#        "test_dir_foxtrot/D/D-ccc.csv",
#        "test_dir_foxtrot/D/D-ddd.csv",
#        "test_dir_foxtrot/D/D-eee.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    my_data_connector_yaml = yaml.load(
#        f"""
#            module_name: great_expectations.datasource.data_connector
#            class_name: ConfiguredAssetS3DataConnector
#            bucket: {bucket}
#            prefix: test_dir_foxtrot
#            assets:
#              A:
#                prefix: test_dir_foxtrot/A/
#              B:
#                prefix: test_dir_foxtrot/B/
#                pattern: (.+)-(.+)\\.txt
#                group_names:
#                - part_1
#                - part_2
#              C:
#                prefix: test_dir_foxtrot/C/
#              D:
#                prefix: test_dir_foxtrot/D/
#            default_regex:
#                pattern: (.+)-(.+)\\.csv
#                group_names:
#                - part_1
#                - part_2
#        """,
#    )

#    my_data_connector: ConfiguredAssetS3DataConnector = instantiate_class_from_config(
#        config=my_data_connector_yaml,
#        runtime_environment={
#            "name": "general_s3_data_connector",
#            "datasource_name": "BASE",
#        },
#        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
#    )
#    self_check_report = my_data_connector.self_check()
#    assert self_check_report == {
#        "class_name": "ConfiguredAssetS3DataConnector",
#        "data_asset_count": 4,
#        "example_data_asset_names": ["A", "B", "C"],
#        "data_assets": {
#            "A": {
#                "batch_definition_count": 3,
#                "example_data_references": [
#                    "test_dir_foxtrot/A/A-1.csv",
#                    "test_dir_foxtrot/A/A-2.csv",
#                    "test_dir_foxtrot/A/A-3.csv",
#                ],
#            },
#            "B": {
#                "batch_definition_count": 3,
#                "example_data_references": [
#                    "test_dir_foxtrot/B/B-1.txt",
#                    "test_dir_foxtrot/B/B-2.txt",
#                    "test_dir_foxtrot/B/B-3.txt",
#                ],
#            },
#            "C": {
#                "batch_definition_count": 3,
#                "example_data_references": [
#                    "test_dir_foxtrot/C/C-2017.csv",
#                    "test_dir_foxtrot/C/C-2018.csv",
#                    "test_dir_foxtrot/C/C-2019.csv",
#                ],
#            },
#        },
#        "unmatched_data_reference_count": 0,
#        "example_unmatched_data_references": [],
#        # FIXME: (Sam) example_data_reference removed temporarily in PR #2590:
#        # "example_data_reference": {},
#    }
#    my_batch_definition_list: List[BatchDefinition]
#    my_batch_definition: BatchDefinition
#    my_batch_request = BatchRequest(
#        datasource_name="BASE",
#        data_connector_name="general_s3_data_connector",
#        data_asset_name="A",
#        data_connector_query=None,
#    )
#    my_batch_definition_list = (
#        my_data_connector.get_batch_definition_list_from_batch_request(
#            batch_request=my_batch_request
#        )
#    )
#    assert len(my_batch_definition_list) == 3


# @mock_s3
# def test_return_all_batch_definitions_sorted_sorter_named_that_does_not_match_group():
#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "alex_20200809_1000.csv",
#        "eugene_20200809_1500.csv",
#        "james_20200811_1009.csv",
#        "abe_20200809_1040.csv",
#        "will_20200809_1002.csv",
#        "james_20200713_1567.csv",
#        "eugene_20201129_1900.csv",
#        "will_20200810_1001.csv",
#        "james_20200810_1003.csv",
#        "alex_20200819_1300.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    my_data_connector_yaml = yaml.load(
#        f"""
#        class_name: ConfiguredAssetS3DataConnector
#        datasource_name: test_environment
#        #execution_engine:
#        #    class_name: PandasExecutionEngine
#        bucket: bucket
#        assets:
#            TestFiles:
#                pattern: (.+)_(.+)_(.+)\\.csv
#                group_names:
#                    - name
#                    - timestamp
#                    - price
#        default_regex:
#            pattern: (.+)_.+_.+\\.csv
#            group_names:
#                - name
#        sorters:
#            - orderby: asc
#              class_name: LexicographicSorter
#              name: name
#            - datetime_format: "%Y%m%d"
#              orderby: desc
#              class_name: DateTimeSorter
#              name: timestamp
#            - orderby: desc
#              class_name: NumericSorter
#              name: for_me_Me_Me
#    """,
#    )
#    with pytest.raises(ge_exceptions.DataConnectorError):
#        # noinspection PyUnusedLocal
#        my_data_connector: ConfiguredAssetS3DataConnector = (
#            instantiate_class_from_config(
#                config=my_data_connector_yaml,
#                runtime_environment={
#                    "name": "general_s3_data_connector",
#                    "datasource_name": "test_environment",
#                },
#                config_defaults={
#                    "module_name": "great_expectations.datasource.data_connector"
#                },
#            )
#        )


# @mock_s3
# def test_return_all_batch_definitions_too_many_sorters():
#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "alex_20200809_1000.csv",
#        "eugene_20200809_1500.csv",
#        "james_20200811_1009.csv",
#        "abe_20200809_1040.csv",
#        "will_20200809_1002.csv",
#        "james_20200713_1567.csv",
#        "eugene_20201129_1900.csv",
#        "will_20200810_1001.csv",
#        "james_20200810_1003.csv",
#        "alex_20200819_1300.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    my_data_connector_yaml = yaml.load(
#        f"""
#        class_name: ConfiguredAssetS3DataConnector
#        datasource_name: test_environment
#        #execution_engine:
#        #    class_name: PandasExecutionEngine
#        bucket: {bucket}
#        prefix: ""
#        assets:
#            TestFiles:
#        default_regex:
#            pattern: (.+)_.+_.+\\.csv
#            group_names:
#                - name
#        sorters:
#            - orderby: asc
#              class_name: LexicographicSorter
#              name: name
#            - datetime_format: "%Y%m%d"
#              orderby: desc
#              class_name: DateTimeSorter
#              name: timestamp
#            - orderby: desc
#              class_name: NumericSorter
#              name: price

#    """,
#    )
#    with pytest.raises(ge_exceptions.DataConnectorError):
#        # noinspection PyUnusedLocal
#        my_data_connector: ConfiguredAssetS3DataConnector = (
#            instantiate_class_from_config(
#                config=my_data_connector_yaml,
#                runtime_environment={
#                    "name": "general_s3_data_connector",
#                    "datasource_name": "test_environment",
#                },
#                config_defaults={
#                    "module_name": "great_expectations.datasource.data_connector"
#                },
#            )
#        )


# @mock_s3
# def test_example_with_explicit_data_asset_names():
#    region_name: str = "us-east-1"
#    bucket: str = "test_bucket"
#    conn = boto3.resource("s3", region_name=region_name)
#    conn.create_bucket(Bucket=bucket)
#    client = boto3.client("s3", region_name=region_name)

#    test_df: pd.DataFrame = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

#    keys: List[str] = [
#        "my_base_directory/alpha/files/go/here/alpha-202001.csv",
#        "my_base_directory/alpha/files/go/here/alpha-202002.csv",
#        "my_base_directory/alpha/files/go/here/alpha-202003.csv",
#        "my_base_directory/beta_here/beta-202001.txt",
#        "my_base_directory/beta_here/beta-202002.txt",
#        "my_base_directory/beta_here/beta-202003.txt",
#        "my_base_directory/beta_here/beta-202004.txt",
#        "my_base_directory/gamma-202001.csv",
#        "my_base_directory/gamma-202002.csv",
#        "my_base_directory/gamma-202003.csv",
#        "my_base_directory/gamma-202004.csv",
#        "my_base_directory/gamma-202005.csv",
#    ]
#    for key in keys:
#        client.put_object(
#            Bucket=bucket, Body=test_df.to_csv(index=False).encode("utf-8"), Key=key
#        )

#    yaml_string = f"""
# class_name: ConfiguredAssetS3DataConnector
# datasource_name: FAKE_DATASOURCE_NAME
# bucket: {bucket}
# prefix: my_base_directory/
# default_regex:
#    pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
#    group_names:
#        - data_asset_name
#        - year_dir
#        - month_dir
# assets:
#    alpha:
#        prefix: my_base_directory/alpha/files/go/here/
#        pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.csv$
#    beta:
#        prefix: my_base_directory/beta_here/
#        pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.txt$
#    gamma:
#        pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.csv$

#    """
#    config = yaml.load(yaml_string)
#    my_data_connector = instantiate_class_from_config(
#        config,
#        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
#        runtime_environment={"name": "my_data_connector"},
#    )
#    # noinspection PyProtectedMember
#    my_data_connector._refresh_data_references_cache()

#    assert len(my_data_connector.get_unmatched_data_references()) == 0

#    assert (
#        len(
#            my_data_connector.get_batch_definition_list_from_batch_request(
#                batch_request=BatchRequest(
#                    datasource_name="FAKE_DATASOURCE_NAME",
#                    data_connector_name="my_data_connector",
#                    data_asset_name="alpha",
#                )
#            )
#        )
#        == 3
#    )

#    assert (
#        len(
#            my_data_connector.get_batch_definition_list_from_batch_request(
#                batch_request=BatchRequest(
#                    datasource_name="FAKE_DATASOURCE_NAME",
#                    data_connector_name="my_data_connector",
#                    data_asset_name="beta",
#                )
#            )
#        )
#        == 4
#    )

#    assert (
#        len(
#            my_data_connector.get_batch_definition_list_from_batch_request(
#                batch_request=BatchRequest(
#                    datasource_name="FAKE_DATASOURCE_NAME",
#                    data_connector_name="my_data_connector",
#                    data_asset_name="gamma",
#                )
#            )
#        )
#        == 5
#    )
