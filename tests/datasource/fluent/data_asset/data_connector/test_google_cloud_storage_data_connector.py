import logging
import re
from typing import TYPE_CHECKING, Iterator, List, cast
from unittest import mock

import pytest

from great_expectations.compatibility import google
from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition
from great_expectations.core.util import GCSUrl
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.data_asset.data_connector import (
    GoogleCloudStorageDataConnector,
)

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )


logger = logging.getLogger(__name__)


if not google.storage:
    pytest.skip(
        'Could not import "storage" from google.cloud in configured_asset_gcs_data_connector.py',
        allow_module_level=True,
    )


class MockGCSClient:
    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def list_blobs(
        self,
        bucket_or_name,
        max_results=None,
        prefix=None,
        delimiter=None,
        **kwargs,
    ) -> Iterator:
        return iter([])


@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
def test_basic_instantiation(mock_list_keys):
    mock_list_keys.return_value = [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]

    gcs_client: google.Client = cast(google.Client, MockGCSClient())
    my_data_connector: DataConnector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"alpha-(.*)\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
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
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
def test_instantiation_batching_regex_does_not_match_paths(mock_list_keys):
    mock_list_keys.return_value = [
        "alpha-1.csv",
        "alpha-2.csv",
        "alpha-3.csv",
    ]

    gcs_client: google.Client = cast(google.Client, MockGCSClient())
    my_data_connector: DataConnector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.*)\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
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
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
def test_return_all_batch_definitions_unsorted(mock_list_keys):
    mock_list_keys.return_value = [
        "abe_20200809_1040.csv",
        "alex_20200809_1000.csv",
        "alex_20200819_1300.csv",
        "eugene_20200809_1500.csv",
        "eugene_20201129_1900.csv",
        "james_20200713_1567.csv",
        "james_20200810_1003.csv",
        "james_20200811_1009.csv",
        "will_20200809_1002.csv",
        "will_20200810_1001.csv",
    ]

    gcs_client: google.Client = cast(google.Client, MockGCSClient())
    my_data_connector: DataConnector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.*)\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
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
            data_asset_name="my_google_cloud_storage_data_asset",
            options={},
        )
    )
    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
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
            data_asset_name="my_google_cloud_storage_data_asset",
            options={"name": "alex", "timestamp": "20200819", "price": "1300"},
        )
    )
    assert expected[2:3] == unsorted_batch_definition_list


# TODO: <Alex>ALEX-UNCOMMENT_WHEN_SORTERS_ARE_INCLUDED_AND_TEST_SORTED_BATCH_DEFINITION_LIST</Alex>
# @pytest.mark.integration
# @mock.patch(
#     "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
# )
# def test_return_all_batch_definitions_sorted(
#     mock_list_keys,
#     empty_data_context_stats_enabled,
# ):
#     mock_list_keys.return_value = [
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
#
#     gcs_client: GoogleCloudStorageClient = cast(GoogleCloudStorageClient, MockGCSClient())
#     my_data_connector: DataConnector = GoogleCloudStorageDataConnector(
#         datasource_name="my_file_path_datasource",
#         data_asset_name="my_google_cloud_storage_data_asset",
#         batching_regex=re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.*)\.csv"),
#         gcs_client=gcs_client,
#         bucket_or_name = "my_bucket",
#         prefix = "",
#         file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
#     )
#     # with missing BatchRequest arguments
#     with pytest.raises(TypeError):
#         # noinspection PyArgumentList
#         my_data_connector.get_batch_definition_list()
#
#     # with empty options
#     sorted_batch_definition_list: List[
#         BatchDefinition
#     ] = my_data_connector.get_batch_definition_list(
#         BatchRequest(
#             datasource_name="my_file_path_datasource",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             options={},
#         )
#     )
#     expected: List[BatchDefinition] = [
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "alex_20200809_1000.csv", "name": "alex", "timestamp": "20200809", "price": "1000"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "eugene_20200809_1500.csv", "name": "eugene", "timestamp": "20200809", "price": "1500"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "james_20200811_1009.csv", "name": "james", "timestamp": "20200811", "price": "1009"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "abe_20200809_1040.csv", "name": "abe", "timestamp": "20200809", "price": "1040"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "will_20200809_1002.csv", "name": "will", "timestamp": "20200809", "price": "1002"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "james_20200713_1567.csv", "name": "james", "timestamp": "20200713", "price": "1567"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "eugene_20201129_1900.csv", "name": "eugene", "timestamp": "20201129", "price": "1900"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "will_20200810_1001.csv", "name": "will", "timestamp": "20200810", "price": "1001"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "james_20200810_1003.csv", "name": "james", "timestamp": "20200810", "price": "1003"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             batch_identifiers=IDDict(
#                 {"path": "alex_20200819_1300.csv", "name": "alex", "timestamp": "20200819", "price": "1300"}
#             ),
#         ),
#     ]
#     assert expected == sorted_batch_definition_list
#
#     # with specified Batch query options
#     sorted_batch_definition_list = my_data_connector.get_batch_definition_list(
#         BatchRequest(
#             datasource_name="my_file_path_datasource",
#             data_asset_name="my_google_cloud_storage_data_asset",
#             options={"name": "alex", "timestamp": "20200819", "price": "1300"},
#         )
#     )
#     assert expected[9:10] == sorted_batch_definition_list
# TODO: <Alex>ALEX</Alex>


@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
def test_return_only_unique_batch_definitions(mock_list_keys):
    mock_list_keys.return_value = [
        "A/file_1.csv",
        "A/file_2.csv",
        "A/file_3.csv",
    ]

    gcs_client: google.Client = cast(google.Client, MockGCSClient())

    my_data_connector: DataConnector

    my_data_connector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<name>.+).*\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="A",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
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

    mock_list_keys.return_value = [
        "B/file_1.csv",
        "B/file_2.csv",
    ]

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_google_cloud_storage_data_asset",
            batch_identifiers=IDDict({"path": "B/file_1.csv", "filename": "file_1"}),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_google_cloud_storage_data_asset",
            batch_identifiers=IDDict({"path": "B/file_2.csv", "filename": "file_2"}),
        ),
    ]

    my_data_connector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<filename>.+).*\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="B",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
    )

    unsorted_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(
        BatchRequest(
            datasource_name="my_file_path_datasource",
            data_asset_name="my_google_cloud_storage_data_asset",
            options={},
        )
    )
    assert expected == unsorted_batch_definition_list


@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
def test_alpha(mock_list_keys):
    mock_list_keys.return_value = [
        "test_dir_alpha/A.csv",
        "test_dir_alpha/B.csv",
        "test_dir_alpha/C.csv",
        "test_dir_alpha/D.csv",
    ]

    gcs_client: google.Client = cast(google.Client, MockGCSClient())
    my_data_connector: DataConnector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.*)\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="test_dir_alpha",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
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
        data_asset_name="my_google_cloud_storage_data_asset",
        options={"part_1": "B"},
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 1


@pytest.mark.integration
@mock.patch(
    "great_expectations.datasource.fluent.data_asset.data_connector.google_cloud_storage_data_connector.list_gcs_keys"
)
def test_foxtrot(mock_list_keys):
    mock_list_keys.return_value = []

    gcs_client: google.Client = cast(google.Client, MockGCSClient())

    my_data_connector: DataConnector

    my_data_connector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
    )
    assert my_data_connector.get_data_reference_count() == 0
    assert my_data_connector.get_data_references()[:3] == []
    assert my_data_connector.get_matched_data_reference_count() == 0
    assert my_data_connector.get_matched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    mock_list_keys.return_value = [
        "test_dir_foxtrot/A/A-1.csv",
        "test_dir_foxtrot/A/A-2.csv",
        "test_dir_foxtrot/A/A-3.csv",
    ]

    my_data_connector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="test_dir_foxtrot/A",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
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

    mock_list_keys.return_value = [
        "test_dir_foxtrot/B/B-1.txt",
        "test_dir_foxtrot/B/B-2.txt",
        "test_dir_foxtrot/B/B-3.txt",
    ]

    my_data_connector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.txt"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="test_dir_foxtrot/B",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
    )

    mock_list_keys.return_value = [
        "test_dir_foxtrot/B/B-1.txt",
        "test_dir_foxtrot/B/B-2.txt",
        "test_dir_foxtrot/B/B-3.txt",
    ]

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

    my_data_connector = GoogleCloudStorageDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_google_cloud_storage_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.*)\.csv"),
        gcs_client=gcs_client,
        bucket_or_name="my_bucket",
        prefix="test_dir_foxtrot/C",
        file_path_template_map_fn=GCSUrl.OBJECT_URL_TEMPLATE.format,
    )

    mock_list_keys.return_value = [
        "test_dir_foxtrot/C/C-2017.csv",
        "test_dir_foxtrot/C/C-2018.csv",
        "test_dir_foxtrot/C/C-2019.csv",
    ]

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
        data_asset_name="my_google_cloud_storage_data_asset",
        options={},
    )
    my_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(batch_request=my_batch_request)
    assert len(my_batch_definition_list) == 3


# TODO: <Alex>ALEX-UNCOMMENT_WHEN_SORTERS_ARE_INCLUDED_AND_TEST_SORTED_BATCH_DEFINITION_LIST</Alex>
# TODO: <Alex>ALEX</Alex>
# @mock.patch(
#     "great_expectations.datasource.data_connector.configured_asset_google_cloud_storage_data_connector.GCSClient"
# )
# @mock.patch(
#     "great_expectations.datasource.data_connector.configured_asset_google_cloud_storage_data_connector.list_gcs_keys"
# )
# @mock.patch(
#     "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
# )
# def test_return_all_batch_definitions_raises_error_due_to_sorter_that_does_not_match_group(
#     mock_gcs_conn, mock_list_keys, mock_emit, empty_data_context_stats_enabled
# ):
#     my_data_connector_yaml = yaml.load(
#         """
#        class_name: ConfiguredAssetAzureDataConnector
#        datasource_name: test_environment
#        container: my_container
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
#
#        gcs_options:
#            account_url: my_account_url.blob.core.windows.net
#            credential: my_credential
#    """,
#     )
#
#     mock_list_keys.return_value = [
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
#
#     # Raises error due to a sorter (for_me_Me_me) not matching a group_name in `FilePathDataConnector._validate_sorters_configuration()`
#     with pytest.raises(gx_exceptions.DataConnectorError):
#         instantiate_class_from_config(
#             config=my_data_connector_yaml,
#             runtime_environment={
#                 "name": "general_google_cloud_storage_data_connector",
#                 "execution_engine": PandasExecutionEngine(),
#             },
#             config_defaults={
#                 "module_name": "great_expectations.datasource.data_connector"
#             },
#         )
# TODO: <Alex>ALEX</Alex>
