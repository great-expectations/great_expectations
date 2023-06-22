import pathlib
import re
from typing import TYPE_CHECKING, List

import pydantic
import pytest

from great_expectations.core import IDDict
from great_expectations.core.batch import BatchDefinition
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.data_asset.data_connector import (
    FilesystemDataConnector,
)
from tests.test_utils import create_files_in_directory

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.data_asset.data_connector import (
        DataConnector,
    )


@pytest.mark.integration
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_basic_instantiation(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_basic_instantiation"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    my_data_connector: DataConnector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"alpha-(.*)\.csv"),
        base_directory=pathlib.Path(base_directory),
        glob_directive="*.csv",
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

    # Missing "data_asset_name" argument.
    with pytest.raises(pydantic.ValidationError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list(
            BatchRequest(
                datasource_name="something",
                options={},
            )
        )


@pytest.mark.integration
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_instantiation_batching_regex_does_not_match_paths(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp(
            "test_instantiation_from_a_config_batching_regex_does_not_match_paths"
        )
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    my_data_connector: DataConnector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"beta-(.*)\.csv"),
        base_directory=pathlib.Path(base_directory),
        glob_directive="*.csv",
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
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_return_all_batch_definitions_unsorted(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_return_all_batch_definitions_unsorted")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
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
        ],
    )

    my_data_connector: DataConnector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.+)\.csv"),
        base_directory=pathlib.Path(base_directory),
        glob_directive="*.csv",
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
            data_asset_name="my_filesystem_data_asset",
            options={},
        )
    )
    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
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
            data_asset_name="my_filesystem_data_asset",
            options={"name": "alex", "timestamp": "20200819", "price": "1300"},
        )
    )
    assert expected[2:3] == unsorted_batch_definition_list


# TODO: <Alex>ALEX-UNCOMMENT_WHEN_SORTERS_ARE_INCLUDED_AND_TEST_SORTED_BATCH_DEFINITION_LIST</Alex>
# TODO: <Alex>ALEX</Alex>
# @pytest.mark.integration
# @pytest.mark.slow  # creating small number of`file handles in temporary file system
# def test_return_all_batch_definitions_sorted(tmp_path_factory):
#     base_directory = str(
#         tmp_path_factory.mktemp("test_return_all_batch_definitions_sorted")
#     )
#     create_files_in_directory(
#         directory=base_directory,
#         file_name_list=[
#             "alex_20200809_1000.csv",
#             "eugene_20200809_1500.csv",
#             "james_20200811_1009.csv",
#             "abe_20200809_1040.csv",
#             "will_20200809_1002.csv",
#             "james_20200713_1567.csv",
#             "eugene_20201129_1900.csv",
#             "will_20200810_1001.csv",
#             "james_20200810_1003.csv",
#             "alex_20200819_1300.csv",
#         ],
#     )
#
#     my_data_connector: DataConnector = FilesystemDataConnector(
#         datasource_name="my_file_path_datasource",
#         data_asset_name="my_filesystem_data_asset",
#         batching_regex=re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.+)\.csv"),
#         base_directory=pathlib.Path(base_directory),
#         glob_directive="*.csv",
#     )
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
#                 data_asset_name="my_filesystem_data_asset",
#                 options={},
#             )
#         )
#     )
#
#     expected: List[BatchDefinition] = [
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "abe", "timestamp": "20200809", "price": "1040"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "alex", "timestamp": "20200819", "price": "1300"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "alex", "timestamp": "20200809", "price": "1000"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "eugene", "timestamp": "20201129", "price": "1900"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "eugene", "timestamp": "20200809", "price": "1500"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "james", "timestamp": "20200811", "price": "1009"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "james", "timestamp": "20200810", "price": "1003"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "james", "timestamp": "20200713", "price": "1567"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
#             batch_identifiers=IDDict(
#                 {"name": "will", "timestamp": "20200810", "price": "1001"}
#             ),
#         ),
#         BatchDefinition(
#             datasource_name="my_file_path_datasource",
#             data_connector_name="fluent",
#             data_asset_name="my_filesystem_data_asset",
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
#         data_asset_name="my_filesystem_data_asset",
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
#         data_asset_name="my_filesystem_data_asset",
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
#         data_asset_name="my_filesystem_data_asset",
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
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_return_only_unique_batch_definitions(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_return_only_unique_batch_definitions")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "A/file_1.csv",
            "A/file_2.csv",
            "A/file_3.csv",
            "B/file_1.csv",
            "B/file_2.csv",
        ],
    )

    my_data_connector: DataConnector

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)/.+\.csv"),
        base_directory=pathlib.Path(base_directory),
        # glob_directive="*.csv",  # omitting for purposes of this test
    )
    assert my_data_connector.get_data_reference_count() == 7
    assert my_data_connector.get_data_references()[:3] == [
        "A",
        "A/file_1.csv",
        "A/file_2.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 5
    assert my_data_connector.get_matched_data_references()[:3] == [
        "A/file_1.csv",
        "A/file_2.csv",
        "A/file_3.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == [
        "A",
        "B",
    ]
    assert my_data_connector.get_unmatched_data_reference_count() == 2

    expected: List[BatchDefinition] = [
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "A/file_1.csv", "directory": "A", "filename": "file_1.csv"}
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "A/file_2.csv", "directory": "A", "filename": "file_2.csv"}
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "A/file_3.csv", "directory": "A", "filename": "file_3.csv"}
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "B/file_1.csv", "directory": "B", "filename": "file_1.csv"}
            ),
        ),
        BatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "B/file_2.csv", "directory": "B", "filename": "file_2.csv"}
            ),
        ),
    ]

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<directory>.+)/(?P<filename>.+\.csv)"),
        base_directory=pathlib.Path(base_directory),
        # glob_directive="*.csv",  # omitting for purposes of this test
    )

    unsorted_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(
        BatchRequest(
            datasource_name="my_file_path_datasource",
            data_asset_name="my_filesystem_data_asset",
            options={},
        )
    )
    assert expected == unsorted_batch_definition_list


@pytest.mark.integration
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_alpha(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_alpha"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_alpha/A.csv",
            "test_dir_alpha/B.csv",
            "test_dir_alpha/C.csv",
            "test_dir_alpha/D.csv",
        ],
    )

    my_data_connector: DataConnector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)\.csv"),
        base_directory=pathlib.Path(base_directory) / "test_dir_alpha",
        glob_directive="*.csv",
    )
    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_data_references()[:3] == [
        "A.csv",
        "B.csv",
        "C.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 4
    assert my_data_connector.get_matched_data_references()[:3] == [
        "A.csv",
        "B.csv",
        "C.csv",
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
        data_asset_name="my_filesystem_data_asset",
        options={"part_1": "B"},
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 1


@pytest.mark.integration
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_foxtrot(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_foxtrot"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
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
        ],
    )

    my_data_connector: DataConnector

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.csv"),
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot",
        glob_directive="*.csv",
    )
    assert my_data_connector.get_data_reference_count() == 0
    assert my_data_connector.get_data_references()[:3] == []
    assert my_data_connector.get_matched_data_reference_count() == 0
    assert my_data_connector.get_matched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.csv"),
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot" / "A",
        glob_directive="*.csv",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "A-1.csv",
        "A-2.csv",
        "A-3.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "A-1.csv",
        "A-2.csv",
        "A-3.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.txt"),
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot" / "B",
        glob_directive="*.*",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "B-1.txt",
        "B-2.txt",
        "B-3.txt",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "B-1.txt",
        "B-2.txt",
        "B-3.txt",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0
    assert my_data_connector.get_data_reference_count() == 3

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.csv"),
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot" / "C",
        glob_directive="*",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "C-2017.csv",
        "C-2018.csv",
        "C-2019.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 3
    assert my_data_connector.get_matched_data_references()[:3] == [
        "C-2017.csv",
        "C-2018.csv",
        "C-2019.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_batch_request = BatchRequest(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        options={},
    )
    my_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(batch_request=my_batch_request)
    assert len(my_batch_definition_list) == 3


@pytest.mark.integration
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_relative_base_directory_path(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_relative_asset_base_directory_path")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "test_dir_0/A/B/C/logfile_0.csv",
            "test_dir_0/A/B/C/bigfile_1.csv",
            "test_dir_0/A/filename2.csv",
            "test_dir_0/A/filename3.csv",
        ],
    )

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<part_1>.+)\.csv"),
        base_directory=pathlib.Path(base_directory) / "test_dir_0" / "A",
        glob_directive="*",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "B",
        "filename2.csv",
        "filename3.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 2
    assert my_data_connector.get_matched_data_references()[:3] == [
        "filename2.csv",
        "filename3.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == [
        "B",
    ]
    assert my_data_connector.get_unmatched_data_reference_count() == 1

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        batching_regex=re.compile(r"(?P<name>.+)_(?P<number>.+)\.csv"),
        base_directory=pathlib.Path(base_directory) / "test_dir_0" / "A" / "B" / "C",
        glob_directive="log*.csv",
    )
    assert my_data_connector.get_data_reference_count() == 1
    assert my_data_connector.get_data_references()[:3] == [
        "logfile_0.csv",
    ]
    assert my_data_connector.get_matched_data_reference_count() == 1
    assert my_data_connector.get_matched_data_references()[:3] == [
        "logfile_0.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert (
        my_data_connector._get_full_file_path(path="bigfile_1.csv")
        == f"{base_directory}/test_dir_0/A/B/C/bigfile_1.csv"
    )

    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        options={},
    )
    my_batch_definition_list: List[
        BatchDefinition
    ] = my_data_connector.get_batch_definition_list(batch_request=my_batch_request)
    assert len(my_batch_definition_list) == 1


# TODO: <Alex>ALEX-UNCOMMENT_WHEN_SORTERS_ARE_INCLUDED_AND_TEST_SORTED_BATCH_DEFINITION_LIST</Alex>
# TODO: <Alex>ALEX</Alex>
# def test_return_all_batch_definitions_sorted_sorter_named_that_does_not_match_group(
#     tmp_path_factory,
# ):
#     base_directory = str(
#         tmp_path_factory.mktemp(
#             "test_return_all_batch_definitions_sorted_sorter_named_that_does_not_match_group"
#         )
#     )
#     create_files_in_directory(
#         directory=base_directory,
#         file_name_list=[
#             "alex_20200809_1000.csv",
#             "eugene_20200809_1500.csv",
#             "james_20200811_1009.csv",
#             "abe_20200809_1040.csv",
#             "will_20200809_1002.csv",
#             "james_20200713_1567.csv",
#             "eugene_20201129_1900.csv",
#             "will_20200810_1001.csv",
#             "james_20200810_1003.csv",
#             "alex_20200819_1300.csv",
#         ],
#     )
#     my_data_connector_yaml = yaml.load(
#         f"""
#         class_name: FilesystemDataConnector
#         datasource_name: test_environment
#         base_directory: {base_directory}
#         glob_directive: "*.csv"
#         assets:
#             my_filesystem_data_asset:
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
#         my_data_connector: FilesystemDataConnector = (
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
