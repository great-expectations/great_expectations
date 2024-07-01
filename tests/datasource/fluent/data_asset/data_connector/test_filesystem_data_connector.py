import pathlib
import re
from typing import TYPE_CHECKING, List, Union

import pytest

from great_expectations.compatibility import pydantic
from great_expectations.core import IDDict
from great_expectations.core.batch import LegacyBatchDefinition
from great_expectations.core.partitioners import FileNamePartitionerPath, FileNamePartitionerYearly
from great_expectations.datasource.fluent import BatchRequest
from great_expectations.datasource.fluent.constants import MATCH_ALL_PATTERN
from great_expectations.datasource.fluent.data_connector import (
    FilesystemDataConnector,
)
from tests.test_utils import create_files_in_directory

if TYPE_CHECKING:
    from great_expectations.datasource.fluent.data_connector import (
        DataConnector,
    )


@pytest.mark.filesystem
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


@pytest.mark.filesystem
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_return_all_batch_definitions_unsorted(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_return_all_batch_definitions_unsorted"))
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
    batching_regex = re.compile(r"(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.+)\.csv")
    my_data_connector: DataConnector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        base_directory=pathlib.Path(base_directory),
        glob_directive="*.csv",
    )
    # with missing BatchRequest arguments
    with pytest.raises(TypeError):
        # noinspection PyArgumentList
        my_data_connector.get_batch_definition_list()

    # with empty options
    unsorted_batch_definition_list: List[LegacyBatchDefinition] = (
        my_data_connector.get_batch_definition_list(
            BatchRequest(
                datasource_name="my_file_path_datasource",
                data_asset_name="my_filesystem_data_asset",
                options={},
                partitioner=FileNamePartitionerPath(regex=batching_regex),
            )
        )
    )
    processed_batching_regex = re.compile(
        "(?P<path>(?P<name>.+)_(?P<timestamp>.+)_(?P<price>.+)\\.csv)"
    )
    expected: List[LegacyBatchDefinition] = [
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
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
            batching_regex=processed_batching_regex,
        ),
    ]
    assert expected == unsorted_batch_definition_list

    # with specified Batch query options
    unsorted_batch_definition_list = my_data_connector.get_batch_definition_list(
        BatchRequest(
            datasource_name="my_file_path_datasource",
            data_asset_name="my_filesystem_data_asset",
            options={"name": "alex", "timestamp": "20200819", "price": "1300"},
            partitioner=FileNamePartitionerPath(regex=batching_regex),
        )
    )
    assert expected[2:3] == unsorted_batch_definition_list


@pytest.mark.filesystem
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_return_only_unique_batch_definitions(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_return_only_unique_batch_definitions"))
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
        base_directory=pathlib.Path(base_directory),
    )

    assert my_data_connector.get_data_reference_count() == 7
    assert my_data_connector.get_data_references()[:3] == [
        "A",
        "A/file_1.csv",
        "A/file_2.csv",
    ]

    batching_regex = re.compile(r"(?P<name>.+)/.+\.csv")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert len(matched_data_references) == 5
    assert matched_data_references[:3] == [
        "A/file_1.csv",
        "A/file_2.csv",
        "A/file_3.csv",
    ]

    processed_batching_regex = re.compile("(?P<path>(?P<directory>.+)/(?P<filename>.+\\.csv))")
    expected: List[LegacyBatchDefinition] = [
        LegacyBatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "A/file_1.csv", "directory": "A", "filename": "file_1.csv"}
            ),
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "A/file_2.csv", "directory": "A", "filename": "file_2.csv"}
            ),
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "A/file_3.csv", "directory": "A", "filename": "file_3.csv"}
            ),
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "B/file_1.csv", "directory": "B", "filename": "file_1.csv"}
            ),
            batching_regex=processed_batching_regex,
        ),
        LegacyBatchDefinition(
            datasource_name="my_file_path_datasource",
            data_connector_name="fluent",
            data_asset_name="my_filesystem_data_asset",
            batch_identifiers=IDDict(
                {"path": "B/file_2.csv", "directory": "B", "filename": "file_2.csv"}
            ),
            batching_regex=processed_batching_regex,
        ),
    ]

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        base_directory=pathlib.Path(base_directory),
    )

    batching_regex = re.compile(r"(?P<directory>.+)/(?P<filename>.+\.csv)")
    unsorted_batch_definition_list: List[LegacyBatchDefinition] = (
        my_data_connector.get_batch_definition_list(
            BatchRequest(
                datasource_name="my_file_path_datasource",
                data_asset_name="my_filesystem_data_asset",
                options={},
                partitioner=FileNamePartitionerPath(regex=batching_regex),
            )
        )
    )
    assert expected == unsorted_batch_definition_list


@pytest.mark.filesystem
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
        base_directory=pathlib.Path(base_directory) / "test_dir_alpha",
        glob_directive="*.csv",
    )
    assert my_data_connector.get_data_reference_count() == 4
    assert my_data_connector.get_data_references()[:3] == [
        "A.csv",
        "B.csv",
        "C.csv",
    ]

    batching_regex = re.compile(r"(?P<part_1>.+)\.csv")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert len(matched_data_references) == 4
    assert matched_data_references[:3] == [
        "A.csv",
        "B.csv",
        "C.csv",
    ]

    # Try to fetch a batch from a nonexistent asset
    my_batch_request = BatchRequest(
        datasource_name="BASE",
        data_asset_name="A",
        options={},
        partitioner=FileNamePartitionerPath(regex=batching_regex),
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 0

    my_batch_request = BatchRequest(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        options={"part_1": "B"},
        partitioner=FileNamePartitionerPath(regex=batching_regex),
    )
    my_batch_definition_list = my_data_connector.get_batch_definition_list(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 1


@pytest.mark.filesystem
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
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot",
        glob_directive="*.csv",
    )

    batching_regex = re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.csv")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert my_data_connector.get_data_reference_count() == 0
    assert my_data_connector.get_data_references()[:3] == []
    assert len(matched_data_references) == 0

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot" / "A",
        glob_directive="*.csv",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "A-1.csv",
        "A-2.csv",
        "A-3.csv",
    ]

    batching_regex = re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.csv")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert len(matched_data_references) == 3
    assert matched_data_references[:3] == [
        "A-1.csv",
        "A-2.csv",
        "A-3.csv",
    ]
    assert my_data_connector.get_unmatched_data_references()[:3] == []
    assert my_data_connector.get_unmatched_data_reference_count() == 0

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot" / "B",
        glob_directive="*.*",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "B-1.txt",
        "B-2.txt",
        "B-3.txt",
    ]

    batching_regex = re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.txt")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert len(matched_data_references) == 3
    assert matched_data_references[:3] == [
        "B-1.txt",
        "B-2.txt",
        "B-3.txt",
    ]
    assert my_data_connector.get_data_reference_count() == 3

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        base_directory=pathlib.Path(base_directory) / "test_dir_foxtrot" / "C",
        glob_directive="*",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "C-2017.csv",
        "C-2018.csv",
        "C-2019.csv",
    ]

    batching_regex = re.compile(r"(?P<part_1>.+)-(?P<part_2>.+)\.csv")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert len(matched_data_references) == 3
    assert matched_data_references[:3] == [
        "C-2017.csv",
        "C-2018.csv",
        "C-2019.csv",
    ]

    my_batch_request = BatchRequest(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        options={},
        partitioner=FileNamePartitionerPath(regex=batching_regex),
    )
    my_batch_definition_list: List[LegacyBatchDefinition] = (
        my_data_connector.get_batch_definition_list(batch_request=my_batch_request)
    )
    assert len(my_batch_definition_list) == 3


@pytest.mark.filesystem
@pytest.mark.slow  # creating small number of`file handles in temporary file system
def test_relative_base_directory_path(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_relative_asset_base_directory_path"))
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
        base_directory=pathlib.Path(base_directory) / "test_dir_0" / "A",
        glob_directive="*",
    )
    assert my_data_connector.get_data_reference_count() == 3
    assert my_data_connector.get_data_references()[:3] == [
        "B",
        "filename2.csv",
        "filename3.csv",
    ]

    batching_regex = re.compile(r"(?P<part_1>.+)\.csv")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert len(matched_data_references) == 2
    assert matched_data_references[:3] == [
        "filename2.csv",
        "filename3.csv",
    ]

    my_data_connector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        base_directory=pathlib.Path(base_directory) / "test_dir_0" / "A" / "B" / "C",
        glob_directive="log*.csv",
    )
    assert my_data_connector.get_data_reference_count() == 1
    assert my_data_connector.get_data_references()[:3] == [
        "logfile_0.csv",
    ]

    batching_regex = re.compile(r"(?P<name>.+)_(?P<number>.+)\.csv")
    matched_data_references = my_data_connector.get_matched_data_references(regex=batching_regex)
    assert len(matched_data_references) == 1
    assert matched_data_references[:3] == [
        "logfile_0.csv",
    ]
    assert (
        my_data_connector._get_full_file_path(path="bigfile_1.csv")
        == f"{base_directory}/test_dir_0/A/B/C/bigfile_1.csv"
    )

    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        options={},
        partitioner=FileNamePartitionerPath(regex=batching_regex),
    )
    my_batch_definition_list: List[LegacyBatchDefinition] = (
        my_data_connector.get_batch_definition_list(batch_request=my_batch_request)
    )
    assert len(my_batch_definition_list) == 1


@pytest.mark.filesystem
@pytest.mark.parametrize(
    "batching_regex,batch_definition_count",
    [
        pytest.param(MATCH_ALL_PATTERN, 6, id="Match All Pattern"),
        pytest.param(r"alpha-(.*)\.csv", 3, id="Match Alpha CSV with Group"),
        pytest.param(r"single-match\..*", 1, id="Match a single CSV"),
        pytest.param(r"mismatch", 0, id="Match nothing"),
    ],
)
def test_filesystem_data_connector_uses_batching_regex_from_batch_request(
    tmp_path_factory, batching_regex: Union[str, re.Pattern], batch_definition_count
):
    # arrange
    base_directory = str(tmp_path_factory.mktemp("test_basic_instantiation"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
            "dont-match.csv",
            "this-either.csv",
            "single-match.csv",
        ],
    )

    my_data_connector: DataConnector = FilesystemDataConnector(
        datasource_name="my_file_path_datasource",
        data_asset_name="my_filesystem_data_asset",
        base_directory=pathlib.Path(base_directory),
        glob_directive="*.csv",
    )

    # act
    batch_definitions = my_data_connector.get_batch_definition_list(
        BatchRequest(
            datasource_name="my_file_path_datasource",
            data_asset_name="my_filesystem_data_asset",
            options={},
            partitioner=FileNamePartitionerYearly(regex=re.compile(batching_regex)),
        )
    )

    # assert
    assert len(batch_definitions) == batch_definition_count
