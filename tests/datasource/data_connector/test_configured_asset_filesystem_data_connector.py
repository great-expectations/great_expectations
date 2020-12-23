import json
import os
from typing import List

import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions.exceptions as ge_exceptions
from great_expectations.core.batch import (
    BatchDefinition,
    BatchRequest,
    PartitionDefinition,
    PartitionRequest,
)
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.datasource.data_connector import (
    ConfiguredAssetFilesystemDataConnector,
)
from tests.test_utils import create_files_in_directory

yaml = YAML()


def test_basic_instantiation(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    my_data_connector = ConfiguredAssetFilesystemDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        base_directory=base_directory,
        assets={"alpha": {}},
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
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
        "example_data_reference": {},
    }

    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 3
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


def test_instantiation_from_a_config(empty_data_context, tmp_path_factory):
    context = empty_data_context

    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    report_object = context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: ConfiguredAssetFilesystemDataConnector
datasource_name: FAKE_DATASOURCE
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}
# glob_directive: "*.csv"

default_regex:
    pattern: alpha-(.*)\\.csv
    group_names:
        - index

assets:
    alpha:
    """,
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
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
        "example_data_reference": {},
    }


def test_instantiation_from_a_config_regex_does_not_match_paths(
    empty_data_context, tmp_path_factory
):
    context = empty_data_context

    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    report_object = context.test_yaml_config(
        f"""
module_name: great_expectations.datasource.data_connector
class_name: ConfiguredAssetFilesystemDataConnector
datasource_name: FAKE_DATASOURCE
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}
# glob_directive: "*.csv"

default_regex:
    pattern: beta-(.*)\\.csv
    group_names:
        - index

assets:
    alpha:

    """,
        return_mode="report_object",
    )

    assert report_object == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {"example_data_references": [], "batch_definition_count": 0},
        },
        "example_unmatched_data_references": [
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
        "unmatched_data_reference_count": 3,
        "example_data_reference": {},
    }


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

    my_data_connector_yaml = yaml.load(
        f"""
            class_name: ConfiguredAssetFilesystemDataConnector
            datasource_name: test_environment
            #execution_engine:
            #    class_name: PandasExecutionEngine
            base_directory: {base_directory}
            glob_directive: "*.csv"
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

    my_data_connector: ConfiguredAssetFilesystemDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "general_filesystem_data_connector",
                "datasource_name": "test_environment",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )

    with pytest.raises(TypeError):
        my_data_connector.get_batch_definition_list_from_batch_request()

    # with unnamed data_asset_name
    unsorted_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name=None,
            )
        )
    )
    expected = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "alex", "timestamp": "20200809", "price": "1000"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "alex", "timestamp": "20200819", "price": "1300"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "james", "timestamp": "20200713", "price": "1567"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "james", "timestamp": "20200810", "price": "1003"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "will", "timestamp": "20200809", "price": "1002"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "will", "timestamp": "20200810", "price": "1001"}
            ),
        ),
    ]
    assert expected == unsorted_batch_definition_list

    # with named data_asset_name
    unsorted_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
            )
        )
    )
    assert expected == unsorted_batch_definition_list


def test_return_all_batch_definitions_sorted(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_return_all_batch_definitions_sorted")
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

    my_data_connector_yaml = yaml.load(
        f"""
        class_name: ConfiguredAssetFilesystemDataConnector
        datasource_name: test_environment
        #execution_engine:
        #    class_name: PandasExecutionEngine
        base_directory: {base_directory}
        glob_directive: "*.csv"
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

    my_data_connector: ConfiguredAssetFilesystemDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "general_filesystem_data_connector",
                "datasource_name": "test_environment",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )

    self_check_report = my_data_connector.self_check()

    assert self_check_report["class_name"] == "ConfiguredAssetFilesystemDataConnector"
    assert self_check_report["data_asset_count"] == 1
    assert self_check_report["data_assets"]["TestFiles"]["batch_definition_count"] == 10
    assert self_check_report["unmatched_data_reference_count"] == 0

    sorted_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            BatchRequest(
                datasource_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
            )
        )
    )

    expected = [
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "abe", "timestamp": "20200809", "price": "1040"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "alex", "timestamp": "20200819", "price": "1300"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "alex", "timestamp": "20200809", "price": "1000"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "eugene", "timestamp": "20201129", "price": "1900"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "eugene", "timestamp": "20200809", "price": "1500"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "james", "timestamp": "20200811", "price": "1009"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "james", "timestamp": "20200810", "price": "1003"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "james", "timestamp": "20200713", "price": "1567"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "will", "timestamp": "20200810", "price": "1001"}
            ),
        ),
        BatchDefinition(
            datasource_name="test_environment",
            data_connector_name="general_filesystem_data_connector",
            data_asset_name="TestFiles",
            partition_definition=PartitionDefinition(
                {"name": "will", "timestamp": "20200809", "price": "1002"}
            ),
        ),
    ]

    # TEST 1: Sorting works
    assert expected == sorted_batch_definition_list

    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        partition_request=PartitionRequest(
            **{
                "partition_identifiers": {
                    "name": "james",
                    "timestamp": "20200713",
                    "price": "1567",
                }
            }
        ),
    )

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition

    # TEST 2: Should only return the specified partition
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )

    assert len(my_batch_definition_list) == 1
    my_batch_definition = my_batch_definition_list[0]
    expected_batch_definition: BatchDefinition = BatchDefinition(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        partition_definition=PartitionDefinition(
            **{
                "name": "james",
                "timestamp": "20200713",
                "price": "1567",
            }
        ),
    )
    assert my_batch_definition == expected_batch_definition

    # TEST 3: Without partition request, should return all 10
    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        partition_request=None,
    )
    # should return 10
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 10


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

    my_data_connector_yaml = yaml.load(
        f"""
                module_name: great_expectations.datasource.data_connector
                class_name: ConfiguredAssetFilesystemDataConnector
                base_directory: {base_directory}/test_dir_alpha
                assets:
                  A:
                    glob_directive: "*.csv"
                default_regex:
                    pattern: (.+)\\.csv
                    group_names:
                    - part_1
            """,
    )

    my_data_connector: ConfiguredAssetFilesystemDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "general_filesystem_data_connector",
                "datasource_name": "BASE",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )
    self_check_report = my_data_connector.self_check()
    print(json.dumps(self_check_report, indent=2))

    assert self_check_report["class_name"] == "ConfiguredAssetFilesystemDataConnector"
    assert self_check_report["data_asset_count"] == 1
    assert set(list(self_check_report["data_assets"].keys())) == {"A"}
    assert self_check_report["unmatched_data_reference_count"] == 0

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition

    # Try to fetch a batch from a nonexistent asset
    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="B",
        partition_request=None,
    )

    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 0

    my_batch_request: BatchRequest = BatchRequest(
        datasource_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="A",
        partition_request=PartitionRequest(
            **{"partition_identifiers": {"part_1": "B"}}
        ),
    )
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 1


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

    my_data_connector_yaml = yaml.load(
        f"""
            module_name: great_expectations.datasource.data_connector
            class_name: ConfiguredAssetFilesystemDataConnector
            base_directory: {base_directory}/test_dir_foxtrot
            assets:
              A:
                base_directory: A/
              B:
                base_directory: B/
                pattern: (.*)-(.*)\\.txt
                group_names:
                - part_1
                - part_2
              C:
                glob_directive: "*"
                base_directory: C/
              D:
                glob_directive: "*"
                base_directory: D/
            default_regex:
                pattern: (.*)-(.*)\\.csv
                group_names:
                - part_1
                - part_2
        """,
    )

    my_data_connector: ConfiguredAssetFilesystemDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "general_filesystem_data_connector",
                "datasource_name": "BASE",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )
    self_check_report = my_data_connector.self_check()
    assert self_check_report == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
        "data_asset_count": 4,
        "example_data_asset_names": ["A", "B", "C"],
        "data_assets": {
            "A": {
                "batch_definition_count": 3,
                "example_data_references": [
                    "A-1.csv",
                    "A-2.csv",
                    "A-3.csv",
                ],
            },
            "B": {
                "batch_definition_count": 3,
                "example_data_references": [
                    "B-1.txt",
                    "B-2.txt",
                    "B-3.txt",
                ],
            },
            "C": {
                "batch_definition_count": 3,
                "example_data_references": [
                    "C-2017.csv",
                    "C-2018.csv",
                    "C-2019.csv",
                ],
            },
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {},
    }
    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition
    my_batch_request = BatchRequest(
        datasource_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="A",
        partition_request=None,
    )
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 3


def test_relative_asset_base_directory_path(tmp_path_factory):
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

    my_data_connector_yaml = yaml.load(
        f"""
            module_name: great_expectations.datasource.data_connector
            class_name: ConfiguredAssetFilesystemDataConnector
            base_directory: {base_directory}/test_dir_0/A
            glob_directive: "*"
            default_regex:
              pattern: (.+)\\.csv
              group_names:
              - name

            assets:
              A:
                base_directory: B/C
                glob_directive: "log*.csv"
                pattern: (.+)_(\\d+)\\.csv
                group_names:
                - name
                - number
        """,
    )

    my_data_connector: ConfiguredAssetFilesystemDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_configured_asset_filesystem_data_connector",
                "datasource_name": "BASE",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )
    my_data_connector.data_context_root_directory = base_directory

    assert (
        my_data_connector._get_full_file_path_for_asset(
            path="bigfile_1.csv", asset=my_data_connector.assets["A"]
        )
        == f"{base_directory}/test_dir_0/A/B/C/bigfile_1.csv"
    )
    self_check_report = my_data_connector.self_check()
    assert self_check_report == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["A"],
        "data_assets": {
            "A": {
                "batch_definition_count": 1,
                "example_data_references": ["logfile_0.csv"],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {},
    }

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition
    my_batch_request = BatchRequest(
        datasource_name="BASE",
        data_connector_name="my_configured_asset_filesystem_data_connector",
        data_asset_name="A",
        partition_request=None,
    )
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 1


def test_relative_default_and_relative_asset_base_directory_paths(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp(
            "test_relative_default_and_relative_asset_base_directory_paths"
        )
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

    my_data_connector_yaml = yaml.load(
        f"""
            module_name: great_expectations.datasource.data_connector
            class_name: ConfiguredAssetFilesystemDataConnector
            base_directory: test_dir_0/A
            glob_directive: "*"
            default_regex:
              pattern: (.+)\\.csv
              group_names:
              - name

            assets:
              A:
                base_directory: B/C
                glob_directive: "log*.csv"
                pattern: (.+)_(\\d+)\\.csv
                group_names:
                - name
                - number
        """,
    )

    my_data_connector: ConfiguredAssetFilesystemDataConnector = (
        instantiate_class_from_config(
            config=my_data_connector_yaml,
            runtime_environment={
                "name": "my_configured_asset_filesystem_data_connector",
                "datasource_name": "BASE",
            },
            config_defaults={
                "module_name": "great_expectations.datasource.data_connector"
            },
        )
    )
    my_data_connector.data_context_root_directory = base_directory

    assert my_data_connector.base_directory == f"{base_directory}/test_dir_0/A"
    assert (
        my_data_connector._get_full_file_path_for_asset(
            path="bigfile_1.csv", asset=my_data_connector.assets["A"]
        )
        == f"{base_directory}/test_dir_0/A/B/C/bigfile_1.csv"
    )
    self_check_report = my_data_connector.self_check()
    assert self_check_report == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": ["A"],
        "data_assets": {
            "A": {
                "batch_definition_count": 1,
                "example_data_references": ["logfile_0.csv"],
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": [],
        "example_data_reference": {},
    }

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition
    my_batch_request = BatchRequest(
        datasource_name="BASE",
        data_connector_name="my_configured_asset_filesystem_data_connector",
        data_asset_name="A",
        partition_request=None,
    )
    my_batch_definition_list = (
        my_data_connector.get_batch_definition_list_from_batch_request(
            batch_request=my_batch_request
        )
    )
    assert len(my_batch_definition_list) == 1


def test_return_all_batch_definitions_sorted_sorter_named_that_does_not_match_group(
    tmp_path_factory,
):
    base_directory = str(
        tmp_path_factory.mktemp(
            "test_return_all_batch_definitions_sorted_sorter_named_that_does_not_match_group"
        )
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
    my_data_connector_yaml = yaml.load(
        f"""
        class_name: ConfiguredAssetFilesystemDataConnector
        datasource_name: test_environment
        #execution_engine:
        #    class_name: PandasExecutionEngine
        base_directory: {base_directory}
        glob_directive: "*.csv"
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
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: ConfiguredAssetFilesystemDataConnector = (
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "general_filesystem_data_connector",
                    "datasource_name": "test_environment",
                },
                config_defaults={
                    "module_name": "great_expectations.datasource.data_connector"
                },
            )
        )


def test_return_all_batch_definitions_too_many_sorters(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_return_all_batch_definitions_too_many_sorters")
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
    my_data_connector_yaml = yaml.load(
        f"""
        class_name: ConfiguredAssetFilesystemDataConnector
        datasource_name: test_environment
        #execution_engine:
        #    class_name: PandasExecutionEngine
        base_directory: {base_directory}
        glob_directive: "*.csv"
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
    with pytest.raises(ge_exceptions.DataConnectorError):
        # noinspection PyUnusedLocal
        my_data_connector: ConfiguredAssetFilesystemDataConnector = (
            instantiate_class_from_config(
                config=my_data_connector_yaml,
                runtime_environment={
                    "name": "general_filesystem_data_connector",
                    "datasource_name": "test_environment",
                },
                config_defaults={
                    "module_name": "great_expectations.datasource.data_connector"
                },
            )
        )


def test_example_with_explicit_data_asset_names(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_example_with_explicit_data_asset_names")
    )
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
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
        ],
    )
    yaml_string = f"""
class_name: ConfiguredAssetFilesystemDataConnector
datasource_name: FAKE_DATASOURCE_NAME
base_directory: {base_directory}/my_base_directory/
default_regex:
    pattern: ^(.+)-(\\d{{4}})(\\d{{2}})\\.(csv|txt)$
    group_names:
        - data_asset_name
        - year_dir
        - month_dir
assets:
    alpha:
        base_directory: {base_directory}/my_base_directory/alpha/files/go/here/
        glob_directive: "*.csv"
    beta:
        base_directory: {base_directory}/my_base_directory/beta_here/
        glob_directive: "*.txt"
    gamma:
        glob_directive: "*.csv"

    """
    config = yaml.load(yaml_string)
    my_data_connector = instantiate_class_from_config(
        config,
        config_defaults={"module_name": "great_expectations.datasource.data_connector"},
        runtime_environment={"name": "my_data_connector"},
    )
    # noinspection PyProtectedMember
    my_data_connector._refresh_data_references_cache()

    assert len(my_data_connector.get_unmatched_data_references()) == 0

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

    assert (
        len(
            my_data_connector.get_batch_definition_list_from_batch_request(
                batch_request=BatchRequest(
                    data_connector_name="my_data_connector",
                    data_asset_name="gamma",
                )
            )
        )
        == 5
    )


def test_basic_instantiation_with_nested_directories(tmp_path_factory):
    base_directory = str(
        tmp_path_factory.mktemp("test_basic_instantiation_with_nested_directories")
    )
    os.makedirs(os.path.join(base_directory, "foo"))
    create_files_in_directory(
        directory=os.path.join(base_directory, "foo"),
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ],
    )

    my_data_connector = ConfiguredAssetFilesystemDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        base_directory=os.path.join(base_directory, "foo"),
        assets={"alpha": {}},
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
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
        "example_data_reference": {},
    }

    my_data_connector = ConfiguredAssetFilesystemDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        base_directory=base_directory,
        assets={"alpha": {"base_directory": "foo"}},
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
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
        "example_data_reference": {},
    }

    my_data_connector = ConfiguredAssetFilesystemDataConnector(
        name="my_data_connector",
        datasource_name="FAKE_DATASOURCE_NAME",
        default_regex={
            "pattern": "foo/alpha-(.*)\\.csv",
            "group_names": ["index"],
        },
        base_directory=base_directory,
        assets={"alpha": {}},
    )

    assert my_data_connector.self_check() == {
        "class_name": "ConfiguredAssetFilesystemDataConnector",
        "data_asset_count": 1,
        "example_data_asset_names": [
            "alpha",
        ],
        "data_assets": {
            "alpha": {
                "example_data_references": [
                    "foo/alpha-1.csv",
                    "foo/alpha-2.csv",
                    "foo/alpha-3.csv",
                ],
                "batch_definition_count": 3,
            },
        },
        "example_unmatched_data_references": ["foo"],
        "unmatched_data_reference_count": 1,
        "example_data_reference": {},
    }
