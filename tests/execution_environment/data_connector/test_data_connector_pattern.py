import pytest
import yaml
import json

from typing import List

from great_expectations.execution_environment.data_connector import DataConnector

from great_expectations.execution_environment.data_connector.sorter import (
    Sorter,
    LexicographicSorter,
    DateTimeSorter,
    NumericSorter,
)

from great_expectations.data_context.util import (
    instantiate_class_from_config,
)
from tests.test_utils import (
    create_files_in_directory,
)
from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
    PartitionDefinition,
)
from great_expectations.execution_environment.data_connector.util import (
    batch_definition_matches_batch_request,
    map_data_reference_string_to_batch_definition_list_using_regex,
    convert_data_reference_string_to_batch_request_using_regex,
    map_batch_definition_to_data_reference_string_using_regex,
    convert_batch_request_to_data_reference_string_using_regex
)
from great_expectations.data_context.util import instantiate_class_from_config


def test_return_all_batch_definitions_unsorted(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
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
        ]
    )

    my_data_connector_yaml = yaml.load(f"""
            class_name: FilesDataConnector
            execution_environment_name: test_environment
            execution_engine:
                BASE_ENGINE:
                class_name: PandasExecutionEngine
            class_name: FilesDataConnector
            base_directory: {base_directory}
            glob_directive: '*.csv'
            assets:
                TestFiles:
                    partitioner_name: default_partitioner_name
            default_regex:
                pattern: (.+)_(.+)_(.+)\\.csv
                group_names:
                    - name
                    - timestamp
                    - price
        """, Loader=yaml.FullLoader)

    my_data_connector: DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_filesystem_data_connector",
            "execution_environment_name": "test_environment",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    unsorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
    ))

    print(unsorted_batch_definition_list)
    expected = [
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'james', 'timestamp': '20200810', 'price': '1003'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'abe', 'timestamp': '20200809', 'price': '1040'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'eugene', 'timestamp': '20200809', 'price': '1500'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'alex', 'timestamp': '20200819', 'price': '1300'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'alex', 'timestamp': '20200809', 'price': '1000'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'will', 'timestamp': '20200810', 'price': '1001'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'eugene', 'timestamp': '20201129', 'price': '1900'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'will', 'timestamp': '20200809', 'price': '1002'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'james', 'timestamp': '20200811', 'price': '1009'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'james', 'timestamp': '20200713', 'price': '1567'}
                        )),
    ]
    assert expected == unsorted_batch_definition_list


def test_name_date_price_list(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
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
        ]
    )

    my_data_connector_yaml = yaml.load(f"""
        class_name: FilesDataConnector
        execution_environment_name: test_environment
        execution_engine:
            BASE_ENGINE:
            class_name: PandasExecutionEngine
        class_name: FilesDataConnector
        base_directory: {base_directory}
        glob_directive: '*.csv'
        assets:
            TestFiles:
                partitioner_name: default_partitioner_name
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
            - datetime_format: '%Y%m%d'
              orderby: desc
              class_name: DateTimeSorter
              name: timestamp
            - orderby: desc
              class_name: NumericSorter
              name: price

    """, Loader=yaml.FullLoader)

    my_data_connector: DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_filesystem_data_connector",
            "execution_environment_name": "test_environment",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    self_check_report = my_data_connector.self_check()

    assert self_check_report["class_name"] == "FilesDataConnector"
    assert self_check_report["data_asset_count"] == 1
    assert self_check_report["data_assets"]["TestFiles"]["batch_definition_count"] == 10
    assert self_check_report["unmatched_data_reference_count"] == 0

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
    ))

    expected = [
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'abe', 'timestamp': '20200809', 'price': '1040',
                             'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'alex', 'timestamp': '20200819', 'price': '1300',
                             'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'alex', 'timestamp': '20200809', 'price': '1000', 'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'eugene', 'timestamp': '20201129', 'price': '1900', 'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'eugene', 'timestamp': '20200809', 'price': '1500', 'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'james', 'timestamp': '20200811', 'price': '1009', 'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'james', 'timestamp': '20200810', 'price': '1003', 'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'james', 'timestamp': '20200713', 'price': '1567', 'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'will', 'timestamp': '20200810', 'price': '1001', 'data_asset_name': 'TestFiles'}
                        )),
        BatchDefinition(execution_environment_name="test_environment",
                        data_connector_name="general_filesystem_data_connector",
                        data_asset_name="TestFiles",
                        partition_definition=PartitionDefinition(
                            {'name': 'will', 'timestamp': '20200809', 'price': '1002', 'data_asset_name': 'TestFiles'}
                        )),
    ]



    my_batch_request: BatchRequest = BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        partition_request=PartitionRequest(**{
            "name": "james",
            "timestamp": "20200713",
            "price": "1567",
        }))

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition

    # TEST 1: Should only return the specified partition
    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )

    assert len(my_batch_definition_list) == 1
    my_batch_definition = my_batch_definition_list[0]
    expected_batch_definition: BatchDefinition = BatchDefinition(
        execution_environment_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        partition_definition=PartitionDefinition(
            {
                "name": "james",
                "timestamp": "20200713",
                "price": "1567",
            }
        ),
    )
    assert my_batch_definition == expected_batch_definition

    # TEST 2: Without partition request, should return all 10
    my_batch_request: BatchRequest = BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        partition_request=None
    )
    # should return 10
    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 10


def test_alpha(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector__alpha"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            'test_dir_alpha/A.csv',
            'test_dir_alpha/B.csv',
            'test_dir_alpha/C.csv',
            'test_dir_alpha/D.csv',
        ]
    )

    my_data_connector_yaml = yaml.load(f"""
                module_name: great_expectations.execution_environment.data_connector
                class_name: FilesDataConnector
                base_directory: {base_directory + "/test_dir_alpha"}
                assets:
                  A:
                    glob_directive: '*.csv'
                default_regex:
                    pattern: (.*).csv
                    group_names:
                    - part_1
            """, Loader=yaml.FullLoader)

    my_data_connector: DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_filesystem_data_connector",
            "execution_environment_name": "BASE",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    self_check_report = my_data_connector.self_check()
    print(json.dumps(self_check_report, indent=2))

    assert self_check_report["class_name"] == "FilesDataConnector"
    assert self_check_report["data_asset_count"] == 1
    assert set(list(self_check_report["data_assets"].keys())) == {"A"}
    assert self_check_report["unmatched_data_reference_count"] == 0

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition

    # Try to fetch a batch from a nonexistent asset
    my_batch_request: BatchRequest = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="B",
        partition_request=None
    )

    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 0

    my_batch_request: BatchRequest = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="A",
        partition_request=PartitionRequest(**{
            "part_1": "B"
        })
    )

    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 1


def test_foxtrot(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            'test_dir_foxtrot/A/A-1.csv',
            'test_dir_foxtrot/A/A-2.csv',
            'test_dir_foxtrot/A/A-3.csv',

            'test_dir_foxtrot/B/B-1.txt',
            'test_dir_foxtrot/B/B-2.txt',
            'test_dir_foxtrot/B/B-3.txt',

            'test_dir_foxtrot/C/C-2017.csv',
            'test_dir_foxtrot/C/C-2018.csv',
            'test_dir_foxtrot/C/C-2019.csv',

            'test_dir_foxtrot/D/D-aaa.csv',
            'test_dir_foxtrot/D/D-bbb.csv',
            'test_dir_foxtrot/D/D-ccc.csv',
            'test_dir_foxtrot/D/D-ddd.csv',
            'test_dir_foxtrot/D/D-eee.csv',
        ],
    )

    my_data_connector_yaml = yaml.load(f"""
            module_name: great_expectations.execution_environment.data_connector
            class_name: FilesDataConnector
            base_directory: {base_directory + "/test_dir_foxtrot"}
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
                glob_directive: '*'
                base_directory: C/
              D:
                glob_directive: '*'
                base_directory: D/
            default_regex:
                pattern: (.*)-(.*)\\.csv
                group_names:
                - part_1
                - part_2
        """, Loader=yaml.FullLoader)

    my_data_connector: DataConnector = instantiate_class_from_config(
        config=my_data_connector_yaml,
        runtime_environment={
            "name": "general_filesystem_data_connector",
            "execution_environment_name": "BASE",
            "data_context_root_directory": base_directory,
            "execution_engine": "BASE_ENGINE",
        },
        config_defaults={
            "module_name": "great_expectations.execution_environment.data_connector"
        },
    )

    self_check_report = my_data_connector.self_check()
    # TODO: This report is wrong; replace with something correct.
    print(json.dumps(self_check_report, indent=2))

    assert self_check_report == {
        "class_name": "FilesDataConnector",
        "data_asset_count": 4,
        "example_data_asset_names": [
            "A",
            "B",
            "C"
        ],
        "data_assets": {
            "A": {
                "batch_definition_count": 3,
                "example_data_references": [
                    "A-1.csv",
                    "A-2.csv",
                    "A-3.csv",
                ]
            },
            "B": {
                "batch_definition_count": 3,
                "example_data_references": [
                    "B-1.txt",
                    "B-2.txt",
                    "B-3.txt",
                ]
            },
            "C": {
                "batch_definition_count": 3,
                "example_data_references": [
                    "C-2017.csv",
                    "C-2018.csv",
                    "C-2019.csv",
                ]
            }
        },
        "unmatched_data_reference_count": 0,
        "example_unmatched_data_references": []
    }

    my_batch_definition_list: List[BatchDefinition]
    my_batch_definition: BatchDefinition

    my_batch_request = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="A",
        partition_request=None
    )

    my_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(
        batch_request=my_batch_request
    )
    assert len(my_batch_definition_list) == 3

