import pytest
import yaml
import json

from typing import List

from great_expectations.execution_environment.data_connector import DataConnector

from great_expectations.execution_environment.data_connector.sorter import(
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


# TODO: Abe 20201026: This test currently fails. We need to implement sorters before we can fix it.
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

    #print(json.dumps(self_check_report, indent=2))

    assert self_check_report["class_name"] == "FilesDataConnector"
    assert self_check_report["data_asset_count"] == 1
    assert self_check_report["data_assets"]["TestFiles"]["batch_definition_count"] == 10
    assert self_check_report["unmatched_data_reference_count"] == 0


    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
                execution_environment_name="test_environment",
                data_connector_name="general_filesystem_data_connector",
                data_asset_name="TestFiles",
            ))

    assert sorted_batch_definition_list == [{'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'abe', 'timestamp': '20200809', 'price': '1040', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'alex', 'timestamp': '20200819', 'price': '1300', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'alex', 'timestamp': '20200809', 'price': '1000', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'eugene', 'timestamp': '20201129', 'price': '1900', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'eugene', 'timestamp': '20200809', 'price': '1500', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'james', 'timestamp': '20200811', 'price': '1009', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'james', 'timestamp': '20200810', 'price': '1003', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'james', 'timestamp': '20200713', 'price': '1567', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'will', 'timestamp': '20200810', 'price': '1001', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'will', 'timestamp': '20200809', 'price': '1002', 'data_asset_name': 'TestFiles'}"}]

    my_data_connector._sorters = []
    unsorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
    ))
    
    assert unsorted_batch_definition_list == [{'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'james', 'timestamp': '20200810', 'price': '1003', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'abe', 'timestamp': '20200809', 'price': '1040', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'eugene', 'timestamp': '20200809', 'price': '1500', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'alex', 'timestamp': '20200819', 'price': '1300', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'alex', 'timestamp': '20200809', 'price': '1000', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'will', 'timestamp': '20200810', 'price': '1001', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'eugene', 'timestamp': '20201129', 'price': '1900', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'will', 'timestamp': '20200809', 'price': '1002', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'james', 'timestamp': '20200811', 'price': '1009', 'data_asset_name': 'TestFiles'}"},
     {'execution_environment_name': 'test_environment', 'data_connector_name': 'general_filesystem_data_connector',
      'data_asset_name': 'TestFiles',
      'partition_definition': "{'name': 'james', 'timestamp': '20200713', 'price': '1567', 'data_asset_name': 'TestFiles'}"}]

    # what are the downstream consequences of sorting the batch_definitions? does it
    # there