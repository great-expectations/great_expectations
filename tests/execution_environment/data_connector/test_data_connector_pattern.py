import pytest
import yaml
import json

from great_expectations.execution_environment.data_connector import (
    DataConnector
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


def test_name_date_price_list(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=["alex_20200809_1000.csv",
                        "eugene_20200809_1500.csv",
                        "james_20200811_1009.csv",
                        "abe_20200809_1040.csv",
                        "will_20200809_1002.csv",
                        "james_20200713_1567.csv",
                        "eugene_20201129_1900.csv",
                        "will_20200810_1001.csv",
                        "james_20200810_1003.csv",
                        "alex_20200819_1300.csv", ]
    )
    my_data_connector_yaml = yaml.load(f"""
        module_name: great_expectations.execution_environment.data_connector
        class_name: FilesDataConnector
        base_directory: {base_directory}
        glob_directive: '*'
        default_partitioner: my_standard_partitioner
        assets:
          DEFAULT_ASSET_NAME:
            class_name: Asset
            glob_directive: '*'
            partitioner: my_standard_partitioner
        partitioners:
          my_standard_partitioner:
            class_name: RegexPartitioner
            regex:
              pattern: .+\/(.+)_(.+)_(.+)\.csv
              group_names:
              - name
              - timestamp
              - price
            allow_multipart_partitions: false
            sorters:
            - orderby: asc
              class_name: LexicographicSorter
              name: name
              datetime_format: '%Y%m%d'
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
    # TODO: This report is wrong; replace with something correct.
    assert self_check_report == {}

    my_batch_request = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="DEFAULT_ASSET_NAME",
        partition_request=PartitionRequest(**{
            "name": "james",
            "timestamp": "20200713",
            "price": "1567",
        }))


    # TEST 1: Should only return the specified partition
    my_batch_definition = my_data_connector.get_batch_definition_list_from_batch_request(my_batch_request)
    assert len(my_batch_definition) == 1
    expected_batch_definition = BatchDefinition(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="DEFAULT_ASSET_NAME",
        partition_definition=PartitionDefinition({"name": "james", "timestamp": "20200713", "price": "1567"}),
    )
    assert my_batch_definition[0] == expected_batch_definition

    # TEST 2: Without partition request, should return all 10
    my_batch_request = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="DEFAULT_ASSET_NAME",
        partition_request=None)
    # should return 10
    my_batch_definition = my_data_connector.get_batch_definition_list_from_batch_request(my_batch_request)
    assert len(my_batch_definition) == 10



def test_alpha(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("basic_data_connector__filesystem_data_connector"))
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
                glob_directive: '*'
                default_partitioner: my_standard_partitioner
                assets:
                  A:
                    class_name: Asset
                    glob_directive: '*.csv'
                    partitioner: my_standard_partitioner
                  B:
                    class_name: Asset
                    glob_directive: '*.csv'
                    partitioner: my_standard_partitioner
                  C:
                    class_name: Asset
                    glob_directive: '*.csv'
                    partitioner: my_standard_partitioner
                  D:
                    class_name: Asset
                    glob_directive: '*.csv'
                    partitioner: my_standard_partitioner
                partitioners:
                  my_standard_partitioner:
                    class_name: RegexPartitioner
                    regex:
                      pattern: .*/(.*).csv
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
    # TODO: This report is wrong; replace with something correct.
    assert self_check_report == {}

    # TODO : What should work
    my_batch_request = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="B",
        partition_request=None)

    my_batch_definition = my_data_connector.get_batch_definition_list_from_batch_request(my_batch_request)
    assert my_batch_definition == []


    # TODO : What actually works
    my_batch_request = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="DEFAULT_ASSET_NAME",
        partition_request= PartitionRequest(**{
            "part_1": "B"
        }))

    my_batch_definition = my_data_connector.get_batch_definition_list_from_batch_request(my_batch_request)
    print(my_batch_definition[0])




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
            glob_directive: '*'
            default_partitioner: my_standard_partitioner
            assets:
              A:
                class_name: Asset
                glob_directive: '*'
                base_directory: A/
                partitioner: my_standard_partitioner
              B:
                class_name: Asset
                glob_directive: '*'
                base_directory: B/
                partitioner: my_standard_partitioner
              C:
                class_name: Asset
                glob_directive: '*'
                base_directory: C/
                partitioner: my_standard_partitioner
              D:
                class_name: Asset
                glob_directive: '*'
                base_directory: D/
                partitioner: my_standard_partitioner
            partitioners:
              my_standard_partitioner:
                class_name: RegexPartitioner
                regex:
                  pattern: (.*)-(.*).csv
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
    print(json.dumps(self_check_report, indent=2))
    # TODO: This report is wrong; replace with something correct.
    assert self_check_report == {
      "class_name": "FilesDataConnector",
      "data_asset_count": 4,
      "example_data_asset_names": [
        "A",
        "B",
        "C"
      ],
      "assets": {
        "A": {
          "batch_definition_count": 0,
          "example_data_references": []
        },
        "B": {
          "batch_definition_count": 0,
          "example_data_references": []
        },
        "C": {
          "batch_definition_count": 0,
          "example_data_references": []
        }
      },
      "unmatched_data_reference_count": 4,
      "example_unmatched_data_references": [
        "/private/var/folders/tc/579dj32d3pjdkx1vbxdm3ss80000gn/T/pytest-of-abe/pytest-1435/basic_data_connector__filesystem_data_connector2/test_dir_foxtrot/A",
        "/private/var/folders/tc/579dj32d3pjdkx1vbxdm3ss80000gn/T/pytest-of-abe/pytest-1435/basic_data_connector__filesystem_data_connector2/test_dir_foxtrot/C",
        "/private/var/folders/tc/579dj32d3pjdkx1vbxdm3ss80000gn/T/pytest-of-abe/pytest-1435/basic_data_connector__filesystem_data_connector2/test_dir_foxtrot/D"
      ]
    }

    # TODO : What should work
    my_batch_request = BatchRequest(
        execution_environment_name="BASE",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="A",
        partition_request=None)

    my_batch_definition = my_data_connector.get_batch_definition_list_from_batch_request(my_batch_request)
    assert my_batch_definition == []
