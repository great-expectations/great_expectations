# import pytest
# import pandas as pd
import yaml
import json

from typing import List

from great_expectations.execution_environment.data_connector import (
    FilesDataConnector,
)
from great_expectations.data_context.util import (
    instantiate_class_from_config,
)
from tests.test_utils import (
    create_files_in_directory,
)

from great_expectations.execution_environment.data_connector import DataConnector

from great_expectations.core.batch import (
    BatchRequest,
    BatchDefinition,
    PartitionRequest,
    PartitionDefinition,
)



# FROM TEST PARTITIONING.py
# THIS TEST SHOULD WORK
# <WILL> should this be?
def test_return_all_available_partitions_illegal_index_and_limit_combination(tmp_path_factory):
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

    # this line should work:

    sorted_batch_definition_list = my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="test_environment",
        data_connector_name="general_filesystem_data_connector",
        data_asset_name="TestFiles",
        partition_request={
            "limit": 1
        },
    ))
    print(sorted_batch_definition_list)