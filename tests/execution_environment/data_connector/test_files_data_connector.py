# import pytest
# import pandas as pd
# import yaml
# import json

from great_expectations.execution_environment.data_connector import (
    FilesDataConnector,
)
# from great_expectations.data_context.util import (
#     instantiate_class_from_config,
# )
from tests.test_utils import (
    create_files_in_directory,
)

from great_expectations.core.batch import (
    BatchRequest,
#     BatchDefinition,
#     PartitionRequest,
#     PartitionDefinition,
)

def test_basic_instantiation(tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "2020/01/alpha-1001.csv",
            "2020/01/beta-1002.csv",
            "2020/02/alpha-1003.csv",
            "2020/02/beta-1004.csv",
            "2020/03/alpha-1005.csv",
            "2020/03/beta-1006.csv",
            "2020/04/beta-1007.csv",
        ]
    )

    my_data_connector = FilesDataConnector(
        name="my_data_connector",
        execution_environment_name="FAKE_EXECUTION_ENVIRONMENT_NAME",
        partitioners = {
            "my_partitioner" : {
                "class_name": "RegexPartitioner",
                "pattern": "(.d{4})/(.d{2})/alpha-(\\d+)\\.csv",
                "group_names": ["year", "month", "letter", "number"],
            }
        },
        default_partitioner_name="my_partitioner",
        base_directory=base_directory,
        glob_directive="*/*/*.csv",
        # assets = {
        #     "alpha" : {}
        # }
    )

    my_data_connector.refresh_data_references_cache()
    assert my_data_connector.get_data_reference_list_count() == 7
    assert my_data_connector.get_unmatched_data_references() == []

    print(my_data_connector.get_batch_definition_list_from_batch_request(BatchRequest(
        execution_environment_name="something",
        data_connector_name="my_data_connector",
        data_asset_name="something",
    )))

    my_data_connector.self_check()

def test_instantiation_from_a_config(empty_data_context, tmp_path_factory):
    base_directory = str(tmp_path_factory.mktemp("test_test_yaml_config"))
    create_files_in_directory(
        directory=base_directory,
        file_name_list=[
            "alpha-1.csv",
            "alpha-2.csv",
            "alpha-3.csv",
        ]
    )

    return_object = empty_data_context.test_yaml_config(f"""
module_name: great_expectations.execution_environment.data_connector
class_name: FilesDataConnector
execution_environment_name: FAKE_EXECUTION_ENVIRONMENT
name: TEST_DATA_CONNECTOR

base_directory: {base_directory}/
# glob_directive: "*.csv"

default_regex:
    pattern: alpha-(.*)\\.csv
    group_names:
        - index

assets:
    alpha:
        partitioner_name: my_partitioner

    """, return_mode="return_object")

    assert return_object == {
        'class_name': 'FilesDataConnector',
        'data_asset_count': 1,
        'example_data_asset_names': [
            'alpha',
        ],
        'data_assets': {
            'alpha': {
                'example_data_references': ['alpha-1.csv', 'alpha-2.csv', 'alpha-3.csv'],
                'batch_definition_count': 3
            },
        },
        'example_unmatched_data_references': [],
        'unmatched_data_reference_count': 0,
    }