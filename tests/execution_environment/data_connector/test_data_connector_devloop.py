import logging

import pytest

try:
    from unittest import mock
except ImportError:
    import mock

import great_expectations.execution_environment.data_connector
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.execution_environment.execution_environment import (
    ExecutionEnvironment as exec,
)
from great_expectations.execution_environment.execution_environment import *

logger = logging.getLogger(__name__)


def print_data_connector(my_connector):
    data_asset_names = my_connector.get_available_data_asset_names()
    print(data_asset_names)
    # print("=== Data asset names ===")
    #print(data_asset_names)

    #print("\n=== Partitions ===")
    #for data_asset_name in data_asset_names:
    #    print(data_asset_name, ":", my_connector.get_available_partitions(data_asset_name=data_asset_name))

def test_simple():
    # loading parameters
    execution_engine = {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine.pandas_execution_engine",
    }

    asset_params_test_empty = {
        "test_assets": {
            "reader_method": "read_csv",
        }
    }

    asset_params_test_year_only_1 = {
        "test_assets": {
            "partition_regex": r"file_(.*)_.*.csv",
            "partition_param": ["year"],
            "partition_delimiter": "-",
            "reader_method": "read_csv",
        }
    }

    asset_params_all = {
        "test_assets": {
            "partition_regex": r"(.*)/file_(.*)_(.*).csv",
            "partition_param": ["subdir_name", "year", "file_num"],
            "partition_delimiter": "-",
            "reader_method": "read_csv",
        }
    }

    # loading parameters
    execution_engine = {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine.pandas_execution_engine",
    }

    data_connectors = {
        "my_files_connector": {
            "class_name": "FilesDataConnector",
            "asset_param": asset_params_all,
            "base_directory": "/Users/work/Development/GE_Data/NestedDirectory/"
        }
    }

    execution_environment = ExecutionEnvironment(
        name="foo", execution_engine=execution_engine, data_connectors=data_connectors
    )

    my_connector = execution_environment.get_data_connector("my_files_connector")
    result_we_get = my_connector.get_available_partitions(data_asset_name="test_assets")
    print(result_we_get)
    print_data_connector(my_connector)
