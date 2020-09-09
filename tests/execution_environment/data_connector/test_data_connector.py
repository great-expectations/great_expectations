import logging

import pytest

import great_expectations.execution_environment.data_connector
from great_expectations.core.id_dict import BatchKwargs
from great_expectations.execution_environment.execution_environment import \
    ExecutionEnvironment as exec
from great_expectations.execution_environment.execution_environment import *

try:
    from unittest import mock
except ImportError:
    import mock


logger = logging.getLogger(__name__)

# additional cases we want to consider :
"""

Notes:
- think of partitions as segments :
-


1. What happens if we have a

    - What does it look/feel like to start with an empty value in asset_params_test1 (or other param), and quickly iterate into a fully working config?
    - empty param:
        - what sort of message do we want?

    - how do we build the parameters?
    -


2. get rid of glob : this will read in all the files in a directory?
        - done


3. For 1, I’m thinking that it would be useful to define (with a default) a partitioner that handles the mapping in a more flexible way than just concatenation of match groups (even though I think that’s definitely a perfectly sensible approach.



4. Actually, also a third issue: how do I tell the DataConnector that the partition is an entire directory (a common case in spark especially)


"""

"""
execution_engine = {
    "class_name": "PandasExecutionEngine",
    "module_name": "great_expectations.execution_engine.pandas_execution_engine",
}

asset_params = {
    "test_assets": {
        "partition_regex": r"/Users/work/Development/GE_Data/Covid_renamed/file_(.*)_(.*).csv",
        "partition_param": ["year", "file_num"],
        "partition_delimiter": "-",
        "reader_method": "read_csv",
    }
}

data_connectors = {
    "my_files_connector": {
        "class_name": "FilesDataConnector",
        "asset_globs": asset_params,
        "base_directory": "/Users/work/Development/GE_Data/Covid_renamed/"

    }
}

execution_environment = ExecutionEnvironment(
    name="foo", execution_engine=execution_engine, data_connectors=data_connectors
)

my_connector = execution_environment.get_data_connector("my_files_connector")

result_we_get = my_connector.get_available_partitions(data_asset_name="test_assets")
"""

"""


"""


def test_data_connector_params():
    # this is the full working version
    asset_param = {
        "test_assets": {
            "partition_regex": r"file_(.*)_(.*).csv",
            "partition_param": ["year", "file_num"],
            "partition_delimiter": "-",
            "reader_method": "read_csv",
        }
    }

    execution_engine = {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine.pandas_execution_engine",
    }

    data_connectors = {
        "my_files_connector": {
            "class_name": "FilesDataConnector",
            "asset_param": asset_param,
            "base_directory": "/Users/work/Development/GE_Data/Covid_renamed/",
        }
    }

    execution_environment = ExecutionEnvironment(
        name="foo", execution_engine=execution_engine, data_connectors=data_connectors
    )

    my_connector = execution_environment.get_data_connector("my_files_connector")

    result_we_get_def = my_connector.get_available_partitions(
        data_asset_name="test_assets"
    )

    print("hello will")
    print(result_we_get_def)

    result_we_get = my_connector.get_regex("test_assets")
    print(result_we_get)


def no_test_build_execution_environment_simple_directory():
    # we have this DataSource Design documennt : https://github.com/superconductive/design/blob/main/docs/20200813_datasource_configuration.md
    """
    execution_environments:
     fa   pandas:
        default: true
        execution_engine:
            class_name: PandasDataset
        data_connectors:
            simple:
            default: true
            class_name: DataConnector
            # knows about: dataset, path, query, table_name, but requires a data_asset_name (and partition id??) to use them
    """

    execution_engine = {
        "class_name": "PandasExecutionEngine",
        "module_name": "great_expectations.execution_engine.pandas_execution_engine",
    }

    asset_param = {
        "test_assets": {
            "partition_regex": r"/Users/work/Development/GE_Data/Covid_renamed/file_(.*)_(.*).csv",
            "partition_param": ["year", "file_num"],
            "partition_delimiter": "-",
            "reader_method": "read_csv",
        }
    }

    data_connectors = {
        "my_files_connector": {
            "class_name": "FilesDataConnector",
            "asset_param": asset_param,
            "base_directory": "/Users/work/Development/GE_Data/Covid_renamed/",
        }
    }

    execution_environment = ExecutionEnvironment(
        name="foo", execution_engine=execution_engine, data_connectors=data_connectors
    )

    # assert isinstance(execution_environment, ExecutionEnvironment)

    # do we do this through config?
    # print(exec.build_configuration(class_name = "MetaPandasExecutionEngine"))

    my_connector = execution_environment.get_data_connector("my_files_connector")

    result_we_get = my_connector.get_available_partitions(data_asset_name="test_assets")

    result_we_want = [
        {
            "partition_definition": {"year": "2020", "file_num": "1"},
            "partition_id": "2020-1",
        },
        {
            "partition_definition": {"year": "2020", "file_num": "2"},
            "partition_id": "2020-2",
        },
        {
            "partition_definition": {"year": "2020", "file_num": "3"},
            "partition_id": "2020-3",
        },
    ]
    # assert result_we_get == result_we_want

    result_we_get_def = my_connector.get_available_partition_definitions(
        data_asset_name="test_assets"
    )
    result_we_want_def = [
        {"year": "2020", "file_num": "1"},
        {"year": "2020", "file_num": "2"},
        {"year": "2020", "file_num": "3"},
    ]

    # assert result_we_get_def == result_we_want_def

    result_we_get_id = my_connector.get_available_partition_ids(
        data_asset_name="test_assets"
    )
    result_we_want_id = ["2020-1", "2020-2", "2020-3"]
    # assert result_we_get_id == result_we_want_id


# def test_data_connector():

# you have something called build data connect
# you dont configure it on it's onw
#          - but you do ti
# DataSource --> Execution Environment :
# so we still need to build this
#
# data connector requires an execution environment, and default batchparameters
# once it happens, it will take the batch parameters and generate batch kwargs
# see if we can make it work for glob reader


# new_data_connector = DataConnector(name"test", )
