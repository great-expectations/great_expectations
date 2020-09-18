
import logging
import pytest


try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)


from great_expectations.execution_environment import ExecutionEnvironment

from great_expectations.execution_engine import PandasExecutionEngine

from great_expectations.execution_environment.data_connector.data_connector import DataConnector
from great_expectations.execution_environment.data_connector.files_data_connector import FilesDataConnector

from great_expectations.execution_environment.data_connector.partitioner.partition import Partition
from great_expectations.execution_environment.data_connector.partitioner.sorter import Sorter
from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner
from great_expectations.execution_environment.data_connector.partitioner.regex_partitioner import RegexPartitioner


execution_engine = {
    "class_name": "PandasExecutionEngine",
    "module_name": "great_expectations.execution_engine.pandas_execution_engine",
}


# <WILL> currently "runs" against a local `/my_dir` defined as base_directory. It contains empty CSV files that corresponds to this:
# batch_paths: list = [
#         "my_dir/alex_20200809_1000.csv",
#         "my_dir/eugene_20200809_1500.csv",
#         "my_dir/james_20200811_1009.csv",
#         "my_dir/abe_20200809_1040.csv",
#         "my_dir/will_20200809_1002.csv",
#         "my_dir/james_20200713_1567.csv",
#         "my_dir/eugene_20201129_1900.csv",
#         "my_dir/will_20200810_1001.csv",
#         "my_dir/james_20200810_1003.csv",
#         "my_dir/alex_20200819_1300.csv",
#     ]


data_connectors = {
    "my_data_connector": {
        "class_name": "FilesDataConnector",
        "base_directory": "/Users/work/Development/GE_Data/my_dir/",
        "assets": {
            "testing_data": {
                "partitioner_name" : "my_partitioner"
            },
            "testing_data_v2": {
                "partitioner_name": "my_partitioner"
            }
        },
        "partitioners": {
            "my_partitioner":{
                "class_name": "RegexPartitioner",
                "module_name": "great_expectations.execution_environment.data_connector.partitioner.regex_partitioner",
                "regex": r'(.*)_(.*)_(.*).csv',
                "sorters": {
                    "name":{
                        "class_name": "LexicographicalSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographical_sorter",
                        "orderby": "desc",
                    },
                    "date": {
                        "class_name": "DateTimeSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.datetime_sorter",
                        "timeformatstring": 'yyyymmdd',
                        "orderby": "desc",
                    },
                    "price": {
                        "class_name": "NumericalSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.numerical_sorter",
                        "orderby": "asc",
                    }
                }
            }
        }
    }
}


execution_environment = ExecutionEnvironment(
    name="test_env", execution_engine=execution_engine, data_connectors=data_connectors)


test_batch_definition = {
    "execution_environment": "test_env",
    "data_connector": "my_data_connector",
    "data_asset_name": "testing_data",
 }


def test_if_this_works():
    my_data_connector = execution_environment.get_data_connector("my_data_connector")
    print(my_data_connector.assets)
    print(my_data_connector.partitioners)
    print(my_data_connector.get_available_data_asset_names())



