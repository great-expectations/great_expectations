
import logging

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)


from great_expectations.execution_environment import ExecutionEnvironment

from great_expectations.execution_environment.data_connector.data_connector import DataConnector

from great_expectations.execution_environment.data_connector.partitioner.partitioner import Partitioner

execution_engine = {
    "class_name": "PandasExecutionEngine",
    "module_name": "great_expectations.execution_engine.pandas_execution_engine",
}



# <WILL> - this is the full data_connector config that is here for reference. the tests only use the small portions

data_connectors = {
    "my_data_connector": {
        "class_name": "FilesDataConnector",
        "base_directory": "/Users/work/Development/GE_Data/my_dir/",
        "assets": {
            "testing_data": {
                "partitioner_name" : "my_partitioner"
            },
        },
        "partitioners": {
            "my_partitioner":{
                "class_name": "RegexPartitioner",
                "module_name": "great_expectations.execution_environment.data_connector.partitioner.regex_partitioner",
                "regex": r'(.*)_(.*)_(.*).csv',
                "sorters": {
                    "name":{
                        "class_name": "LexicographicSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographic_sorter",
                        "orderby": "desc",
                    },
                    "date": {
                        "class_name": "DateTimeSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.date_time_sorter",
                        "timeformatstring": 'yyyymmdd',
                        "orderby": "desc",
                    },
                    "price": {
                        "class_name": "NumericSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.numeric_sorter",
                        "orderby": "asc",
                    }
                },
            }
        }
    }
}



# Necessary paramter 1
batch_paths: list = [
        "my_dir/alex_20200809_1000.csv",
        "my_dir/eugene_20200809_1500.csv",
        "my_dir/james_20200811_1009.csv",
        "my_dir/abe_20200809_1040.csv",
        "my_dir/will_20200809_1002.csv",
        "my_dir/james_20200713_1567.csv",
        "my_dir/eugene_20201129_1900.csv",
        "my_dir/will_20200810_1001.csv",
        "my_dir/james_20200810_1003.csv",
        "my_dir/alex_20200819_1300.csv",
    ]

# necessary parameter 2
sorter_config = {
                    "name":{
                        "class_name": "LexicographicSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.lexicographic_sorter",
                        "orderby": "desc",
                    },
                    "date": {
                        "class_name": "DateTimeSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.date_time_sorter",
                        "timeformatstring": 'yyyymmdd',
                        "orderby": "desc",
                    },
                    "price": {
                        "class_name": "NumericSorter",
                        "module_name": "great_expectations.execution_environment.data_connector.partitioner.sorter.numeric_sorter",
                        "orderby": "asc",
                    }
                }



execution_environment = ExecutionEnvironment(
    name="test_env", execution_engine=execution_engine, data_connectors=data_connectors)

test_batch_definition = {
    "execution_environment": "test_env",
    "data_connector": "my_data_connector",
    "data_asset_name": "testing_data",
 }


def test_sorter_instantiation():

    my_data_connector = execution_environment.get_data_connector("my_data_connector")
    my_partitioner = Partitioner(
        data_connector=my_data_connector,
        name="mine_all_mine",
        paths=batch_paths,
        regex=r'(.*)_(.*)_(.*).csv',
        allow_multifile_partitions=False,
        sorters=sorter_config
    )

    name_sorter = my_partitioner.get_sorter("name")
    #assert name_sorter == {'name': 'name', 'orderby': 'desc', 'reverse': True, 'type': 'LexicographicSorter'}

    date_sorter = my_partitioner.get_sorter("date")
    #assert date_sorter == {'name': 'date', 'orderby': 'desc', 'reverse': True, 'type': 'DateTimeSorter'}

    price_sorter = my_partitioner.get_sorter("price")
    #assert price_sorter == {'name': 'price', 'orderby': 'asc', 'reverse': False, 'type': 'NumericSorter'}


    all_sorters = my_partitioner.get_all_sorters()
    #assert all_sorters ==  [
    #    {'name': 'name', 'orderby': 'desc', 'reverse': True, 'type': 'LexicographicSorter'},
    #    {'name': 'date', 'orderby': 'desc', 'reverse': True, 'type': 'DateTimeSorter'},
    #    {'name': 'price', 'orderby': 'asc', 'reverse': False, 'type': 'NumericSorter'}
    #]