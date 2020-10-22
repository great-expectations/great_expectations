import pytest
import tempfile
import os
import json
from tests.test_utils import (
    create_files_in_directory,
)


def test_empty_store(empty_data_context):

    my_expectation_store = empty_data_context.test_yaml_config(
        yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
""")

    # assert False


def test_config_with_yaml_error(empty_data_context):

    with pytest.raises(Exception):
        my_expectation_store = empty_data_context.test_yaml_config(
            yaml_config="""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:
    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
EGREGIOUS FORMATTING ERROR
""")


def test_filesystem_store(empty_data_context):
    tmp_dir = str(tempfile.mkdtemp())
    with open(os.path.join(tmp_dir, "expectations_A1.json"), "w") as f_:
        f_.write("\n")
    with open(os.path.join(tmp_dir, "expectations_A2.json"), "w") as f_:
        f_.write("\n")


    my_expectation_store = empty_data_context.test_yaml_config(
        yaml_config=f"""
module_name: great_expectations.data_context.store.expectations_store
class_name: ExpectationsStore
store_backend:

    module_name: "great_expectations.data_context.store"
    class_name: TupleFilesystemStoreBackend
    base_directory: {tmp_dir}
""")


def test_empty_store2(empty_data_context):

    my_expectation_store = empty_data_context.test_yaml_config(
        yaml_config="""
class_name: ValidationsStore
store_backend:

    module_name: "great_expectations.data_context.store.store_backend"
    class_name: InMemoryStoreBackend
""")


def test_execution_environment_config(empty_data_context):

    temp_dir = str(tempfile.mkdtemp())
    create_files_in_directory(
        directory=temp_dir,
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
    print(temp_dir)

    return_obj = empty_data_context.test_yaml_config(
        yaml_config=f"""
class_name: ExecutionEnvironment

execution_engine:
    class_name: PandasExecutionEngine

data_connectors:
    my_filesystem_data_connector:
        class_name: FilesDataConnector
        base_directory: {temp_dir}
        glob_directive: '*.csv'
            
        default_partitioner: my_regex_partitioner
        partitioners:
            my_regex_partitioner:
                class_name: RegexPartitioner
                regex:
                    group_names:
                        - letter
                        - number
                    pattern: {temp_dir}/(.+)(\d+)\.csv
""", return_mode="return_object"
    )

    print(json.dumps(return_obj, indent=2))

    assert return_obj == {
        "execution_engine":{
            "class_name":"PandasExecutionEngine"
        },
        "data_connectors":{
            "count":1,
            "my_filesystem_data_connector":{
                "class_name":"FilesDataConnector",
                "data_asset_count":10,
                "example_data_asset_names":[
                    "abe_20200809_1040",
                    "alex_20200809_1000"
                ],
                "assets":{
                    "abe_20200809_1040":{
                    "partition_count":1,
                    "example_partition_names":[
                        {
                            "letter":"abe_20200809_104",
                            "number":"0"
                        }
                    ]
                    },
                    "alex_20200809_1000":{
                    "partition_count":1,
                    "example_partition_names":[
                        {
                            "letter":"alex_20200809_100",
                            "number":"0"
                        }
                    ]
                    }
                }
            }
        }
        }
