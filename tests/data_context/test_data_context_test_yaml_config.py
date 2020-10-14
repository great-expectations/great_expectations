import pytest
import tempfile
import os

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

def create_files(
    path: str,
    file_name_list: list
):
    #NOTE: A more sophisticated version of this method would create subdirectories, too.
    for file_name in file_name_list:	
        file_path = os.path.join(path, file_name)	
        with open(file_path, "w") as fp:	
            fp.writelines([f'The name of this file is: "{file_path}".\n'])	

def test_execution_environment_config(empty_data_context):

    temp_dir = str(tempfile.mkdtemp())
    create_files(
        path=temp_dir,
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

#     my_execution_environnment = empty_data_context.test_yaml_config(
#         yaml_config=f"""
# class_name: ExecutionEnvironment
# execution_engine:
#     module_name: great_expectations.execution_engine.pandas_execution_engine
#     class_name: PandasExecutionEngine
#     engine_spec_passthrough:
#         reader_method: read_csv
#         reader_options:
#         header: 0

# data_connectors:
#     subdir:
#         module_name: great_expectations.execution_environment.data_connector.files_data_connector
#         class_name: FilesDataConnector
#         assets:
#         engine_spec_passthrough:
#             reader_method: read_csv
#             reader_options:
#                 header: 0
#         base_path: {temp_dir}
# """)
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
                config_params:
                    regex:
                        group_names:
                            - letter
                            - number
                        pattern: {temp_dir}/(.+)(\d+)\.csv
""", return_mode="return_object"
    )

#     return_obj = empty_data_context.test_yaml_config(
#         yaml_config=f"""
# class_name: ExecutionEnvironment
# execution_engine:
#     module_name: great_expectations.execution_engine.pandas_execution_engine
#     class_name: PandasExecutionEngine
#     engine_spec_passthrough:
#         reader_method: read_csv
#         reader_options:
#         header: 0

# data_connectors:
#     test_filesystem_data_connector:
#         module_name: great_expectations.execution_environment.data_connector
#         class_name: FilesDataConnector

#         config_params:
#             base_directory: {temp_dir}
#             glob_directive: "*"
            
#         partitioners:
#             test_regex_partitioner:
#                 module_name: great_expectations.execution_environment.data_connector.partitioner
#                 class_name: RegexPartitioner
#                 config_params:
#                     regex:
#                         pattern: ".+\/(.+)_(.+)_(.+)\\\.csv"
#                         group_names: group_names

#                     allow_multipart_partitions: False,
#                     sorters: sorters

#         default_partitioner: test_regex_partitioner
#         assets:
#             test_asset_0:
#                 config_params:
#                     glob_directive: "alex*"
#                 partitioner: test_regex_partitioner
#                 base_directory: data_asset_base_directory
# """, return_mode="return_object")
    
    print(return_obj)