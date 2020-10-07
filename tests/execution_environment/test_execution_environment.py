from great_expectations.execution_environment import ExecutionEnvironment
import yaml

def test_basic_execution_environment_setup():

    datasource = ExecutionEnvironment(
        "my_pandas_datasource",
        **yaml.load("""
execution_engine:
    module_name: great_expectations.execution_engine.pandas_execution_engine
    class_name: PandasExecutionEngine
    engine_spec_passthrough:
        reader_method: read_csv
        reader_options:
        header: 0

data_connector:
    subdir:
        module_name: great_expectations.execution_environment.data_connector.files_data_connector
        class_name: FilesDataConnector
        assets:
        rapid_prototyping:
            partitioner:
            regex: /foo/(.*)\.csv
            partition_id:
                - file
        # engine spec passthrough at per-asset level
        engine_spec_passthrough:
            reader_method: read_csv
            reader_options:
            header: 0
        base_path: /usr/data
        # engine spec passthrough at a per-connector level
        # closest to invocation overrides
        engine_spec_passthrough:
        reader_method: read_csv
        reader_options:
            header: 0
    
    """, Loader=yaml.FullLoader)
    )