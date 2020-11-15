import pytest

from great_expectations.exceptions import ClassInstantiationError

def read_config_from_file(config_filename):
    with open(config_filename, 'r') as f_:
        config = f_.read()
    
    return config

def test_add_store_immediately_adds_to_config(empty_data_context):
    context = empty_data_context
    config_filename = context.root_directory+"/great_expectations.yml"

    assert not "my_new_store" in read_config_from_file(config_filename)
    context.add_store(
        "my_new_store",
        {
            "module_name": "great_expectations.data_context.store",
            "class_name": "ExpectationsStore",
        }
    )
    assert "my_new_store" in read_config_from_file(config_filename)


def test_add_execution_environment(empty_data_context):
    context = empty_data_context
    config_filename = context.root_directory+"/great_expectations.yml"

    # Config can't be instantiated
    with pytest.raises(KeyError):
        context.add_execution_environment(
            "my_new_execution_environment",
            {
                "some": "broken",
                "config": "yikes",
            }
        )
    assert "my_new_execution_environment" not in context.datasources
    assert "my_new_execution_environment" not in read_config_from_file(config_filename)

    # Config doesn't instantiate an ExecutionEnvironment
    with pytest.raises(TypeError):
        context.add_execution_environment(
            "my_new_execution_environment",
            {
                "module_name": "great_expectations.data_context.store",
                "class_name": "ExpectationsStore",
            }
        )
    assert "my_new_execution_environment" not in context.datasources
    assert "my_new_execution_environment" not in read_config_from_file(config_filename)

    # Config successfully instantiates an ExecutionEnvironment
    context.add_execution_environment(
        "my_new_execution_environment",
        {
            "class_name": "ExecutionEnvironment",
            "execution_engine": {
                "class_name": "PandasExecutionEngine"
            },
            "data_connectors": {
                "test_runtime_data_connector": {
                    "class_name": "RuntimeDataConnector",
                    "runtime_keys": ["run_id", "y", "m", "d"]
                }
            }
        }
    )
    assert "my_new_execution_environment" in context.datasources
    context.get_config()
    assert "my_new_execution_environment" in read_config_from_file(config_filename)

