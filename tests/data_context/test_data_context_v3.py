import pytest

from great_expectations.data_context.types.base import DataContextConfig

def test_get_config(empty_data_context_v3):
    context = empty_data_context_v3

    #We can call get_config in several different modes
    assert type(context.get_config()) == DataContextConfig
    assert type(context.get_config(mode="typed")) == DataContextConfig
    assert type(context.get_config(mode="dict")) == dict
    assert type(context.get_config(mode="yaml")) == str
    with pytest.raises(ValueError):
        context.get_config(mode="foobar")

    print(context.get_config(mode="yaml"))
    print(context.get_config("dict").keys())

    assert set(context.get_config("dict").keys()) == set([
        'config_version',
        'datasources',
        'config_variables_file_path',
        'plugins_directory',
        'validation_operators',
        'stores',
        'expectations_store_name',
        'validations_store_name',
        'evaluation_parameter_store_name',
        'data_docs_sites',
        'anonymous_usage_statistics'
    ])


def test_config_variables(empty_data_context_v3):
    context = empty_data_context_v3
    assert type(context.config_variables) == dict
    assert set(context.config_variables.keys()) == {"instance_id"}
