import pytest
import os
from great_expectations.exceptions import InvalidConfigError


def test_config_variables_on_context_without_config_variables_filepath_configured(data_context_without_config_variables_filepath_configured):

    # test the behavior on a context that does not config_variables_filepath (the location of
    # the file with config variables values) configured.

    context = data_context_without_config_variables_filepath_configured

    # an attempt to save a config variable should raise an exception

    with pytest.raises(InvalidConfigError) as exc:
        context.save_config_variable("var_name_1", {"n1": "v1"})
        assert "'config_variables_file_path' property is not found in config" in exc.message


def test_setting_config_variables_is_visible_immediately(data_context_with_variables_in_config):
    context = data_context_with_variables_in_config

    config_variables_file_path = context.get_config()["config_variables_file_path"]

    assert config_variables_file_path == "uncommitted/config_variables.yml"

    # verify the that config variables file does not exist in this fixture - we will check later that
    # it is automatically created on the first write
    assert not os.path.isfile(os.path.join(context._context_root_directory, config_variables_file_path))

    # the context's config has a one config variable
    assert context.get_config_with_variables_substituted()["datasources"]["mydatasource"]["generators"]["mygenerator"]["reader_options"]["test_variable_sub"] == "${replace_me}"

    # verify that the config variable will be substituted with the value of the env var if there is no value in the
    # config variables file
    os.environ["replace_me"] = "value_from_env_var"
    assert context.get_config_with_variables_substituted()["datasources"]["mydatasource"]["generators"]["mygenerator"][
               "reader_options"]["test_variable_sub"] == "value_from_env_var"


    # verify that we can save a config variable in the config variables file
    # and the value is retrievable
    context.save_config_variable("replace_me", {"n1": "v1"})
    config_variables = context._load_config_variables_file()
    assert config_variables["replace_me"] == {"n1": "v1"}

    # verify that the value of the config variable is immediately updated and the one from the
    # config variables file takes precedence over the one from the env var
    context._project_config_with_varibles_substituted["datasources"]["mydatasource"]["generators"]["mygenerator"][
               "reader_options"]["test_variable_sub"] == {"n1": "v1"}
