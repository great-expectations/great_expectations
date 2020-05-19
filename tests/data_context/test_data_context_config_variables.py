import os

import pytest
import great_expectations as ge
from great_expectations.data_context.util import substitute_config_variable
from great_expectations.exceptions import InvalidConfigError, MissingConfigVariableError
from tests.data_context.conftest import create_data_context_files


def test_config_variables_on_context_without_config_variables_filepath_configured(
    data_context_without_config_variables_filepath_configured,
):

    # test the behavior on a context that does not config_variables_filepath (the location of
    # the file with config variables values) configured.

    context = data_context_without_config_variables_filepath_configured

    # an attempt to save a config variable should raise an exception

    with pytest.raises(InvalidConfigError) as exc:
        context.save_config_variable("var_name_1", {"n1": "v1"})
    assert (
        "'config_variables_file_path' property is not found in config"
        in exc.value.message
    )


def test_setting_config_variables_is_visible_immediately(
    data_context_with_variables_in_config,
):
    context = data_context_with_variables_in_config

    config_variables_file_path = context.get_config()["config_variables_file_path"]

    assert config_variables_file_path == "uncommitted/config_variables.yml"

    # The config variables must have been present to instantiate the config
    assert os.path.isfile(
        os.path.join(context._context_root_directory, config_variables_file_path)
    )

    # the context's config has two config variables - one using the ${} syntax and the other - $.
    assert (
        context.get_config()["datasources"]["mydatasource"]["batch_kwargs_generators"][
            "mygenerator"
        ]["reader_options"]["test_variable_sub1"]
        == "${replace_me}"
    )
    assert (
        context.get_config()["datasources"]["mydatasource"]["batch_kwargs_generators"][
            "mygenerator"
        ]["reader_options"]["test_variable_sub2"]
        == "$replace_me"
    )

    config_variables = context._load_config_variables_file()
    assert config_variables["replace_me"] == {"n1": "v1"}

    # the context's config has two config variables - one using the ${} syntax and the other - $.
    assert context.get_config_with_variables_substituted().datasources["mydatasource"][
        "batch_kwargs_generators"
    ]["mygenerator"]["reader_options"]["test_variable_sub1"] == {"n1": "v1"}
    assert context.get_config_with_variables_substituted().datasources["mydatasource"][
        "batch_kwargs_generators"
    ]["mygenerator"]["reader_options"]["test_variable_sub2"] == {"n1": "v1"}

    # verify that we can save a config variable in the config variables file
    # and the value is retrievable
    context.save_config_variable("replace_me_2", {"n2": "v2"})
    # Update the config itself
    context._project_config["datasources"]["mydatasource"]["batch_kwargs_generators"][
        "mygenerator"
    ]["reader_options"]["test_variable_sub1"] = "${replace_me_2}"

    # verify that the value of the config variable is immediately updated.
    # verify that the config variable will be substituted with the value from the file if the
    # env variable is not set (for both ${} and $ syntax variations)
    assert context.get_config_with_variables_substituted().datasources["mydatasource"][
        "batch_kwargs_generators"
    ]["mygenerator"]["reader_options"]["test_variable_sub1"] == {"n2": "v2"}
    assert context.get_config_with_variables_substituted().datasources["mydatasource"][
        "batch_kwargs_generators"
    ]["mygenerator"]["reader_options"]["test_variable_sub2"] == {"n1": "v1"}

    try:
        # verify that the value of the env var takes precedence over the one from the config variables file
        os.environ["replace_me_2"] = "value_from_env_var"
        assert (
            context.get_config_with_variables_substituted().datasources["mydatasource"][
                "batch_kwargs_generators"
            ]["mygenerator"]["reader_options"]["test_variable_sub1"]
            == "value_from_env_var"
        )
    except Exception:
        raise
    finally:
        del os.environ["replace_me_2"]


def test_runtime_environment_are_used_preferentially(tmp_path_factory):
    value_from_environment = "from_environment"
    os.environ["replace_me"] = value_from_environment

    value_from_runtime_override = "runtime_var"
    runtime_environment = {"replace_me": value_from_runtime_override}

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    create_data_context_files(context_path, asset_config_path, True)

    data_context = ge.data_context.DataContext(
        context_path, runtime_environment=runtime_environment
    )
    config = data_context.get_config_with_variables_substituted()

    try:
        assert (
            config.datasources["mydatasource"]["batch_kwargs_generators"][
                "mygenerator"
            ]["reader_options"]["test_variable_sub1"]
            == value_from_runtime_override
        )
        assert (
            config.datasources["mydatasource"]["batch_kwargs_generators"][
                "mygenerator"
            ]["reader_options"]["test_variable_sub2"]
            == value_from_runtime_override
        )
    except Exception:
        raise
    finally:
        del os.environ["replace_me"]


def test_substitute_config_variable():
    config_variables_dict = {"arg0": "val_of_arg_0", "arg2": {"v1": 2}}
    assert (
        substitute_config_variable("abc${arg0}", config_variables_dict)
        == "abcval_of_arg_0"
    )
    assert (
        substitute_config_variable("abc$arg0", config_variables_dict)
        == "abcval_of_arg_0"
    )
    assert (
        substitute_config_variable("${arg0}", config_variables_dict) == "val_of_arg_0"
    )
    assert substitute_config_variable("hhhhhhh", config_variables_dict) == "hhhhhhh"
    with pytest.raises(MissingConfigVariableError) as exc:
        substitute_config_variable(
            "abc${arg1} def${foo}", config_variables_dict
        )  # does NOT equal "abc${arg1}"
    assert (
        """Unable to find a match for config substitution variable: `arg1`.
Please add this missing variable to your `uncommitted/config_variables.yml` file or your environment variables.
See https://great-expectations.readthedocs.io/en/latest/reference/data_context_reference.html#managing-environment-and-secrets"""
        in exc.value.message
    )
    assert (
        substitute_config_variable("${arg2}", config_variables_dict)
        == config_variables_dict["arg2"]
    )
    assert exc.value.missing_config_variable == "arg1"
