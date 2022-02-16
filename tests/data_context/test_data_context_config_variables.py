import os
from collections import OrderedDict

import pytest
from ruamel.yaml import YAML

import great_expectations as ge
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigSchema,
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.data_context.util import (
    file_relative_path,
    substitute_config_variable,
)
from great_expectations.exceptions import InvalidConfigError, MissingConfigVariableError
from tests.data_context.conftest import create_data_context_files

yaml = YAML()
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.default_flow_style = False

dataContextConfigSchema = DataContextConfigSchema()


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

    assert type(context.get_config()) == DataContextConfig

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

    # verify the same for escaped variables
    context.save_config_variable(
        "escaped_password", "this_is_$mypassword_escape_the_$signs"
    )
    dict_to_escape = {
        "drivername": "po$tgresql",
        "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "port": "5432",
        "username": "postgres",
        "password": "pas$wor$d1$",
        "database": "postgres",
    }
    context.save_config_variable(
        "escaped_password_dict",
        dict_to_escape,
    )

    context._project_config["datasources"]["mydatasource"]["batch_kwargs_generators"][
        "mygenerator"
    ]["reader_options"]["test_variable_sub_escaped"] = "${escaped_password}"
    context._project_config["datasources"]["mydatasource"]["batch_kwargs_generators"][
        "mygenerator"
    ]["reader_options"]["test_variable_sub_escaped_dict"] = "${escaped_password_dict}"

    assert (
        context.get_config().datasources["mydatasource"]["batch_kwargs_generators"][
            "mygenerator"
        ]["reader_options"]["test_variable_sub_escaped"]
        == "${escaped_password}"
    )
    assert (
        context.get_config().datasources["mydatasource"]["batch_kwargs_generators"][
            "mygenerator"
        ]["reader_options"]["test_variable_sub_escaped_dict"]
        == "${escaped_password_dict}"
    )

    # Ensure that the value saved in config variables has escaped the $
    config_variables_with_escaped_vars = context._load_config_variables_file()
    assert (
        config_variables_with_escaped_vars["escaped_password"]
        == r"this_is_\$mypassword_escape_the_\$signs"
    )
    assert config_variables_with_escaped_vars["escaped_password_dict"] == {
        "drivername": r"po\$tgresql",
        "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "port": "5432",
        "username": "postgres",
        "password": r"pas\$wor\$d1\$",
        "database": "postgres",
    }

    # Ensure that when reading the escaped config variable, the escaping should be removed
    assert (
        context.get_config_with_variables_substituted().datasources["mydatasource"][
            "batch_kwargs_generators"
        ]["mygenerator"]["reader_options"]["test_variable_sub_escaped"]
        == "this_is_$mypassword_escape_the_$signs"
    )
    assert (
        context.get_config_with_variables_substituted().datasources["mydatasource"][
            "batch_kwargs_generators"
        ]["mygenerator"]["reader_options"]["test_variable_sub_escaped_dict"]
        == dict_to_escape
    )

    assert (
        context.get_config_with_variables_substituted().datasources["mydatasource"][
            "batch_kwargs_generators"
        ]["mygenerator"]["reader_options"][
            "test_escaped_manually_entered_value_from_config"
        ]
        == "correct_hor$e_battery_$taple"
    )

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


def test_substituted_config_variables_not_written_to_file(tmp_path_factory):
    # this test uses a great_expectations.yml with almost all values replaced
    # with substitution variables

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")

    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_v013_basic_with_exhaustive_variables.yml",
        config_variables_fixture_filename="config_variables_exhaustive.yml",
    )

    # load ge config fixture for expected
    path_to_yml = (
        "../test_fixtures/great_expectations_v013_basic_with_exhaustive_variables.yml"
    )
    path_to_yml = file_relative_path(__file__, path_to_yml)
    with open(path_to_yml) as data:
        config_commented_map_from_yaml = yaml.load(data)
    expected_config = DataContextConfig.from_commented_map(
        config_commented_map_from_yaml
    )
    expected_config_commented_map = dataContextConfigSchema.dump(expected_config)
    expected_config_commented_map.pop("anonymous_usage_statistics")

    # instantiate data_context twice to go through cycle of loading config from file then saving
    context = ge.data_context.DataContext(context_path)
    context._save_project_config()
    context_config_commented_map = dataContextConfigSchema.dump(
        ge.data_context.DataContext(context_path)._project_config
    )
    context_config_commented_map.pop("anonymous_usage_statistics")

    assert context_config_commented_map == expected_config_commented_map


def test_runtime_environment_are_used_preferentially(tmp_path_factory, monkeypatch):
    monkeypatch.setenv("FOO", "BAR")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", r"ive_been_\$replaced")
    value_from_environment = "from_environment"
    os.environ["replace_me"] = value_from_environment

    value_from_runtime_override = "runtime_var"
    runtime_environment = {"replace_me": value_from_runtime_override}

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")
    asset_config_path = os.path.join(context_path, "expectations")
    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_variables.yml",
        config_variables_fixture_filename="config_variables.yml",
    )

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
    config_variables_dict = {
        "arg0": "val_of_arg_0",
        "arg2": {"v1": 2},
        "aRg3": "val_of_aRg_3",
        "ARG4": "val_of_ARG_4",
    }
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

    # Null cases
    assert substitute_config_variable("", config_variables_dict) == ""
    assert substitute_config_variable(None, config_variables_dict) == None

    # Test with mixed case
    assert (
        substitute_config_variable("prefix_${aRg3}_suffix", config_variables_dict)
        == "prefix_val_of_aRg_3_suffix"
    )
    assert (
        substitute_config_variable("${aRg3}", config_variables_dict) == "val_of_aRg_3"
    )
    # Test with upper case
    assert (
        substitute_config_variable("prefix_$ARG4/suffix", config_variables_dict)
        == "prefix_val_of_ARG_4/suffix"
    )
    assert substitute_config_variable("$ARG4", config_variables_dict) == "val_of_ARG_4"

    # Test with multiple substitutions
    assert (
        substitute_config_variable("prefix${arg0}$aRg3", config_variables_dict)
        == "prefixval_of_arg_0val_of_aRg_3"
    )

    # Escaped `$` (don't substitute, but return un-escaped string)
    assert (
        substitute_config_variable(r"abc\${arg0}\$aRg3", config_variables_dict)
        == "abc${arg0}$aRg3"
    )

    # Multiple configurations together
    assert (
        substitute_config_variable(
            r"prefix$ARG4.$arg0/$aRg3:${ARG4}/\$dontsub${arg0}:${aRg3}.suffix",
            config_variables_dict,
        )
        == "prefixval_of_ARG_4.val_of_arg_0/val_of_aRg_3:val_of_ARG_4/$dontsubval_of_arg_0:val_of_aRg_3.suffix"
    )


def test_substitute_env_var_in_config_variable_file(
    monkeypatch, empty_data_context_with_config_variables
):
    monkeypatch.setenv("FOO", "correct_val_of_replace_me")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", r"ive_been_\$replaced")
    context = empty_data_context_with_config_variables
    context_config = context.get_config_with_variables_substituted()
    my_generator = context_config["datasources"]["mydatasource"][
        "batch_kwargs_generators"
    ]["mygenerator"]
    reader_options = my_generator["reader_options"]

    assert reader_options["test_variable_sub3"] == "correct_val_of_replace_me"
    assert reader_options["test_variable_sub4"] == {
        "inner_env_sub": "correct_val_of_replace_me"
    }
    assert reader_options["password"] == "dont$replaceme"

    # Escaped variables (variables containing `$` that have been escaped)
    assert (
        reader_options["test_escaped_env_var_from_config"]
        == "prefixive_been_$replaced/suffix"
    )
    assert (
        my_generator["test_variable_escaped"]
        == "dont$replace$me$please$$$$thanksive_been_$replaced"
    )


def test_escape_all_config_variables(empty_data_context_with_config_variables):
    """
    Make sure that all types of input to escape_all_config_variables are escaped properly: str, dict, OrderedDict, list
    Make sure that changing the escape string works as expected.
    """
    context = empty_data_context_with_config_variables

    # str
    value_str = "pas$word1"
    escaped_value_str = r"pas\$word1"
    assert context.escape_all_config_variables(value=value_str) == escaped_value_str

    value_str2 = "pas$wor$d1$"
    escaped_value_str2 = r"pas\$wor\$d1\$"
    assert context.escape_all_config_variables(value=value_str2) == escaped_value_str2

    # dict
    value_dict = {
        "drivername": "postgresql",
        "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "port": "5432",
        "username": "postgres",
        "password": "pass$word1",
        "database": "postgres",
    }
    escaped_value_dict = {
        "drivername": "postgresql",
        "host": os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "port": "5432",
        "username": "postgres",
        "password": r"pass\$word1",
        "database": "postgres",
    }
    assert context.escape_all_config_variables(value=value_dict) == escaped_value_dict

    # OrderedDict
    value_ordered_dict = OrderedDict(
        [
            ("UNCOMMITTED", "uncommitted"),
            ("docs_test_folder", "test$folder"),
            (
                "test_db",
                {
                    "drivername": "postgresql",
                    "host": "some_host",
                    "port": "5432",
                    "username": "postgres",
                    "password": "pa$sword1",
                    "database": "postgres",
                },
            ),
        ]
    )
    escaped_value_ordered_dict = OrderedDict(
        [
            ("UNCOMMITTED", "uncommitted"),
            ("docs_test_folder", r"test\$folder"),
            (
                "test_db",
                {
                    "drivername": "postgresql",
                    "host": "some_host",
                    "port": "5432",
                    "username": "postgres",
                    "password": r"pa\$sword1",
                    "database": "postgres",
                },
            ),
        ]
    )
    assert (
        context.escape_all_config_variables(value=value_ordered_dict)
        == escaped_value_ordered_dict
    )

    # list
    value_list = [
        "postgresql",
        os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "5432",
        "postgres",
        "pass$word1",
        "postgres",
    ]
    escaped_value_list = [
        "postgresql",
        os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "5432",
        "postgres",
        r"pass\$word1",
        "postgres",
    ]
    assert context.escape_all_config_variables(value=value_list) == escaped_value_list

    # Custom escape string
    value_str_custom_escape_string = "pas$word1"
    escaped_value_str_custom_escape_string = "pas@*&$word1"
    assert (
        context.escape_all_config_variables(
            value=value_str_custom_escape_string, dollar_sign_escape_string="@*&$"
        )
        == escaped_value_str_custom_escape_string
    )

    value_str_custom_escape_string2 = "pas$wor$d1$"
    escaped_value_str_custom_escape_string2 = "pas@*&$wor@*&$d1@*&$"
    assert (
        context.escape_all_config_variables(
            value=value_str_custom_escape_string2, dollar_sign_escape_string="@*&$"
        )
        == escaped_value_str_custom_escape_string2
    )


def test_escape_all_config_variables_skip_substitution_vars(
    empty_data_context_with_config_variables,
):
    """
    What does this test and why?
    escape_all_config_variables(skip_if_substitution_variable=True/False) should function as documented.
    """
    context = empty_data_context_with_config_variables

    # str
    value_str = "$VALUE_STR"
    escaped_value_str = r"\$VALUE_STR"
    assert (
        context.escape_all_config_variables(
            value=value_str, skip_if_substitution_variable=True
        )
        == value_str
    )
    assert (
        context.escape_all_config_variables(
            value=value_str, skip_if_substitution_variable=False
        )
        == escaped_value_str
    )

    value_str2 = "VALUE_$TR"
    escaped_value_str2 = r"VALUE_\$TR"
    assert (
        context.escape_all_config_variables(
            value=value_str2, skip_if_substitution_variable=True
        )
        == escaped_value_str2
    )
    assert (
        context.escape_all_config_variables(
            value=value_str2, skip_if_substitution_variable=False
        )
        == escaped_value_str2
    )

    multi_value_str = "${USER}:pas$word@${HOST}:${PORT}/${DATABASE}"
    escaped_multi_value_str = r"\${USER}:pas\$word@\${HOST}:\${PORT}/\${DATABASE}"
    assert (
        context.escape_all_config_variables(
            value=multi_value_str, skip_if_substitution_variable=True
        )
        == multi_value_str
    )
    assert (
        context.escape_all_config_variables(
            value=multi_value_str, skip_if_substitution_variable=False
        )
        == escaped_multi_value_str
    )

    multi_value_str2 = "$USER:pas$word@$HOST:${PORT}/${DATABASE}"
    escaped_multi_value_str2 = r"\$USER:pas\$word@\$HOST:\${PORT}/\${DATABASE}"
    assert (
        context.escape_all_config_variables(
            value=multi_value_str2, skip_if_substitution_variable=True
        )
        == multi_value_str2
    )
    assert (
        context.escape_all_config_variables(
            value=multi_value_str2, skip_if_substitution_variable=False
        )
        == escaped_multi_value_str2
    )

    multi_value_str3 = "USER:pas$word@$HOST:${PORT}/${DATABASE}"
    escaped_multi_value_str3 = r"USER:pas\$word@\$HOST:\${PORT}/\${DATABASE}"
    assert (
        context.escape_all_config_variables(
            value=multi_value_str3, skip_if_substitution_variable=True
        )
        == escaped_multi_value_str3
    )
    assert (
        context.escape_all_config_variables(
            value=multi_value_str3, skip_if_substitution_variable=False
        )
        == escaped_multi_value_str3
    )

    # dict
    value_dict = {
        "drivername": "postgresql",
        "host": "${HOST}",
        "port": "5432",
        "username": "postgres",
        "password": "pass$word1",
        "database": "$postgres",
        "sub_dict": {
            "test_val_no_escaping": "test_val",
            "test_val_escaping": "te$t_val",
            "test_val_substitution": "$test_val",
            "test_val_substitution_braces": "${test_val}",
        },
    }
    escaped_value_dict = {
        "drivername": "postgresql",
        "host": r"\${HOST}",
        "port": "5432",
        "username": "postgres",
        "password": r"pass\$word1",
        "database": r"\$postgres",
        "sub_dict": {
            "test_val_no_escaping": "test_val",
            "test_val_escaping": r"te\$t_val",
            "test_val_substitution": r"\$test_val",
            "test_val_substitution_braces": r"\${test_val}",
        },
    }
    escaped_value_dict_skip_substitution_variables = {
        "drivername": "postgresql",
        "host": "${HOST}",
        "port": "5432",
        "username": "postgres",
        "password": r"pass\$word1",
        "database": "$postgres",
        "sub_dict": {
            "test_val_no_escaping": "test_val",
            "test_val_escaping": r"te\$t_val",
            "test_val_substitution": "$test_val",
            "test_val_substitution_braces": "${test_val}",
        },
    }
    assert (
        context.escape_all_config_variables(
            value=value_dict, skip_if_substitution_variable=False
        )
        == escaped_value_dict
    )
    assert (
        context.escape_all_config_variables(
            value=value_dict, skip_if_substitution_variable=True
        )
        == escaped_value_dict_skip_substitution_variables
    )

    # OrderedDict
    value_ordered_dict = OrderedDict(
        [
            ("UNCOMMITTED", "uncommitted"),
            ("docs_test_folder", "test$folder"),
            (
                "test_db",
                {
                    "drivername": "$postgresql",
                    "host": "some_host",
                    "port": "5432",
                    "username": "${USERNAME}",
                    "password": "pa$sword1",
                    "database": "postgres",
                },
            ),
        ]
    )
    escaped_value_ordered_dict = OrderedDict(
        [
            ("UNCOMMITTED", "uncommitted"),
            ("docs_test_folder", r"test\$folder"),
            (
                "test_db",
                {
                    "drivername": r"\$postgresql",
                    "host": "some_host",
                    "port": "5432",
                    "username": r"\${USERNAME}",
                    "password": r"pa\$sword1",
                    "database": "postgres",
                },
            ),
        ]
    )
    escaped_value_ordered_dict_skip_substitution_variables = OrderedDict(
        [
            ("UNCOMMITTED", "uncommitted"),
            ("docs_test_folder", r"test\$folder"),
            (
                "test_db",
                {
                    "drivername": "$postgresql",
                    "host": "some_host",
                    "port": "5432",
                    "username": "${USERNAME}",
                    "password": r"pa\$sword1",
                    "database": "postgres",
                },
            ),
        ]
    )
    assert (
        context.escape_all_config_variables(
            value=value_ordered_dict, skip_if_substitution_variable=False
        )
        == escaped_value_ordered_dict
    )
    assert (
        context.escape_all_config_variables(
            value=value_ordered_dict, skip_if_substitution_variable=True
        )
        == escaped_value_ordered_dict_skip_substitution_variables
    )

    # list
    value_list = [
        "postgresql",
        os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "5432",
        "$postgres",
        "pass$word1",
        "${POSTGRES}",
    ]
    escaped_value_list = [
        "postgresql",
        os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "5432",
        r"\$postgres",
        r"pass\$word1",
        r"\${POSTGRES}",
    ]
    escaped_value_list_skip_substitution_variables = [
        "postgresql",
        os.getenv("GE_TEST_LOCAL_DB_HOSTNAME", "localhost"),
        "5432",
        "$postgres",
        r"pass\$word1",
        "${POSTGRES}",
    ]
    assert (
        context.escape_all_config_variables(
            value=value_list, skip_if_substitution_variable=False
        )
        == escaped_value_list
    )
    assert (
        context.escape_all_config_variables(
            value=value_list, skip_if_substitution_variable=True
        )
        == escaped_value_list_skip_substitution_variables
    )


def test_create_data_context_and_config_vars_in_code(tmp_path_factory, monkeypatch):
    """
    What does this test and why?
    Creating a DataContext via .create(), then using .save_config_variable() to save a variable that will eventually be substituted (e.g. ${SOME_VAR}) should result in the proper escaping of $.
    This is in response to issue #2196
    """

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context = ge.DataContext.create(
        project_root_dir=project_path,
        usage_statistics_enabled=False,
    )

    CONFIG_VARS = {
        "DB_HOST": "${DB_HOST_FROM_ENV_VAR}",
        "DB_NAME": "DB_NAME",
        "DB_USER": "DB_USER",
        "DB_PWD": "pas$word",
    }
    for k, v in CONFIG_VARS.items():
        context.save_config_variable(k, v)

    config_vars_file_contents = context._load_config_variables_file()

    # Add escaping for DB_PWD since it is not of the form ${SOMEVAR} or $SOMEVAR
    CONFIG_VARS_WITH_ESCAPING = CONFIG_VARS.copy()
    CONFIG_VARS_WITH_ESCAPING["DB_PWD"] = r"pas\$word"

    # Ensure all config vars saved are in the config_variables.yml file
    # and that escaping was added for "pas$word" -> "pas\$word"
    assert all(
        item in config_vars_file_contents.items()
        for item in CONFIG_VARS_WITH_ESCAPING.items()
    )
    assert not all(
        item in config_vars_file_contents.items() for item in CONFIG_VARS.items()
    )

    # Add env var for substitution
    monkeypatch.setenv("DB_HOST_FROM_ENV_VAR", "DB_HOST_FROM_ENV_VAR_VALUE")

    datasource_config = DatasourceConfig(
        class_name="SqlAlchemyDatasource",
        credentials={
            "drivername": "postgresql",
            "host": "$DB_HOST",
            "port": "65432",
            "database": "${DB_NAME}",
            "username": "${DB_USER}",
            "password": "${DB_PWD}",
        },
    )
    datasource_config_schema = DatasourceConfigSchema()

    # use context.add_datasource to test this by adding a datasource with values to substitute.
    context.add_datasource(
        initialize=False,
        name="test_datasource",
        **datasource_config_schema.dump(datasource_config)
    )

    assert context.list_datasources()[0]["credentials"] == {
        "drivername": "postgresql",
        "host": "DB_HOST_FROM_ENV_VAR_VALUE",
        "port": "65432",
        "database": "DB_NAME",
        "username": "DB_USER",
        # Note masking of "password" field
        "password": "***",
    }

    # Check context substitutes escaped variables appropriately
    data_context_config_schema = DataContextConfigSchema()
    context_with_variables_substituted_dict = data_context_config_schema.dump(
        context.get_config_with_variables_substituted()
    )

    test_datasource_credentials = context_with_variables_substituted_dict[
        "datasources"
    ]["test_datasource"]["credentials"]

    assert test_datasource_credentials["host"] == "DB_HOST_FROM_ENV_VAR_VALUE"
    assert test_datasource_credentials["username"] == "DB_USER"
    assert test_datasource_credentials["password"] == "pas$word"
    assert test_datasource_credentials["database"] == "DB_NAME"

    # Ensure skip_if_substitution_variable=False works as documented
    context.save_config_variable(
        "escaped", "$SOME_VAR", skip_if_substitution_variable=False
    )
    context.save_config_variable(
        "escaped_curly", "${SOME_VAR}", skip_if_substitution_variable=False
    )

    config_vars_file_contents = context._load_config_variables_file()

    assert config_vars_file_contents["escaped"] == r"\$SOME_VAR"
    assert config_vars_file_contents["escaped_curly"] == r"\${SOME_VAR}"
