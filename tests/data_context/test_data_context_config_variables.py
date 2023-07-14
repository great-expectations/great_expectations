import os
import shutil
from collections import OrderedDict

import pytest

from great_expectations.core.config_provider import _ConfigurationSubstitutor
from great_expectations.core.yaml_handler import YAMLHandler
from great_expectations.data_context.data_context.file_data_context import (
    FileDataContext,
)
from great_expectations.data_context.types.base import (
    DataContextConfig,
    DataContextConfigSchema,
    DatasourceConfig,
    DatasourceConfigSchema,
)
from great_expectations.data_context.util import PasswordMasker, file_relative_path
from great_expectations.exceptions import InvalidConfigError, MissingConfigVariableError
from great_expectations.util import get_context
from tests.data_context.conftest import create_data_context_files

yaml = YAMLHandler()

dataContextConfigSchema = DataContextConfigSchema()


@pytest.fixture
def empty_data_context_with_config_variables(monkeypatch, empty_data_context):
    monkeypatch.setenv("FOO", "BAR")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", "ive_been_$--replaced")
    root_dir = empty_data_context.root_directory
    ge_config_path = file_relative_path(
        __file__,
        "../test_fixtures/great_expectations_basic_with_variables.yml",
    )
    shutil.copy(
        ge_config_path, os.path.join(root_dir, "great_expectations.yml")  # noqa: PTH118
    )
    config_variables_path = file_relative_path(
        __file__,
        "../test_fixtures/config_variables.yml",
    )
    shutil.copy(
        config_variables_path, os.path.join(root_dir, "uncommitted")  # noqa: PTH118
    )
    return get_context(context_root_dir=root_dir)


@pytest.mark.filesystem
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


@pytest.mark.filesystem
def test_substituted_config_variables_not_written_to_file(tmp_path_factory):
    # this test uses a great_expectations.yml with almost all values replaced
    # with substitution variables

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118

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
    context = get_context(context_root_dir=context_path)
    context._save_project_config()
    context_config_commented_map = dataContextConfigSchema.dump(
        get_context(context_root_dir=context_path)._project_config
    )
    context_config_commented_map.pop("anonymous_usage_statistics")

    assert context_config_commented_map == expected_config_commented_map


@pytest.mark.filesystem
def test_runtime_environment_are_used_preferentially(tmp_path_factory, monkeypatch):
    monkeypatch.setenv("FOO", "BAR")
    monkeypatch.setenv("REPLACE_ME_ESCAPED_ENV", r"ive_been_\$replaced")
    value_from_environment = "from_environment"
    os.environ["replace_me"] = value_from_environment

    value_from_runtime_override = "runtime_var"
    runtime_environment = {"replace_me": value_from_runtime_override}

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context_path = os.path.join(project_path, "great_expectations")  # noqa: PTH118
    asset_config_path = os.path.join(context_path, "expectations")  # noqa: PTH118
    create_data_context_files(
        context_path,
        asset_config_path,
        ge_config_fixture_filename="great_expectations_basic_with_variables.yml",
        config_variables_fixture_filename="config_variables.yml",
    )

    data_context = get_context(
        context_root_dir=context_path, runtime_environment=runtime_environment
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


@pytest.mark.unit
def test_substitute_config_variable():
    config_substitutor = _ConfigurationSubstitutor()
    config_variables_dict = {
        "arg0": "val_of_arg_0",
        "arg2": {"v1": 2},
        "aRg3": "val_of_aRg_3",
        "ARG4": "val_of_ARG_4",
    }
    assert (
        config_substitutor.substitute_config_variable(
            "abc${arg0}", config_variables_dict
        )
        == "abcval_of_arg_0"
    )
    assert (
        config_substitutor.substitute_config_variable("abc$arg0", config_variables_dict)
        == "abcval_of_arg_0"
    )
    assert (
        config_substitutor.substitute_config_variable("${arg0}", config_variables_dict)
        == "val_of_arg_0"
    )
    assert (
        config_substitutor.substitute_config_variable("hhhhhhh", config_variables_dict)
        == "hhhhhhh"
    )
    with pytest.raises(MissingConfigVariableError) as exc:
        config_substitutor.substitute_config_variable(
            "abc${arg1} def${foo}", config_variables_dict
        )  # does NOT equal "abc${arg1}"
    assert (
        "Unable to find a match for config substitution variable: `arg1`."
        in exc.value.message
    )
    assert (
        config_substitutor.substitute_config_variable("${arg2}", config_variables_dict)
        == config_variables_dict["arg2"]
    )
    assert exc.value.missing_config_variable == "arg1"

    # Null cases
    assert (
        config_substitutor.substitute_config_variable("", config_variables_dict)
        == ""  # noqa: PLC1901
    )
    assert (
        config_substitutor.substitute_config_variable(None, config_variables_dict)
        is None
    )

    # Test with mixed case
    assert (
        config_substitutor.substitute_config_variable(
            "prefix_${aRg3}_suffix", config_variables_dict
        )
        == "prefix_val_of_aRg_3_suffix"
    )
    assert (
        config_substitutor.substitute_config_variable("${aRg3}", config_variables_dict)
        == "val_of_aRg_3"
    )
    # Test with upper case
    assert (
        config_substitutor.substitute_config_variable(
            "prefix_$ARG4/suffix", config_variables_dict
        )
        == "prefix_val_of_ARG_4/suffix"
    )
    assert (
        config_substitutor.substitute_config_variable("$ARG4", config_variables_dict)
        == "val_of_ARG_4"
    )

    # Test with multiple substitutions
    assert (
        config_substitutor.substitute_config_variable(
            "prefix${arg0}$aRg3", config_variables_dict
        )
        == "prefixval_of_arg_0val_of_aRg_3"
    )

    # Escaped `$` (don't substitute, but return un-escaped string)
    assert (
        config_substitutor.substitute_config_variable(
            r"abc\${arg0}\$aRg3", config_variables_dict
        )
        == "abc${arg0}$aRg3"
    )

    # Multiple configurations together
    assert (
        config_substitutor.substitute_config_variable(
            r"prefix$ARG4.$arg0/$aRg3:${ARG4}/\$dontsub${arg0}:${aRg3}.suffix",
            config_variables_dict,
        )
        == "prefixval_of_ARG_4.val_of_arg_0/val_of_aRg_3:val_of_ARG_4/$dontsubval_of_arg_0:val_of_aRg_3.suffix"
    )


@pytest.mark.filesystem
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


@pytest.mark.unit
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


@pytest.mark.unit
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


@pytest.mark.filesystem
def test_create_data_context_and_config_vars_in_code(tmp_path_factory, monkeypatch):
    """
    What does this test and why?
    Creating a DataContext via .create(), then using .save_config_variable() to save a variable that will eventually be substituted (e.g. ${SOME_VAR}) should result in the proper escaping of $.
    This is in response to issue #2196
    """

    project_path = str(tmp_path_factory.mktemp("data_context"))
    context = FileDataContext.create(
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

    config_vars_file_contents = context.config_variables

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
        class_name="Datasource",
        execution_engine={
            "class_name": "SqlAlchemyExecutionEngine",
            "module_name": "great_expectations.execution_engine",
            "credentials": {
                "drivername": "postgresql",
                "host": "$DB_HOST",
                "port": "65432",
                "database": "${DB_NAME}",
                "username": "${DB_USER}",
                "password": "${DB_PWD}",
            },
        },
    )

    datasource_config_schema = DatasourceConfigSchema()

    # use context.add_datasource to test this by adding a datasource with values to substitute.
    context.add_datasource(
        initialize=False,
        name="test_datasource",
        **datasource_config_schema.dump(datasource_config),
    )

    assert context.list_datasources()[0]["execution_engine"]["credentials"] == {
        "drivername": "postgresql",
        "host": "DB_HOST_FROM_ENV_VAR_VALUE",
        "port": "65432",
        "database": "DB_NAME",
        "username": "DB_USER",
        # Note masking of "password" field
        "password": PasswordMasker.MASKED_PASSWORD_STRING,
    }

    # Check context substitutes escaped variables appropriately
    data_context_config_schema = DataContextConfigSchema()
    context_with_variables_substituted_dict = data_context_config_schema.dump(
        context.get_config_with_variables_substituted()
    )

    test_datasource_credentials = context_with_variables_substituted_dict[
        "datasources"
    ]["test_datasource"]["execution_engine"]["credentials"]

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

    config_vars_file_contents = context.config_variables

    assert config_vars_file_contents["escaped"] == r"\$SOME_VAR"
    assert config_vars_file_contents["escaped_curly"] == r"\${SOME_VAR}"
