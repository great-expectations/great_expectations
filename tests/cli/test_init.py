import os
import shutil
from unittest import mock

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import gen_directory_tree_str
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_cli_init_on_existing_project_with_no_uncommitted_dirs_answering_yes_to_fixing_them(
    mock_webbrowser,
    caplog,
    tmp_path_factory,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory.
    """
    root_dir = tmp_path_factory.mktemp("hiya")
    root_dir = str(root_dir)
    os.makedirs(os.path.join(root_dir, "data"))
    data_folder_path = os.path.join(root_dir, "data")
    data_path = os.path.join(root_dir, "data", "Titanic.csv")
    fixture_path = file_relative_path(
        __file__, os.path.join("..", "test_sets", "Titanic.csv")
    )
    shutil.copy(fixture_path, data_path)

    # Create a new project from scratch that we will use for the test in the next step

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["init", "-d", root_dir],
        input="\n\n1\n1\n{}\n\n\n\n2\n{}\n\n\n\n".format(data_folder_path, data_path),
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/validations/Titanic/warning/".format(
            root_dir
        )
        in mock_webbrowser.call_args[0][0]
    )

    assert "Great Expectations is now set up." in stdout

    context = DataContext(os.path.join(root_dir, DataContext.GE_DIR))
    uncommitted_dir = os.path.join(context.root_directory, "uncommitted")
    shutil.rmtree(uncommitted_dir)
    assert not os.path.isdir(uncommitted_dir)

    # Test the second invocation of init
    runner = CliRunner(mix_stderr=False)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli, ["init", "-d", root_dir], input="Y\nn\n", catch_exceptions=False
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Great Expectations added some missing files required to run." in stdout
    assert "You may see new files in" in stdout

    assert "OK. You must run" not in stdout
    assert "great_expectations init" not in stdout
    assert "to fix the missing files!" not in stdout
    assert "Would you like to build & view this project's Data Docs!?" in stdout

    assert os.path.isdir(uncommitted_dir)
    config_var_path = os.path.join(uncommitted_dir, "config_variables.yml")
    assert os.path.isfile(config_var_path)
    with open(config_var_path) as f:
        assert f.read() == CONFIG_VARIABLES_TEMPLATE

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_cli_init_on_complete_existing_project_all_uncommitted_dirs_exist(
    mock_webbrowser,
    caplog,
    tmp_path_factory,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory.
    """
    root_dir = tmp_path_factory.mktemp("hiya")
    root_dir = str(root_dir)
    os.makedirs(os.path.join(root_dir, "data"))
    data_folder_path = os.path.join(root_dir, "data")
    data_path = os.path.join(root_dir, "data", "Titanic.csv")
    fixture_path = file_relative_path(
        __file__, os.path.join("..", "test_sets", "Titanic.csv")
    )
    shutil.copy(fixture_path, data_path)

    # Create a new project from scratch that we will use for the test in the next step

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["init", "-d", root_dir],
        input="\n\n1\n1\n{}\n\n\n\n2\n{}\n\n\n\n".format(
            data_folder_path, data_path, catch_exceptions=False
        ),
    )
    assert result.exit_code == 0
    assert mock_webbrowser.call_count == 1
    assert (
        "{}/great_expectations/uncommitted/data_docs/local_site/validations/Titanic/warning/".format(
            root_dir
        )
        in mock_webbrowser.call_args[0][0]
    )

    # Now the test begins - rerun the init on an existing project

    runner = CliRunner(mix_stderr=False)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli, ["init", "-d", root_dir], input="n\n", catch_exceptions=False
        )
    stdout = result.stdout
    assert mock_webbrowser.call_count == 1

    assert result.exit_code == 0
    assert "This looks like an existing project that" in stdout
    assert "appears complete" in stdout
    assert "ready to roll" in stdout
    assert "Would you like to build & view this project's Data Docs" in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_cli_init_connection_string_non_working_db_connection_instructs_user_and_leaves_entries_in_config_files_for_debugging(
    mock_webbrowser, caplog, tmp_path_factory, sa
):
    root_dir = tmp_path_factory.mktemp("bad_con_string_test")
    root_dir = str(root_dir)
    os.chdir(root_dir)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["init"],
        input="\n\n2\n6\nmy_db\nsqlite:////not_a_real.db\n\nn\n",
        catch_exceptions=False,
    )
    stdout = result.output
    assert mock_webbrowser.call_count == 0

    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "Which database backend are you using" in stdout
    assert "What is the url/connection string for the sqlalchemy connection" in stdout
    assert "Give your new Datasource a short name" in stdout
    assert "Attempting to connect to your database. This may take a moment" in stdout
    assert "Cannot connect to the database" in stdout

    assert "Profiling" not in stdout
    assert "Building" not in stdout
    assert "Data Docs" not in stdout
    assert "Great Expectations is now set up" not in stdout

    assert result.exit_code == 1

    ge_dir = os.path.join(root_dir, DataContext.GE_DIR)
    assert os.path.isdir(ge_dir)
    config_path = os.path.join(ge_dir, DataContext.GE_YML)
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path))
    assert config["datasources"] == {
        "my_db": {
            "data_asset_type": {
                "module_name": None,
                "class_name": "SqlAlchemyDataset",
            },
            "credentials": "${my_db}",
            "class_name": "SqlAlchemyDatasource",
            "module_name": "great_expectations.datasource",
        }
    }

    config_path = os.path.join(
        ge_dir, DataContext.GE_UNCOMMITTED_DIR, "config_variables.yml"
    )
    config = yaml.load(open(config_path))
    assert config["my_db"] == {"url": "sqlite:////not_a_real.db"}

    obs_tree = gen_directory_tree_str(os.path.join(root_dir, "great_expectations"))
    assert (
        obs_tree
        == """\
great_expectations/
    .gitignore
    great_expectations.yml
    checkpoints/
    expectations/
        .ge_store_backend_id
    notebooks/
        pandas/
            validation_playground.ipynb
        spark/
            validation_playground.ipynb
        sql/
            validation_playground.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
        validations/
            .ge_store_backend_id
"""
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)
