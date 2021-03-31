import os
import re
import shutil
from unittest import mock

import pytest
from click.testing import CliRunner, Result
from freezegun import freeze_time

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import gen_directory_tree_str
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_cli_init_on_existing_project_with_no_uncommitted_dirs_answering_yes_to_fixing_them(
    mock_webbrowser,
    caplog,
    monkeypatch,
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
    monkeypatch.chdir(root_dir)
    result = runner.invoke(
        cli,
        ["--v3-api", "init"],
        input=f"\n\n1\n1\n{data_folder_path}\n\n\n\n2\n{data_path}\n\n\n\n",
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
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["--v3-api", "init"],
            input="Y\nn\n",
            catch_exceptions=False,
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


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_cli_init_on_complete_existing_project_all_uncommitted_dirs_exist(
    mock_webbrowser,
    caplog,
    monkeypatch,
    tmp_path_factory,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory.
    """
    root_dir: str = str(tmp_path_factory.mktemp("hiya"))
    os.makedirs(os.path.join(root_dir, "data"))
    data_folder_path: str = os.path.join(root_dir, "data")
    data_path: str = os.path.join(root_dir, "data", "Titanic.csv")
    fixture_path: str = file_relative_path(
        __file__, os.path.join("..", "test_sets", "Titanic.csv")
    )
    shutil.copy(fixture_path, data_path)

    # Create a new project from scratch that we will use for the test in the next step

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(root_dir)
    result: Result = runner.invoke(
        cli,
        ["--v3-api", "init"],
        input=f"\n\n1\n1\n{data_folder_path}\n\n\n\n2\n{data_path}\n\n\n\n",
        catch_exceptions=False,
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
    monkeypatch.chdir(root_dir)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["--v3-api", "init"],
            input="n\n",
            catch_exceptions=False,
        )
    stdout = result.stdout
    assert mock_webbrowser.call_count == 1

    assert result.exit_code == 0
    assert "This looks like an existing project that" in stdout
    assert "appears complete" in stdout
    assert "ready to roll" in stdout
    assert "Would you like to build & view this project's Data Docs" in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_cli_init_connection_string_non_working_db_connection_instructs_user_and_leaves_entries_in_config_files_for_debugging(
    mock_webbrowser, caplog, monkeypatch, tmp_path_factory, sa
):
    root_dir = tmp_path_factory.mktemp("bad_con_string_test")
    root_dir = str(root_dir)
    os.chdir(root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(root_dir)
    result = runner.invoke(
        cli,
        ["--v3-api", "init"],
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


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@freeze_time("09/26/2019 13:42:41")
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_init_on_new_project_assume_yes_flag(
    mock_emit, mock_webbrowser, caplog, tmp_path_factory, monkeypatch
):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    project_dir = str(tmp_path_factory.mktemp("test_cli_init_diff"))
    os.makedirs(os.path.join(project_dir, "data"))
    data_path = os.path.join(project_dir, "data", "Titanic.csv")
    fixture_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(fixture_path, data_path)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(project_dir)
    result = runner.invoke(
        cli,
        ["--v3-api", "--assume-yes", "init"],
        catch_exceptions=False,
    )
    stdout = result.output
    assert mock_webbrowser.call_count == 0

    assert len(stdout) < 6000, "CLI output is unreasonably long."
    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" not in stdout
    assert "What are you processing your files with" not in stdout
    assert (
        "Enter the path of a data file (relative or absolute, s3a:// and gs:// paths are ok too)"
        not in stdout
    )
    assert "Name the new Expectation Suite [Titanic.warning]" not in stdout
    assert (
        "Great Expectations will choose a couple of columns and generate expectations about them"
        not in stdout
    )
    assert "Generating example Expectation Suite..." not in stdout
    assert "Building" not in stdout
    assert "Data Docs" not in stdout
    assert "Done generating example Expectation Suite" not in stdout
    assert "Great Expectations is now set up" in stdout

    assert os.path.isdir(os.path.join(project_dir, "great_expectations"))
    config_path = os.path.join(project_dir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path))
    assert config["datasources"] == {}

    obs_tree = gen_directory_tree_str(os.path.join(project_dir, "great_expectations"))

    assert (
        obs_tree
        == """great_expectations/
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

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event_payload": {"api_version": "v3"},
                "event": "cli.init.create",
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)
