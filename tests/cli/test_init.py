import os
import shutil
from unittest import mock

import pytest
from click.testing import CliRunner, Result

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.util import gen_directory_tree_str
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@pytest.mark.parametrize(
    "invocation,input",
    [
        ("--v3-api init", "Y\n"),
        ("--v3-api --assume-yes init", ""),
    ],
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_init_on_new_project(
    mock_emit, invocation, input, caplog, tmp_path, monkeypatch
):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    project_dir_path = tmp_path / "test_cli_init_diff"
    project_dir_path.mkdir()
    project_dir = str(project_dir_path)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(project_dir)
    result = runner.invoke(
        cli,
        invocation,
        input=input,
        catch_exceptions=False,
    )
    stdout = result.output

    assert len(stdout) < 6000, "CLI output is unreasonably long."
    assert "Always know what to expect from your data" in stdout
    lets_create_data_context = (
        "Let's create a new Data Context to hold your project configuration."
    )
    if invocation == "--v3-api init":
        assert lets_create_data_context in stdout
    if invocation == "--v3-api --assume-yes init":
        assert lets_create_data_context not in stdout
    assert (
        "Congratulations! You are now ready to customize your Great Expectations configuration."
        in stdout
    )
    assert (
        "You can customize your configuration in many ways. Here are some examples:"
        in stdout
    )

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

    # Assertions to make sure we didn't regress to v2 API
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
    assert "Done generating example Expectation Suite" not in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cancelled_cli_init_on_new_project(mock_emit, caplog, tmp_path, monkeypatch):
    monkeypatch.delenv(
        "GE_USAGE_STATS", raising=False
    )  # Undo the project-wide test default
    project_dir_path = tmp_path / "test_cli_init_diff"
    project_dir_path.mkdir()
    project_dir = str(project_dir_path)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(project_dir)
    result = runner.invoke(
        cli,
        "--v3-api init",
        input="n\n",
        catch_exceptions=False,
    )
    stdout = result.output

    assert len(stdout) < 6000, "CLI output is unreasonably long."
    assert "Always know what to expect from your data" in stdout
    assert (
        "Let's create a new Data Context to hold your project configuration." in stdout
    )
    assert "OK. You must run" in stdout
    assert "great_expectations init" in stdout
    assert "to fix the missing files!" in stdout

    assert (
        "Congratulations! You are now ready to customize your Great Expectations configuration."
        not in stdout
    )
    assert (
        "You can customize your configuration in many ways. Here are some examples:"
        not in stdout
    )

    assert not os.path.isdir(os.path.join(project_dir, "great_expectations"))
    config_path = os.path.join(project_dir, "great_expectations/great_expectations.yml")
    assert not os.path.isfile(config_path)

    obs_tree = gen_directory_tree_str(os.path.join(project_dir, "great_expectations"))

    assert obs_tree == ""

    assert mock_emit.call_count == 0
    assert mock_emit.call_args_list == []

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_init_on_existing_project_with_no_uncommitted_dirs_answering_no_then_yes_to_fixing_them(
    caplog,
    monkeypatch,
    tmp_path,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory.
    """
    root_dir_path = tmp_path / "hiya"
    root_dir_path.mkdir()
    root_dir = str(root_dir_path)

    # Create a new project from scratch that we will use for the test in the next step

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(root_dir)
    result = runner.invoke(
        cli,
        ["--v3-api", "init"],
        input=f"Y\n",
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 0

    assert (
        "Congratulations! You are now ready to customize your Great Expectations configuration."
        in stdout
    )

    context = DataContext(os.path.join(root_dir, DataContext.GE_DIR))
    uncommitted_dir = os.path.join(context.root_directory, "uncommitted")
    shutil.rmtree(uncommitted_dir)
    assert not os.path.isdir(uncommitted_dir)

    # Test the second invocation of init (answer N to update broken init)
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["--v3-api", "init"],
        input="N\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert (
        "It looks like you have a partially initialized Great Expectations project. Would you like to fix this automatically by adding the following missing files (existing files will not be modified)?"
        in stdout
    )
    assert "Great Expectations added some missing files required to run." not in stdout
    assert "OK. You must run" in stdout
    assert "great_expectations init" in stdout
    assert "to fix the missing files!" in stdout
    assert "You may see new files in" not in stdout
    assert "Would you like to build & view this project's Data Docs!?" not in stdout

    assert not os.path.isdir(uncommitted_dir)
    config_var_path = os.path.join(uncommitted_dir, "config_variables.yml")
    assert not os.path.isfile(config_var_path)

    # Test the third invocation of init (answer Yes to update broken init)
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["--v3-api", "init"],
            input="Y\n",
            catch_exceptions=False,
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert (
        "It looks like you have a partially initialized Great Expectations project. Would you like to fix this automatically by adding the following missing files (existing files will not be modified)?"
        in stdout
    )
    assert "Great Expectations added some missing files required to run." in stdout
    assert "You may see new files in" in stdout

    assert "OK. You must run" not in stdout
    assert "great_expectations init" not in stdout
    assert "to fix the missing files!" not in stdout
    assert "Would you like to build & view this project's Data Docs!?" not in stdout

    assert os.path.isdir(uncommitted_dir)
    config_var_path = os.path.join(uncommitted_dir, "config_variables.yml")
    assert os.path.isfile(config_var_path)
    with open(config_var_path) as f:
        assert f.read() == CONFIG_VARIABLES_TEMPLATE

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_init_on_complete_existing_project_all_uncommitted_dirs_exist(
    caplog,
    monkeypatch,
    tmp_path,
):
    root_dir_path = tmp_path / "hiya"
    root_dir_path.mkdir()
    root_dir = str(root_dir_path)

    # Create a new project from scratch that we will use for the test in the next step

    runner: CliRunner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(root_dir)
    result: Result = runner.invoke(
        cli,
        ["--v3-api", "init"],
        input=f"Y\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    # Now the test begins - rerun the init on an existing project

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(root_dir)
    with pytest.warns(
        UserWarning, match="Warning. An existing `great_expectations.yml` was found"
    ):
        result = runner.invoke(
            cli,
            ["--v3-api", "init"],
            input="",
            catch_exceptions=False,
        )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "This looks like an existing project that" in stdout
    assert "appears complete" in stdout
    assert "ready to roll" in stdout
    assert "Would you like to build & view this project's Data Docs" not in stdout
    assert_no_logging_messages_or_tracebacks(caplog, result)
