import json
import os
from typing import Dict, List
from unittest import mock

import pytest
from click.testing import CliRunner, Result

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.core.batch import BatchRequest
from great_expectations.core.expectation_suite import ExpectationSuite
from tests.cli.utils import (
    VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    assert_no_logging_messages_or_tracebacks,
)
from tests.render.test_util import (
    find_code_in_notebook,
    load_notebook_from_path,
    run_notebook,
)


def test_suite_help_output(caplog):
    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(cli, ["--v3-api", "suite"], catch_exceptions=False)
    assert result.exit_code == 0
    stdout: str = result.stdout
    assert (
        """
Usage: great_expectations suite [OPTIONS] COMMAND [ARGS]...

  Expectation Suite operations

Options:
  --help  Show this message and exit.

Commands:
  delete   Delete an expectation suite from the expectation store.
  demo     This command is not supported in the v3 (Batch Request) API.
  edit     Generate a Jupyter notebook for editing an existing Expectation...
  list     Lists available Expectation Suites.
  new      Create a new empty Expectation Suite.
"""
        in stdout
    )
    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def test_suite_demo_deprecation_message(caplog, monkeypatch, empty_data_context):
    context: DataContext = empty_data_context

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite demo",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "This command is not supported in the v3 (Batch Request) API." in stdout

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_prompted_default_with_jupyter(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "warning"

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite new",
        input="\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "Select a datasource" not in stdout
    assert f"Name the new Expectation Suite [warning]:" in stdout
    assert "Opening a notebook for you now to edit your expectation suite!" in stdout
    assert "If you wish to avoid this you can add the `--no-jupyter` flag." in stdout

    expected_suite_path: str = os.path.join(
        project_dir, "expectations", f"{expectation_suite_name}.json"
    )
    assert os.path.isfile(expected_suite_path)

    expected_notebook_path: str = os.path.join(
        uncommitted_dir, f"edit_{expectation_suite_name}.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    run_notebook(
        notebook_path=expected_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=suite_identifier)",
        replacement_string="",
    )

    context = DataContext(context_root_dir=project_dir)
    assert expectation_suite_name in context.list_expectation_suite_names()

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert suite.expectations == []

    assert mock_subprocess.call_count == 1
    call_args: List[str] = mock_subprocess.call_args[0][0]
    assert call_args[0] == "jupyter"
    assert call_args[1] == "notebook"
    assert expected_notebook_path in call_args[2]

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_prompted_custom_with_jupyter(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite new",
        input=f"{expectation_suite_name}\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "Select a datasource" not in stdout
    assert f"Name the new Expectation Suite [warning]:" in stdout
    assert "Opening a notebook for you now to edit your expectation suite!" in stdout
    assert "If you wish to avoid this you can add the `--no-jupyter` flag." in stdout

    expected_suite_path: str = os.path.join(
        project_dir, "expectations", f"{expectation_suite_name}.json"
    )
    assert os.path.isfile(expected_suite_path)

    expected_notebook_path: str = os.path.join(
        uncommitted_dir, f"edit_{expectation_suite_name}.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    run_notebook(
        notebook_path=expected_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=suite_identifier)",
        replacement_string="",
    )

    context = DataContext(context_root_dir=project_dir)
    assert expectation_suite_name in context.list_expectation_suite_names()

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert suite.expectations == []

    assert mock_subprocess.call_count == 1
    call_args: List[str] = mock_subprocess.call_args[0][0]
    assert call_args[0] == "jupyter"
    assert call_args[1] == "notebook"
    assert expected_notebook_path in call_args[2]

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_arg_custom_with_jupyter(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite new --expectation-suite {expectation_suite_name}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "Select a datasource" not in stdout
    assert "Opening a notebook for you now to edit your expectation suite!" in stdout
    assert "If you wish to avoid this you can add the `--no-jupyter` flag." in stdout

    expected_suite_path: str = os.path.join(
        project_dir, "expectations", f"{expectation_suite_name}.json"
    )
    assert os.path.isfile(expected_suite_path)

    expected_notebook_path: str = os.path.join(
        uncommitted_dir, f"edit_{expectation_suite_name}.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    run_notebook(
        notebook_path=expected_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=suite_identifier)",
        replacement_string="",
    )

    context = DataContext(context_root_dir=project_dir)
    assert expectation_suite_name in context.list_expectation_suite_names()

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert suite.expectations == []

    assert mock_subprocess.call_count == 1
    call_args: List[str] = mock_subprocess.call_args[0][0]
    assert call_args[0] == "jupyter"
    assert call_args[1] == "notebook"
    assert expected_notebook_path in call_args[2]

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_arg_custom_with_no_jupyter(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite new --expectation-suite {expectation_suite_name} --no-jupyter",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "Select a datasource" not in stdout
    assert (
        "Opening a notebook for you now to edit your expectation suite!" not in stdout
    )
    assert (
        "If you wish to avoid this you can add the `--no-jupyter` flag." not in stdout
    )

    expected_suite_path: str = os.path.join(
        project_dir, "expectations", f"{expectation_suite_name}.json"
    )
    assert os.path.isfile(expected_suite_path)

    expected_notebook_path: str = os.path.join(
        uncommitted_dir, f"edit_{expectation_suite_name}.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    run_notebook(
        notebook_path=expected_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=suite_identifier)",
        replacement_string="",
    )

    context = DataContext(context_root_dir=project_dir)
    assert expectation_suite_name in context.list_expectation_suite_names()

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert suite.expectations == []

    assert mock_subprocess.call_count == 0

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_interactive_nonexistent_batch_request_json_file_raises_error(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory

    expectation_suite_name: str = "test_suite_name"

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"""--v3-api suite new --expectation-suite {expectation_suite_name} --interactive --batch-request
nonexistent_file.json --no-jupyter
""",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert 'The JSON file with the path "nonexistent_file.json' in stdout

    context = DataContext(context_root_dir=project_dir)
    assert expectation_suite_name not in context.list_expectation_suite_names()

    assert mock_subprocess.call_count == 0

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_interactive_malformed_batch_request_json_file_raises_error(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    batch_request_file_path: str = os.path.join(uncommitted_dir, f"batch_request.json")
    with open(batch_request_file_path, "w") as json_file:
        json_file.write("not_proper_json")

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"""--v3-api suite new --expectation-suite {expectation_suite_name} --interactive --batch-request
{batch_request_file_path} --no-jupyter
""",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Error" in stdout
    assert "occurred while attempting to load the JSON file with the path" in stdout

    context = DataContext(context_root_dir=project_dir)
    assert expectation_suite_name not in context.list_expectation_suite_names()

    assert mock_subprocess.call_count == 0

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_interactive_valid_batch_request_from_json_file_in_notebook(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    batch_request_file_path: str = os.path.join(uncommitted_dir, f"batch_request.json")
    with open(batch_request_file_path, "w") as json_file:
        json.dump(batch_request, json_file)

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"""--v3-api suite new --expectation-suite {expectation_suite_name} --interactive --batch-request
{batch_request_file_path} --no-jupyter
""",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "Error" not in stdout

    expected_suite_path: str = os.path.join(
        project_dir, "expectations", f"{expectation_suite_name}.json"
    )
    assert os.path.isfile(expected_suite_path)

    expected_notebook_path: str = os.path.join(
        uncommitted_dir, f"edit_{expectation_suite_name}.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    batch_request_string: str = (
        str(BatchRequest(**batch_request))
        .replace("{\n", "{\n  ")
        .replace(",\n", ",\n  ")
        .replace("\n}", ",\n}")
    )
    batch_request_string = fr"batch_request = {batch_request_string}"

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string=batch_request_string,
    )
    assert len(cells_of_interest_dict) == 1

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string="context.open_data_docs(resource_identifier=suite_identifier)",
    )
    assert not cells_of_interest_dict

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string="context.open_data_docs(resource_identifier=validation_result_identifier)",
    )
    assert len(cells_of_interest_dict) == 1

    run_notebook(
        notebook_path=expected_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=validation_result_identifier)",
        replacement_string="",
    )

    context = DataContext(context_root_dir=project_dir)
    assert expectation_suite_name in context.list_expectation_suite_names()

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert suite.expectations == []

    assert mock_subprocess.call_count == 0

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def test_suite_edit_without_suite_name_raises_error(monkeypatch, empty_data_context):
    """This is really only testing click missing arguments"""
    context: DataContext = empty_data_context
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(cli, "--v3-api suite edit", catch_exceptions=False)
    assert result.exit_code == 2

    assert (
        'Error: Missing argument "EXPECTATION_SUITE".' in result.stderr
        or "Error: Missing argument 'EXPECTATION_SUITE'." in result.stderr
    )


def test_suite_edit_datasource_and_batch_request_error(monkeypatch, empty_data_context):
    """This is really only testing click missing arguments"""
    context: DataContext = empty_data_context
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    expectation_suite_name: str = "test_suite_name"

    # noinspection PyUnusedLocal
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert (
        context.list_expectation_suites()[0].expectation_suite_name
        == expectation_suite_name
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite edit {expectation_suite_name} --datasource-name some_datasource_name --batch-request some_file.json --interactive",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "Only one of --datasource-name DATASOURCE_NAME and --batch-request <path to JSON file> options can be used."
        in stdout
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_non_existent_suite_name_raises_error(
    mock_webbrowser, mock_subprocess, caplog, monkeypatch, empty_data_context
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    context: DataContext = empty_data_context

    assert not context.list_expectation_suites()

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite edit not_a_real_suite",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Could not find a suite named `not_a_real_suite`." in stdout
    assert "by running `great_expectations suite list`" in stdout

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_non_existent_datasource_shows_helpful_error_message(
    mock_webbrowser, mock_subprocess, caplog, monkeypatch, empty_data_context
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    context: DataContext = empty_data_context
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    expectation_suite_name: str = "test_suite_name"

    # noinspection PyUnusedLocal
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert (
        context.list_expectation_suites()[0].expectation_suite_name
        == expectation_suite_name
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite edit {expectation_suite_name} --interactive --datasource-name not_real",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert (
        "Unable to load datasource `not_real` -- no configuration found or invalid configuration."
        in stdout
    )

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_no_additional_args_without_citations(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    Here we verify that the "suite edit" command helps the user to specify batch_request
    when it is called without the optional command-line arguments that specify the batch.

    First, we call the "suite new" command to create the expectation suite our test
    will edit -- this step is a just a setup.

    We then call the "suite edit" command without any optional command-line arguments.  This means
    that the command will help us specify batch_request interactively.

    The data context has two datasources -- we choose one of them.
    After that, we select a data connector and, finally, select a data asset from the list.

    The command should:
    - NOT open Data Docs
    - open jupyter
    """
    context: DataContext = titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }
    batch_request_string: str = (
        str(BatchRequest(**batch_request))
        .replace("{\n", "{\n  ")
        .replace(",\n", ",\n  ")
        .replace("\n}", ",\n}")
    )
    batch_request_string = fr"batch_request = {batch_request_string}"

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--expectation-suite",
            f"{expectation_suite_name}",
            "--interactive",
            "--no-jupyter",
        ],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "A batch of data is required to edit the suite" in stdout
    assert "Select a datasource" in stdout

    assert mock_webbrowser.call_count == 0
    mock_webbrowser.reset_mock()

    assert mock_subprocess.call_count == 0
    mock_subprocess.reset_mock()

    # remove the citations from the suite
    context = DataContext(context_root_dir=project_dir)

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert isinstance(suite, ExpectationSuite)

    suite.meta.pop("citations", None)
    context.save_expectation_suite(expectation_suite=suite)

    # Actual testing really starts here
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "edit",
            f"{expectation_suite_name}",
            "--interactive",
        ],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    stdout = result.stdout
    assert "A batch of data is required to edit the suite" in stdout
    assert "Select a datasource" in stdout

    expected_notebook_path: str = os.path.join(
        uncommitted_dir, f"edit_{expectation_suite_name}.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path: str = os.path.join(
        project_dir, "expectations", f"{expectation_suite_name}.json"
    )
    assert os.path.isfile(expected_suite_path)

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string=batch_request_string,
    )
    assert len(cells_of_interest_dict) == 1

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string="context.open_data_docs(resource_identifier=suite_identifier)",
    )
    assert not cells_of_interest_dict

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string="context.open_data_docs(resource_identifier=validation_result_identifier)",
    )
    assert len(cells_of_interest_dict) == 1

    run_notebook(
        notebook_path=expected_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=validation_result_identifier)",
        replacement_string="",
    )

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 1

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_no_additional_args_with_citations(
    mock_webbrowser,
    mock_subprocess,
    caplog,
    monkeypatch,
    titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    Here we verify that the "suite edit" command uses the batch_request found in
    citations in the existing suite when it is called without the optional
    command-line arguments that specify the batch.

    First, we call the "suite new" command to create the expectation suite our
    test will edit -- this step is a just a setup.

    We then call the "suite edit" command without any optional command-line-arguments.

    The command should:
    - NOT open Data Docs
    - NOT open jupyter
    """
    context: DataContext = titanic_v013_multi_datasource_pandas_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
        "limit": 1000,
    }
    batch_request_string: str = (
        str(BatchRequest(**batch_request))
        .replace("{\n", "{\n  ")
        .replace(",\n", ",\n  ")
        .replace("\n}", ",\n}")
    )
    batch_request_string = fr"batch_request = {batch_request_string}"

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--expectation-suite",
            f"{expectation_suite_name}",
            "--interactive",
            "--no-jupyter",
        ],
        input="2\n1\n1\n\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "A batch of data is required to edit the suite" in stdout
    assert "Select a datasource" in stdout

    assert mock_webbrowser.call_count == 0
    mock_webbrowser.reset_mock()

    assert mock_subprocess.call_count == 0
    mock_subprocess.reset_mock()

    context = DataContext(context_root_dir=project_dir)

    suite: ExpectationSuite = context.get_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert isinstance(suite, ExpectationSuite)

    # Actual testing really starts here
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "edit",
            f"{expectation_suite_name}",
            "--interactive",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    stdout = result.stdout
    assert "A batch of data is required to edit the suite" not in stdout
    assert "Select a datasource" not in stdout

    expected_notebook_path: str = os.path.join(
        uncommitted_dir, f"edit_{expectation_suite_name}.ipynb"
    )
    assert os.path.isfile(expected_notebook_path)

    expected_suite_path: str = os.path.join(
        project_dir, "expectations", f"{expectation_suite_name}.json"
    )
    assert os.path.isfile(expected_suite_path)

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string=batch_request_string,
    )
    assert len(cells_of_interest_dict) == 1

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string="context.open_data_docs(resource_identifier=suite_identifier)",
    )
    assert not cells_of_interest_dict

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string="context.open_data_docs(resource_identifier=validation_result_identifier)",
    )
    assert len(cells_of_interest_dict) == 1

    run_notebook(
        notebook_path=expected_notebook_path,
        notebook_dir=uncommitted_dir,
        string_to_be_replaced="context.open_data_docs(resource_identifier=validation_result_identifier)",
        replacement_string="",
    )

    assert mock_webbrowser.call_count == 0
    assert mock_subprocess.call_count == 1

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_interactive_batch_request_without_datasource_json_file_raises_helpful_error(
    mock_webbroser,
    mock_subprocess,
    caplog,
    monkeypatch,
    empty_data_context,
):
    context: DataContext = empty_data_context
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    # noinspection PyUnusedLocal
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    assert (
        context.list_expectation_suites()[0].expectation_suite_name
        == expectation_suite_name
    )

    batch_request: dict = {
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    batch_request_file_path: str = os.path.join(
        uncommitted_dir, f"batch_request_missing_datasource.json"
    )
    with open(batch_request_file_path, "w") as json_file:
        json.dump(batch_request, json_file)

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"""--v3-api suite edit {expectation_suite_name} --interactive --batch-request
{batch_request_file_path} --no-jupyter
""",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "A batch of data is required to edit the suite" not in stdout
    assert "Select a datasource" not in stdout
    assert (
        "Please check that your batch_request is valid and is able to load a batch."
        in stdout
    )
    assert 'The type of an datasource name must be a string (Python "str").' in stdout

    assert mock_subprocess.call_count == 0

    assert mock_webbroser.call_count == 0

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def test_suite_list_with_zero_suites(caplog, monkeypatch, empty_data_context):
    context: DataContext = empty_data_context
    config_file_path: str = os.path.join(
        context.root_directory, "great_expectations.yml"
    )
    assert os.path.exists(config_file_path)

    monkeypatch.chdir(os.path.dirname(context.root_directory))
    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite list",
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    assert "No Expectation Suites found" in result.output

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def test_suite_list_with_one_suite(caplog, monkeypatch, empty_data_context):
    context: DataContext = empty_data_context
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory

    config_file_path: str = os.path.join(project_dir, "great_expectations.yml")
    assert os.path.exists(config_file_path)

    expectation_suite_dir_name: str = "a_dir"
    expectation_suite_name: str = "test_suite_name"

    # noinspection PyUnusedLocal
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=f"{expectation_suite_dir_name}.{expectation_suite_name}"
    )

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "1 Expectation Suite found" in stdout
    assert f"{expectation_suite_dir_name}.{expectation_suite_name}" in stdout

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


def test_suite_list_with_multiple_suites(caplog, monkeypatch, empty_data_context):
    context: DataContext = empty_data_context
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory

    # noinspection PyUnusedLocal
    suite_0: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name="a.warning"
    )
    # noinspection PyUnusedLocal
    suite_1: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name="b.warning"
    )
    # noinspection PyUnusedLocal
    suite_2: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name="c.warning"
    )

    config_file_path: str = os.path.join(project_dir, "great_expectations.yml")
    assert os.path.exists(config_file_path)

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "3 Expectation Suites found:" in stdout
    assert "a.warning" in stdout
    assert "b.warning" in stdout
    assert "c.warning" in stdout

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_delete_with_zero_suites(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite delete not_a_suite",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "No expectation suites found in the project" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.suite.delete",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_delete_with_non_existent_suite(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    expectation_suite_name: str = "test_suite_name"

    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    context.save_expectation_suite(expectation_suite=suite)
    assert (
        context.list_expectation_suites()[0].expectation_suite_name
        == expectation_suite_name
    )

    mock_emit.reset_mock()

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite delete not_a_suite",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "No expectation suite named not_a_suite found" in stdout

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.suite.delete",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_delete_with_one_suite(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = empty_data_context_stats_enabled.root_directory

    expectation_suite_dir_name: str = "a_dir"
    expectation_suite_name: str = "test_suite_name"

    # noinspection PyUnusedLocal
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=f"{expectation_suite_dir_name}.{expectation_suite_name}"
    )
    context.save_expectation_suite(expectation_suite=suite)
    assert (
        context.list_expectation_suites()[0].expectation_suite_name
        == f"{expectation_suite_dir_name}.{expectation_suite_name}"
    )

    mock_emit.reset_mock()

    suite_dir: str = os.path.join(
        project_dir, "expectations", expectation_suite_dir_name
    )
    suite_path: str = os.path.join(suite_dir, f"{expectation_suite_name}.json")
    assert os.path.isfile(suite_path)

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite delete {expectation_suite_dir_name}.{expectation_suite_name}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert (
        f"Deleted the expectation suite named: {expectation_suite_dir_name}.{expectation_suite_name}"
        in stdout
    )

    assert not os.path.isfile(suite_path)

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.suite.delete",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_delete_with_one_suite_assume_yes_flag(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory

    expectation_suite_dir_name: str = "a_dir"
    expectation_suite_name: str = "test_suite_name"

    # noinspection PyUnusedLocal
    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=f"{expectation_suite_dir_name}.{expectation_suite_name}"
    )
    context.save_expectation_suite(expectation_suite=suite)
    assert (
        context.list_expectation_suites()[0].expectation_suite_name
        == f"{expectation_suite_dir_name}.{expectation_suite_name}"
    )

    mock_emit.reset_mock()

    suite_dir: str = os.path.join(
        project_dir, "expectations", expectation_suite_dir_name
    )
    suite_path: str = os.path.join(suite_dir, f"{expectation_suite_name}.json")
    assert os.path.isfile(suite_path)

    runner: CliRunner = CliRunner(mix_stderr=False)
    result: Result = runner.invoke(
        cli,
        f"--v3-api --assume-yes suite delete {expectation_suite_dir_name}.{expectation_suite_name}",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert (
        f"Deleted the expectation suite named: {expectation_suite_dir_name}.{expectation_suite_name}"
        in stdout
    )

    assert "Would you like to proceed? [Y/n]:" not in stdout
    # This assertion is extra assurance since this test is too permissive if we change the confirmation message
    assert "[Y/n]" not in stdout

    assert not os.path.isfile(suite_path)

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.suite.delete",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )

    result = runner.invoke(
        cli,
        f"--v3-api suite list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout = result.stdout
    assert result.exit_code == 0
    assert "No Expectation Suites found" in stdout


# TODO: <Alex>ALEX</Alex>
@pytest.mark.xfail(
    reason="TODO: <Alex>ALEX: This command is not yet implemented for the modern API</Alex>",
    run=True,
    strict=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_suite_scaffold_on_context_with_no_datasource_raises_error(
    mock_subprocess, mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    """
    We call the "suite scaffold" command on a context with no datasource

    The command should:
    - exit with a clear error message
    - send a DataContext init success message
    - send a scaffold fail message
    """
    context = empty_data_context_stats_enabled
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "scaffold",
            "foo",
        ],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert (
        "No datasources found in the context. To add a datasource, run `great_expectations datasource new`"
        in stdout
    )

    assert mock_subprocess.call_count == 0
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


# TODO: <Alex>ALEX</Alex>
@pytest.mark.xfail(
    reason="TODO: <Alex>ALEX: This command is not yet implemented for the modern API</Alex>",
    run=True,
    strict=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_scaffold_on_existing_suite_raises_error(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    """
    We call the "suite scaffold" command with an existing suite

    The command should:
    - exit with a clear error message
    - send a DataContext init success message
    - send a scaffold fail message
    """
    context = empty_data_context_stats_enabled
    suite = context.create_expectation_suite("foo")
    context.save_expectation_suite(expectation_suite=suite)
    assert context.list_expectation_suite_names() == ["foo"]
    mock_emit.reset_mock()

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "scaffold",
            "foo",
        ],
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 1
    assert "An expectation suite named `foo` already exists." in stdout
    assert (
        "If you intend to edit the suite please use `great_expectations suite edit foo`."
        in stdout
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": False}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


# TODO: <Alex>ALEX</Alex>
@pytest.mark.xfail(
    reason="TODO: <Alex>ALEX: This command is not yet implemented for the modern API</Alex>",
    run=True,
    strict=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_suite_scaffold_creates_notebook_and_opens_jupyter(
    mock_subprocess, mock_emit, caplog, monkeypatch, titanic_data_context_stats_enabled
):
    """
    We call the "suite scaffold" command

    The command should:
    - create a new notebook
    - open the notebook in jupyter
    - send a DataContext init success message
    - send a scaffold success message
    """
    context = titanic_data_context_stats_enabled
    suite_name = "foo"
    expected_notebook_path = os.path.join(
        context.root_directory,
        context.GE_EDIT_NOTEBOOK_DIR,
        f"scaffold_{suite_name}.ipynb",
    )
    assert not os.path.isfile(expected_notebook_path)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "scaffold",
            suite_name,
        ],
        input="1\n1\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert os.path.isfile(expected_notebook_path)

    assert mock_subprocess.call_count == 1
    assert mock_subprocess.call_args_list == [
        mock.call(["jupyter", "notebook", expected_notebook_path])
    ]
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": True}
        ),
    ]
    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
        allowed_deprecation_message=VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    )


# TODO: <Alex>ALEX</Alex>
@pytest.mark.xfail(
    reason="TODO: <Alex>ALEX: This command is not yet implemented for the modern API</Alex>",
    run=True,
    strict=True,
)
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_suite_scaffold_creates_notebook_with_no_jupyter_flag(
    mock_subprocess, mock_emit, caplog, monkeypatch, titanic_data_context_stats_enabled
):
    """
    We call the "suite scaffold --no-jupyter"

    The command should:
    - create a new notebook
    - NOT open the notebook in jupyter
    - tell the user to open the notebook
    - send a DataContext init success message
    - send a scaffold success message
    """
    context = titanic_data_context_stats_enabled
    suite_name = "foo"
    expected_notebook_path = os.path.join(
        context.root_directory,
        context.GE_EDIT_NOTEBOOK_DIR,
        f"scaffold_{suite_name}.ipynb",
    )
    assert not os.path.isfile(expected_notebook_path)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "scaffold",
            suite_name,
            "--no-jupyter",
        ],
        input="1\n1\n",
        catch_exceptions=False,
    )
    stdout = result.output
    assert result.exit_code == 0
    assert os.path.isfile(expected_notebook_path)
    assert (
        f"To continue scaffolding this suite, run `jupyter notebook {expected_notebook_path}`"
        in stdout
    )

    assert mock_subprocess.call_count == 0
    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {"event": "cli.suite.scaffold", "event_payload": {}, "success": True}
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
        allowed_deprecation_message=VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    )
