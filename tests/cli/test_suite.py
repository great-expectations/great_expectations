import json
import os
from typing import Any, Dict, List, Tuple
from unittest import mock

import pytest
from _pytest.capture import CaptureResult
from click.testing import CliRunner, Result
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.cli.suite import (
    _process_suite_edit_flags_and_prompt,
    _process_suite_new_flags_and_prompt,
    _suite_new_workflow,
)
from great_expectations.core import ExpectationConfiguration
from great_expectations.core.batch import (
    BatchRequest,
    standardize_batch_request_display_ordering,
)
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.usage_statistics.anonymizers.types.base import (
    CLISuiteInteractiveFlagCombinations,
)
from great_expectations.util import deep_filter_properties_iterable, lint_code
from tests.cli.utils import assert_no_logging_messages_or_tracebacks
from tests.render.test_util import (
    find_code_in_notebook,
    load_notebook_from_path,
    run_notebook,
)

yaml = YAML(typ="safe")


def test_suite_help_output(caplog):
    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
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
  delete  Delete an Expectation Suite from the Expectation Store.
  demo    This command is not supported in the v3 (Batch Request) API.
  edit    Edit an existing Expectation Suite.
  list    List existing Expectation Suites.
  new     Create a new Expectation Suite.
"""
        in stdout
    )
    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_demo_deprecation_message(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite demo",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "This command is not supported in the v3 (Batch Request) API." in stdout

    expected_call_args_list = [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.demo.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.demo.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_prompted_default_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
    # noinspection PyTypeChecker
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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": False,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_prompted_custom_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite new",
        input=f"1\n{expectation_suite_name}\n",
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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": False,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_arg_custom_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
    # noinspection PyTypeChecker
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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": False,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_non_interactive_with_suite_name_arg_custom_runs_notebook_no_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
    # noinspection PyTypeChecker
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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": False,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_interactive_nonexistent_batch_request_json_file_raises_error(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context: DataContext = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory

    expectation_suite_name: str = "test_suite_name"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_interactive_malformed_batch_request_json_file_raises_error(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
    # noinspection PyTypeChecker
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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_interactive_valid_batch_request_from_json_file_in_notebook_runs_notebook_no_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
    # noinspection PyTypeChecker
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

    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 4
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
def test_suite_edit_without_suite_name_raises_error(
    mock_emit,
    monkeypatch,
    empty_data_context_stats_enabled,
):
    """This is really only testing click missing arguments"""
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(cli, "--v3-api suite edit", catch_exceptions=False)
    assert result.exit_code == 2

    assert (
        'Error: Missing argument "EXPECTATION_SUITE".' in result.stderr
        or "Error: Missing argument 'EXPECTATION_SUITE'." in result.stderr
    )

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_suite_edit_datasource_and_batch_request_error(
    mock_emit,
    monkeypatch,
    empty_data_context_stats_enabled,
):
    """This is really only testing click missing arguments"""
    context: DataContext = empty_data_context_stats_enabled
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
    # noinspection PyTypeChecker
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

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "interactive_flag": None,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.ERROR_DATASOURCE_SPECIFIED_BATCH_REQUEST_SPECIFIED.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": False,
            }
        ),
    ]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_non_existent_suite_name_raises_error(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    empty_data_context_stats_enabled,
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    context: DataContext = empty_data_context_stats_enabled

    assert not context.list_expectation_suites()

    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite edit not_a_real_suite",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "Could not find a suite named `not_a_real_suite`." in stdout
    assert "by running `great_expectations suite list`" in stdout

    assert mock_subprocess.call_count == 0

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_with_non_existent_datasource_shows_helpful_error_message(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    empty_data_context_stats_enabled,
):
    """
    The command should:
    - exit with a clear error message
    - NOT open Data Docs
    - NOT open jupyter
    """
    context: DataContext = empty_data_context_stats_enabled
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
    # noinspection PyTypeChecker
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

    assert mock_subprocess.call_count == 0

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_DATASOURCE_SPECIFIED.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_no_additional_args_without_citations_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
        "limit": 1000,
    }
    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
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
    # noinspection PyTypeChecker
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "edit",
            f"{expectation_suite_name}",
            "--interactive",
        ],
        input="1\n1\n1\n1\n",
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

    assert mock_subprocess.call_count == 1

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 9
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.save_expectation_suite",
                "event_payload": {
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe"
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.save_expectation_suite",
                "event_payload": {
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe"
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe",
                    "api_version": "v3",
                },
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_no_additional_args_with_citations_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
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
    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
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
        input="1\n1\n1\n\n",
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
    # noinspection PyTypeChecker
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

    assert mock_subprocess.call_count == 1

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 8
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.save_expectation_suite",
                "event_payload": {
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe"
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe",
                },
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_sql_with_no_additional_args_without_citations_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    Here we verify that the "suite edit" command helps the user to specify batch_request
    when it is called without the optional command-line arguments that specify the batch.

    First, we call the "suite new" command to create the expectation suite our test
    will edit -- this step is a just a setup (we use an SQL datasource for this test).

    We then call the "suite edit" command without any optional command-line arguments.  This means
    that the command will help us specify batch_request interactively.

    The data context has two datasources -- we choose one of them.
    After that, we select a data connector and, finally, select a data asset from the list.

    The command should:
    - NOT open Data Docs
    - open jupyter
    """
    context: DataContext = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    batch_request: dict = {
        "datasource_name": "my_sqlite_db_datasource",
        "data_connector_name": "default_inferred_data_connector_name",
        "data_asset_name": "titanic",
        "limit": 1000,
    }
    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
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
        input="3\n2\ny\n2\n",
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
    # noinspection PyTypeChecker
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "edit",
            f"{expectation_suite_name}",
            "--interactive",
        ],
        input="2\n2\n2\n1\n",
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

    assert mock_subprocess.call_count == 1

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 10
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.save_expectation_suite",
                "event_payload": {
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe"
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.save_expectation_suite",
                "event_payload": {
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe"
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.save_expectation_suite",
                "event_payload": {
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe"
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe",
                    "api_version": "v3",
                },
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_multiple_datasources_with_sql_with_no_additional_args_with_citations_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    Here we verify that the "suite edit" command uses the batch_request found in
    citations in the existing suite when it is called without the optional
    command-line arguments that specify the batch.

    First, we call the "suite new" command to create the expectation suite our
    test will edit -- this step is a just a setup (we use an SQL datasource for this test).

    We then call the "suite edit" command without any optional command-line-arguments.

    The command should:
    - NOT open Data Docs
    - NOT open jupyter
    """
    context: DataContext = titanic_v013_multi_datasource_multi_execution_engine_data_context_with_checkpoints_v1_with_empty_store_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    batch_request: dict = {
        "datasource_name": "my_sqlite_db_datasource",
        "data_connector_name": "default_inferred_data_connector_name",
        "data_asset_name": "titanic",
        "limit": 1000,
    }
    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
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
        input="2\n2\ny\n2\n",
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
    # noinspection PyTypeChecker
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

    assert mock_subprocess.call_count == 1

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 8
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.save_expectation_suite",
                "event_payload": {
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe"
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE.value[
                        "interactive_attribution"
                    ],
                    "anonymized_expectation_suite_name": "9df638a13b727807e51b13ec1839bcbe",
                    "api_version": "v3",
                },
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_edit_interactive_batch_request_without_datasource_json_file_raises_helpful_error(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    empty_data_context_stats_enabled,
):
    context: DataContext = empty_data_context_stats_enabled
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
        "datasource_name": None,
    }

    batch_request_file_path: str = os.path.join(
        uncommitted_dir, f"batch_request_missing_datasource.json"
    )
    with open(batch_request_file_path, "w") as json_file:
        json.dump(batch_request, json_file)

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
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

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.edit.end",
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
def test_suite_list_with_zero_suites(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
    config_file_path: str = os.path.join(
        context.root_directory, "great_expectations.yml"
    )
    assert os.path.exists(config_file_path)

    monkeypatch.chdir(os.path.dirname(context.root_directory))
    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite list",
        catch_exceptions=False,
    )

    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "No Expectation Suites found" in stdout

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.list.end",
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
def test_suite_list_with_one_suite(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
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
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert "1 Expectation Suite found" in stdout
    assert f"{expectation_suite_dir_name}.{expectation_suite_name}" in stdout

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.list.end",
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
def test_suite_list_with_multiple_suites(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
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
    # noinspection PyTypeChecker
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

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.list.end",
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
def test_suite_delete_with_zero_suites(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite delete not_a_suite",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "No expectation suites found in the project" in stdout

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.end",
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
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite delete not_a_suite",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.stdout
    assert "No expectation suite named not_a_suite found" in stdout

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.end",
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
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite delete {expectation_suite_dir_name}.{expectation_suite_name}",
        input="\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert (
        f"Deleted the expectation suite named: {expectation_suite_dir_name}.{expectation_suite_name}"
        in stdout
    )

    assert not os.path.isfile(suite_path)

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.end",
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
def test_suite_delete_canceled_with_one_suite(
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
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        f"--v3-api suite delete {expectation_suite_dir_name}.{expectation_suite_name}",
        input="n\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout: str = result.stdout
    assert (
        f"Suite `{expectation_suite_dir_name}.{expectation_suite_name}` was not deleted"
        in stdout
    )

    assert os.path.isfile(suite_path)

    assert mock_emit.call_count == 2
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.begin",
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
    # noinspection PyTypeChecker
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

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.delete.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
    )

    # noinspection PyTypeChecker
    result = runner.invoke(
        cli,
        f"--v3-api suite list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0

    stdout = result.stdout
    assert result.exit_code == 0
    assert "No Expectation Suites found" in stdout


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_suite_new_profile_on_context_with_no_datasource_raises_error(
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    empty_data_context_stats_enabled,
):
    """
    We call the "suite new --profile" command on a context with no datasource

    The command should:
    - exit with a clear error message
    - send a DataContext init success message
    - send a new fail message
    """
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    expectation_suite_name: str = "test_suite_name"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--interactive",
            "--profile",
            "--expectation-suite",
            f"{expectation_suite_name}",
        ],
        input="\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.output
    assert (
        "No datasources found in the context. To add a datasource, run `great_expectations datasource new`"
        in stdout
    )

    assert mock_subprocess.call_count == 0

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
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
def test_suite_new_profile_on_existing_suite_raises_error(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled
):
    """
    We call the "suite new --profile" command with an existing suite

    The command should:
    - exit with a clear error message
    - send a DataContext init success message
    - send a new fail message
    """
    context: DataContext = empty_data_context_stats_enabled
    monkeypatch.chdir(os.path.dirname(context.root_directory))

    project_dir: str = context.root_directory
    uncommitted_dir: str = os.path.join(project_dir, "uncommitted")

    expectation_suite_name: str = "test_suite_name"

    suite: ExpectationSuite = context.create_expectation_suite(
        expectation_suite_name=expectation_suite_name
    )
    context.save_expectation_suite(expectation_suite=suite)
    assert (
        context.list_expectation_suites()[0].expectation_suite_name
        == expectation_suite_name
    )

    batch_request: dict = {
        "datasource_name": "my_datasource",
        "data_connector_name": "my_basic_data_connector",
        "data_asset_name": "Titanic_1911",
    }

    batch_request_file_path: str = os.path.join(uncommitted_dir, f"batch_request.json")
    with open(batch_request_file_path, "w") as json_file:
        json.dump(batch_request, json_file)

    mock_emit.reset_mock()

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--expectation-suite",
            f"{expectation_suite_name}",
            "--interactive",
            "--batch-request",
            f"{batch_request_file_path}",
            "--profile",
            "--no-jupyter",
        ],
        input="\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 1

    stdout: str = result.output
    assert (
        f"An expectation suite named `{expectation_suite_name}` already exists."
        in stdout
    )
    assert (
        f"If you intend to edit the suite please use `great_expectations suite edit {expectation_suite_name}`."
        in stdout
    )

    assert mock_emit.call_count == 3
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_profile_runs_notebook_no_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    We call the "suite new --profile" command

    The command should:
    - create a new notebook
    - send a DataContext init success message
    - send a new success message
    """
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

    mock_emit.reset_mock()

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--expectation-suite",
            f"{expectation_suite_name}",
            "--interactive",
            "--batch-request",
            f"{batch_request_file_path}",
            "--profile",
            "--no-jupyter",
        ],
        input="\n",
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

    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

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

    profiler_code_cell: str = f"""\
profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=None,
    ignored_columns=ignored_columns,
    not_null_only=False,
    primary_or_compound_key=False,
    semantic_types_dict=None,
    table_expectations_only=False,
    value_set_threshold="MANY",
)
suite = profiler.build_suite()"""
    profiler_code_cell = lint_code(code=profiler_code_cell).rstrip("\n")

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string=profiler_code_cell,
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
    assert suite.expectations == [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": [
                        "Unnamed: 0",
                        "Name",
                        "PClass",
                        "Age",
                        "Sex",
                        "Survived",
                        "SexCode",
                    ]
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"max_value": 1313, "min_value": 1313},
                "meta": {},
            }
        ),
    ]

    assert mock_subprocess.call_count == 0

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 5
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_PROFILE_TRUE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.get_batch_list",
                "event_payload": {
                    "anonymized_batch_request_required_top_level_properties": {
                        "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                        "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                        "anonymized_data_asset_name": "38b9086d45a8746d014a0d63ad58e331",
                    }
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_profile_runs_notebook_opens_jupyter(
    mock_webbrowser,
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    """
    We call the "suite new --profile" command

    The command should:
    - create a new notebook
    - open the notebook in jupyter
    - send a DataContext init success message
    - send a new success message
    """
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

    mock_emit.reset_mock()

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--expectation-suite",
            f"{expectation_suite_name}",
            "--interactive",
            "--batch-request",
            f"{batch_request_file_path}",
            "--profile",
        ],
        input="\n",
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

    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

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

    profiler_code_cell: str = f"""\
profiler = UserConfigurableProfiler(
    profile_dataset=validator,
    excluded_expectations=None,
    ignored_columns=ignored_columns,
    not_null_only=False,
    primary_or_compound_key=False,
    semantic_types_dict=None,
    table_expectations_only=False,
    value_set_threshold="MANY",
)
suite = profiler.build_suite()"""
    profiler_code_cell = lint_code(code=profiler_code_cell).rstrip("\n")

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string=profiler_code_cell,
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
    assert suite.expectations == [
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_columns_to_match_ordered_list",
                "kwargs": {
                    "column_list": [
                        "Unnamed: 0",
                        "Name",
                        "PClass",
                        "Age",
                        "Sex",
                        "Survived",
                        "SexCode",
                    ]
                },
                "meta": {},
            }
        ),
        ExpectationConfiguration(
            **{
                "expectation_type": "expect_table_row_count_to_be_between",
                "kwargs": {"max_value": 1313, "min_value": 1313},
                "meta": {},
            }
        ),
    ]

    assert mock_subprocess.call_count == 1
    call_args: List[str] = mock_subprocess.call_args[0][0]
    assert call_args[0] == "jupyter"
    assert call_args[1] == "notebook"
    assert expected_notebook_path in call_args[2]

    assert mock_webbrowser.call_count == 0

    assert mock_emit.call_count == 5
    assert mock_emit.call_args_list == [
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.suite.new.end",
                "event_payload": {
                    "interactive_flag": True,
                    "interactive_attribution": CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_PROFILE_TRUE.value[
                        "interactive_attribution"
                    ],
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.get_batch_list",
                "event_payload": {
                    "anonymized_batch_request_required_top_level_properties": {
                        "anonymized_datasource_name": "a732a247720783a5931fa7c4606403c2",
                        "anonymized_data_connector_name": "af09acd176f54642635a8a2975305437",
                        "anonymized_data_asset_name": "38b9086d45a8746d014a0d63ad58e331",
                    }
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "data_context.__init__",
                "event_payload": {},
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
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_profile_with_named_arg_runs_notebook_no_jupyter(
    mock_webbrowser: mock.MagicMock,
    mock_subprocess: mock.MagicMock,
    mock_emit: mock.MagicMock,
    monkeypatch: Any,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled: DataContext,
):
    """
    We call the "suite new --profile" command with a named profiler argument

    The command should create a new notebook
    """
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

    mock_emit.reset_mock()

    profiler_name: str = "my_profiler"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--expectation-suite",
            f"{expectation_suite_name}",
            "--interactive",
            "--batch-request",
            f"{batch_request_file_path}",
            "--profile",
            f"{profiler_name}",
            "--no-jupyter",
        ],
        input="\n",
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

    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

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

    profiler_code_cell: str = f"""\
suite = context.run_profiler_with_dynamic_arguments(
    name="{profiler_name}",
    expectation_suite=validator.expectation_suite
)
"""
    profiler_code_cell = lint_code(code=profiler_code_cell).rstrip("\n")

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string=profiler_code_cell,
    )
    assert len(cells_of_interest_dict) == 1


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@mock.patch("webbrowser.open", return_value=True, side_effect=None)
def test_suite_new_profile_with_named_arg_runs_notebook_opens_jupyter(
    mock_webbrowser: mock.MagicMock,
    mock_subprocess: mock.MagicMock,
    mock_emit: mock.MagicMock,
    monkeypatch: Any,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled: DataContext,
):
    """
    We call the "suite new --profile" command with a named profiler argument

    The command should create a new notebook and open it in Jupyter
    """
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

    mock_emit.reset_mock()

    profiler_name: str = "my_profiler"

    runner: CliRunner = CliRunner(mix_stderr=False)
    # noinspection PyTypeChecker
    result: Result = runner.invoke(
        cli,
        [
            "--v3-api",
            "suite",
            "new",
            "--expectation-suite",
            f"{expectation_suite_name}",
            "--interactive",
            "--batch-request",
            f"{batch_request_file_path}",
            "--profile",
            f"{profiler_name}",
        ],
        input="\n",
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

    batch_request_obj: BatchRequest = BatchRequest(**batch_request)
    batch_request = deep_filter_properties_iterable(
        properties=batch_request_obj.to_json_dict(),
    )
    batch_request = standardize_batch_request_display_ordering(
        batch_request=batch_request
    )
    batch_request_string: str = (
        str(batch_request)
        .replace("{", "{\n    ")
        .replace(", ", ",\n    ")
        .replace("}", ",\n}")
        .replace("'", '"')
    )
    batch_request_string = rf"batch_request = {batch_request_string}"

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

    profiler_code_cell: str = f"""\
suite = context.run_profiler_with_dynamic_arguments(
    name="{profiler_name}",
    expectation_suite=validator.expectation_suite
)
"""
    profiler_code_cell = lint_code(code=profiler_code_cell).rstrip("\n")

    cells_of_interest_dict: Dict[int, dict] = find_code_in_notebook(
        nb=load_notebook_from_path(notebook_path=expected_notebook_path),
        search_string=profiler_code_cell,
    )
    assert len(cells_of_interest_dict) == 1


@pytest.fixture
def suite_new_messages():
    return {
        "no_msg": "",
        "happy_path_profile": "Entering interactive mode since you passed the --profile flag",
        "warning_profile": "Warning: Ignoring the --manual flag and entering interactive mode since you passed the --profile flag",
        "happy_path_batch_request": "Entering interactive mode since you passed the --batch-request flag",
        "warning_batch_request": "Warning: Ignoring the --manual flag and entering interactive mode since you passed the --batch-request flag",
        "happy_path_prompt_call": """
How would you like to create your Expectation Suite?
    1. Manually, without interacting with a sample batch of data (default)
    2. Interactively, with a sample batch of data
    3. Automatically, using a profiler
""",
        "error_both_interactive_flags": "Please choose either --interactive or --manual, you may not choose both.",
    }


@pytest.mark.parametrize(
    "interactive_flag,manual_flag,profile_flag,batch_request_flag,error_expected,prompt_input,return_interactive,return_profile,stdout_fixture,stderr_fixture",
    [
        # No error expected
        # return_interactive = True, return_profile = False
        pytest.param(
            True,
            False,
            False,
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE,
            False,
            "no_msg",
            "no_msg",
            id="--interactive",
        ),
        # return_interactive = False, return_profile = False
        pytest.param(
            False,
            True,
            False,
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_FALSE_MANUAL_TRUE,
            False,
            "no_msg",
            "no_msg",
            id="--manual",
        ),
        # return_interactive = True, return_profile = True
        pytest.param(
            False,
            False,
            True,
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_PROFILE_TRUE,
            True,
            "no_msg",
            "no_msg",
            id="--profile",
        ),
        pytest.param(
            True,
            False,
            True,
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_PROFILE_TRUE,
            True,
            "no_msg",
            "no_msg",
            id="--interactive --profile",
        ),
        # batch_request not empty
        pytest.param(
            True,
            False,
            False,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE,
            False,
            "no_msg",
            "no_msg",
            id="--interactive --batch-request",
        ),
        pytest.param(
            False,
            False,
            True,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_PROFILE_TRUE,
            True,
            "happy_path_profile",
            "no_msg",
            id="--profile --batch-request",
        ),
        pytest.param(
            True,
            False,
            True,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_PROFILE_TRUE,
            True,
            "no_msg",
            "no_msg",
            id="--interactive --profile --batch-request",
        ),
        # Prompts
        # Just hit enter (default choice)
        pytest.param(
            False,
            False,
            False,
            None,
            False,
            "",
            CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_DEFAULT,
            False,
            "no_msg",
            "no_msg",
            id="prompt: Default Choice 1 - Manual suite creation (default)",
        ),
        # Choice 1 - Manual suite creation (default)
        pytest.param(
            False,
            False,
            False,
            None,
            False,
            "1",
            CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE,
            False,
            "no_msg",
            "no_msg",
            id="prompt: Choice 1 - Manual suite creation (default)",
        ),
        # Choice 2 - Interactive suite creation
        pytest.param(
            False,
            False,
            False,
            None,
            False,
            "2",
            CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_TRUE_PROFILE_FALSE,
            False,
            "no_msg",
            "no_msg",
            id="prompt: Choice 2 - Interactive suite creation",
        ),
        # Choice 3 - Automatic suite creation (profiler)
        pytest.param(
            False,
            False,
            False,
            None,
            False,
            "3",
            CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_TRUE_PROFILE_TRUE,
            True,
            "no_msg",
            "no_msg",
            id="prompt: Choice 3 - Automatic suite creation (profiler)",
        ),
        # No error but warning expected
        # no-interactive flag with batch_request, with/without profile flag
        pytest.param(
            False,
            True,
            False,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_BATCH_REQUEST_SPECIFIED,
            False,
            "warning_batch_request",
            "no_msg",
            id="warning: --manual --batch-request",
        ),
        pytest.param(
            False,
            True,
            True,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_PROFILE_TRUE,
            True,
            "warning_profile",
            "no_msg",
            id="warning: --manual --profile --batch-request",
        ),
        # no-interactive flag with profile and without batch request flag
        pytest.param(
            False,
            True,
            True,
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_PROFILE_TRUE,
            True,
            "warning_profile",
            "no_msg",
            id="warning: --manual --profile",
        ),
        # Yes error expected
        # both interactive flags, profile=False, with/without batch_request
        pytest.param(
            True,
            True,
            False,
            None,
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE,
            None,
            "error_both_interactive_flags",
            "no_msg",
            id="error: --interactive --manual",
        ),
        pytest.param(
            True,
            True,
            False,
            "batch_request.json",
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE,
            None,
            "error_both_interactive_flags",
            "no_msg",
            id="error: --interactive --manual --batch-request",
        ),
        # both interactive flags, profile=True, with/without batch_request
        pytest.param(
            True,
            True,
            True,
            None,
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE,
            None,
            "error_both_interactive_flags",
            "no_msg",
            id="error: --interactive --manual --profile",
        ),
        pytest.param(
            True,
            True,
            True,
            "batch_request.json",
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE,
            None,
            "error_both_interactive_flags",
            "no_msg",
            id="error: --interactive --manual --profile --batch-request",
        ),
    ],
)
@mock.patch("click.prompt")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test__process_suite_new_flags_and_prompt(
    mock_emit,
    mock_prompt,
    interactive_flag,
    manual_flag,
    profile_flag,
    batch_request_flag,
    error_expected,
    prompt_input,
    return_interactive,
    return_profile,
    stdout_fixture,
    stderr_fixture,
    empty_data_context_stats_enabled,
    capsys,
    suite_new_messages,
):
    """
    What does this test and why?
    _process_suite_new_flags_and_prompt should return the correct configuration or error based on input flags.
    """

    usage_event_end: str = "cli.suite.new.end"
    context: DataContext = empty_data_context_stats_enabled

    # test happy paths
    if not error_expected:
        if prompt_input is not None:
            mock_prompt.side_effect = [prompt_input]
        processed_flags: Tuple[
            CLISuiteInteractiveFlagCombinations, bool
        ] = _process_suite_new_flags_and_prompt(
            context=context,
            usage_event_end=usage_event_end,
            interactive_flag=interactive_flag,
            manual_flag=manual_flag,
            profile=profile_flag,
            batch_request=batch_request_flag,
        )
        assert processed_flags == (
            return_interactive,
            return_profile,
        )
        # Note - in this method on happy path no usage stats message is sent. Other messages are sent during the full
        #  CLI suite new flow of creating a notebook etc.
        assert mock_emit.call_count == 0
        assert mock_emit.call_args_list == []

        # Check output
        captured: CaptureResult = capsys.readouterr()
        assert suite_new_messages[stdout_fixture] in captured.out
        assert suite_new_messages[stderr_fixture] in captured.err

        # Check prompt text and called only when appropriate
        if prompt_input is not None:
            assert mock_prompt.call_count == 1
            assert (
                mock_prompt.call_args_list[0][0][0]
                == suite_new_messages["happy_path_prompt_call"]
            )
        else:
            assert mock_prompt.call_count == 0

    # test error cases
    elif error_expected:
        with pytest.raises(SystemExit):
            _ = _process_suite_new_flags_and_prompt(
                context=context,
                usage_event_end=usage_event_end,
                interactive_flag=interactive_flag,
                manual_flag=manual_flag,
                profile=profile_flag,
                batch_request=batch_request_flag,
            )

        # Check output
        captured: CaptureResult = capsys.readouterr()
        assert suite_new_messages[stdout_fixture] in captured.out
        assert suite_new_messages[stderr_fixture] in captured.err
        assert mock_prompt.call_count == 0

        # Note - in this method only a single usage stats message is sent. Other messages are sent during the full
        #  CLI suite new flow of creating a notebook etc.
        assert mock_emit.call_count == 1
        assert mock_emit.call_args_list == [
            mock.call(
                {
                    "event": usage_event_end,
                    "event_payload": {
                        "interactive_flag": None,
                        "interactive_attribution": return_interactive.value[
                            "interactive_attribution"
                        ],
                        "api_version": "v3",
                    },
                    "success": False,
                }
            ),
        ]


@pytest.fixture
def suite_edit_messages():
    return {
        "no_msg": "",
        "happy_path_datasource_name": "Entering interactive mode since you passed the --datasource-name flag",
        "warning_datasource_name": "Warning: Ignoring the --manual flag and entering interactive mode since you passed the --datasource-name flag",
        "happy_path_batch_request": "Entering interactive mode since you passed the --batch-request flag",
        "warning_batch_request": "Warning: Ignoring the --manual flag and entering interactive mode since you passed the --batch-request flag",
        "happy_path_prompt_call": """
How would you like to edit your Expectation Suite?
    1. Manually, without interacting with a sample batch of data (default)
    2. Interactively, with a sample batch of data
""",
        "error_both_interactive_flags": "Please choose either --interactive or --manual, you may not choose both.",
        "error_both_datasource_name_and_batch_request_flags": """Only one of --datasource-name DATASOURCE_NAME and --batch-request <path to JSON file> \
options can be used.
""",
    }


@pytest.mark.parametrize(
    "interactive_flag,manual_flag,datasource_name_flag,batch_request_flag,error_expected,prompt_input,return_interactive,stdout_fixture,stderr_fixture",
    [
        # No error expected
        # return_interactive = True
        pytest.param(
            True,
            False,
            None,
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE,
            "no_msg",
            "no_msg",
            id="--interactive",
        ),
        # return_interactive = False
        pytest.param(
            False,
            True,
            None,
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_FALSE_MANUAL_TRUE,
            "no_msg",
            "no_msg",
            id="--manual",
        ),
        # return_interactive = True, --datasource-name
        pytest.param(
            False,
            False,
            "some_datasource_name",
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_DATASOURCE_SPECIFIED,
            "happy_path_datasource_name",
            "no_msg",
            id="--datasource-name",
        ),
        pytest.param(
            True,
            False,
            "some_datasource_name",
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_TRUE_MANUAL_FALSE_DATASOURCE_SPECIFIED,
            "no_msg",
            "no_msg",
            id="--interactive --datasource-name",
        ),
        # batch_request not empty
        pytest.param(
            True,
            False,
            None,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_INTERACTIVE_TRUE_MANUAL_FALSE,
            "no_msg",
            "no_msg",
            id="--interactive --batch-request",
        ),
        pytest.param(
            False,
            False,
            None,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_FALSE_BATCH_REQUEST_SPECIFIED,
            "happy_path_batch_request",
            "no_msg",
            id="--batch-request",
        ),
        # Prompts
        # Just hit enter (default choice)
        pytest.param(
            False,
            False,
            None,
            None,
            False,
            "",
            CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_DEFAULT,
            "no_msg",
            "no_msg",
            id="prompt: Default Choice 1 - Manual suite edit (default)",
        ),
        # Choice 1 - Manual suite edit (default)
        pytest.param(
            False,
            False,
            None,
            None,
            False,
            "1",
            CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_FALSE,
            "no_msg",
            "no_msg",
            id="prompt: Choice 1 - Manual suite edit (default)",
        ),
        # # Choice 2 - Interactive suite edit
        pytest.param(
            False,
            False,
            None,
            None,
            False,
            "2",
            CLISuiteInteractiveFlagCombinations.PROMPTED_CHOICE_TRUE,
            "no_msg",
            "no_msg",
            id="prompt: Choice 2 - Interactive suite edit",
        ),
        # No error but warning expected
        # no-interactive flag with batch_request
        pytest.param(
            False,
            True,
            None,
            "batch_request.json",
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_BATCH_REQUEST_SPECIFIED,
            "warning_batch_request",
            "no_msg",
            id="warning: --manual --batch-request",
        ),
        # no-interactive flag with datasource_name
        pytest.param(
            False,
            True,
            "some_datasource_name",
            None,
            False,
            None,
            CLISuiteInteractiveFlagCombinations.UNPROMPTED_OVERRIDE_INTERACTIVE_FALSE_MANUAL_TRUE_DATASOURCE_SPECIFIED,
            "warning_datasource_name",
            "no_msg",
            id="warning: --manual --datasource-name",
        ),
        # Yes error expected
        # both interactive flags, datasource_name=None, with/without batch_request
        pytest.param(
            True,
            True,
            None,
            None,
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE,
            "error_both_interactive_flags",
            "no_msg",
            id="error: --interactive --manual",
        ),
        pytest.param(
            True,
            True,
            None,
            "batch_request.json",
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE,
            "error_both_interactive_flags",
            "no_msg",
            id="error: --interactive --manual --batch-request",
        ),
        # both interactive flags, datasource_name=something, with/without batch_request
        pytest.param(
            True,
            True,
            "some_datasource_name",
            None,
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_INTERACTIVE_TRUE_MANUAL_TRUE,
            "error_both_interactive_flags",
            "no_msg",
            id="error: --interactive --manual --datasource-name",
        ),
        pytest.param(
            True,
            True,
            "some_datasource_name",
            "batch_request.json",
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_DATASOURCE_SPECIFIED_BATCH_REQUEST_SPECIFIED,
            "error_both_datasource_name_and_batch_request_flags",
            "no_msg",
            id="error: --interactive --manual --datasource-name --batch-request",
        ),
        # both --datasource-name and --batch-request
        pytest.param(
            False,
            False,
            "some_datasource_name",
            "batch_request.json",
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_DATASOURCE_SPECIFIED_BATCH_REQUEST_SPECIFIED,
            "error_both_datasource_name_and_batch_request_flags",
            "no_msg",
            id="error: --datasource-name --batch-request",
        ),
        pytest.param(
            True,
            False,
            "some_datasource_name",
            "batch_request.json",
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_DATASOURCE_SPECIFIED_BATCH_REQUEST_SPECIFIED,
            "error_both_datasource_name_and_batch_request_flags",
            "no_msg",
            id="--interactive --datasource-name --batch-request",
        ),
        pytest.param(
            False,
            True,
            "some_datasource_name",
            "batch_request.json",
            True,
            None,
            CLISuiteInteractiveFlagCombinations.ERROR_DATASOURCE_SPECIFIED_BATCH_REQUEST_SPECIFIED,
            "error_both_datasource_name_and_batch_request_flags",
            "no_msg",
            id="--manual --datasource-name --batch-request",
        ),
    ],
)
@mock.patch("click.prompt")
@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test__process_suite_edit_flags_and_prompt(
    mock_emit,
    mock_prompt,
    interactive_flag,
    manual_flag,
    datasource_name_flag,
    batch_request_flag,
    error_expected,
    prompt_input,
    return_interactive,
    stdout_fixture,
    stderr_fixture,
    empty_data_context_stats_enabled,
    capsys,
    suite_edit_messages,
):
    """
    What does this test and why?
    _process_suite_edit_flags_and_prompt should return the correct configuration or error based on input flags.
    """

    usage_event_end: str = "cli.suite.edit.end"
    context: DataContext = empty_data_context_stats_enabled

    # test happy paths
    if not error_expected:
        if prompt_input is not None:
            mock_prompt.side_effect = [prompt_input]
        interactive_mode: CLISuiteInteractiveFlagCombinations = (
            _process_suite_edit_flags_and_prompt(
                context=context,
                usage_event_end=usage_event_end,
                interactive_flag=interactive_flag,
                manual_flag=manual_flag,
                datasource_name=datasource_name_flag,
                batch_request=batch_request_flag,
            )
        )
        assert interactive_mode == return_interactive
        # Note - in this method on happy path no usage stats message is sent. Other messages are sent during the full
        #  CLI suite new flow of creating a notebook etc.
        assert mock_emit.call_count == 0
        assert mock_emit.call_args_list == []

        # Check output
        captured: CaptureResult = capsys.readouterr()
        assert suite_edit_messages[stdout_fixture] in captured.out
        assert suite_edit_messages[stderr_fixture] in captured.err

        # Check prompt text and called only when appropriate
        if prompt_input is not None:
            assert mock_prompt.call_count == 1
            assert (
                mock_prompt.call_args_list[0][0][0]
                == suite_edit_messages["happy_path_prompt_call"]
            )
        else:
            assert mock_prompt.call_count == 0

    # test error cases
    elif error_expected:
        with pytest.raises(SystemExit):
            _ = _process_suite_edit_flags_and_prompt(
                context=context,
                usage_event_end=usage_event_end,
                interactive_flag=interactive_flag,
                manual_flag=manual_flag,
                datasource_name=datasource_name_flag,
                batch_request=batch_request_flag,
            )

        # Check output
        captured: CaptureResult = capsys.readouterr()
        assert suite_edit_messages[stdout_fixture] in captured.out
        assert suite_edit_messages[stderr_fixture] in captured.err
        assert mock_prompt.call_count == 0

        # Note - in this method only a single usage stats message is sent. Other messages are sent during the full
        #  CLI suite new flow of creating a notebook etc.
        assert mock_emit.call_count == 1
        assert mock_emit.call_args_list == [
            mock.call(
                {
                    "event": usage_event_end,
                    "event_payload": {
                        "interactive_flag": None,
                        "interactive_attribution": return_interactive.value[
                            "interactive_attribution"
                        ],
                        "api_version": "v3",
                    },
                    "success": False,
                }
            ),
        ]


def test_suite_new_load_jupyter_configured_asset_sql_data_connector_missing_data_asset(
    sqlite_missing_data_asset_data_context,
):
    context: DataContext = sqlite_missing_data_asset_data_context

    interactive_mode, profile = _process_suite_new_flags_and_prompt(
        context=context,
        usage_event_end=None,
        interactive_flag=True,
        manual_flag=False,
        profile=False,
        batch_request=None,
    )

    try:
        _suite_new_workflow(
            context=context,
            expectation_suite_name="test",
            interactive_mode=interactive_mode,
            profile=profile,
            profiler_name=None,
            no_jupyter=True,
            usage_event=None,
            batch_request=None,
        )
    except TypeError:
        pytest.fail(
            "data_asset_name does not need to be required before suite new/edit notebook workflow begins"
        )
