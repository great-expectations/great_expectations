import os
from unittest import mock

import nbformat
from click.testing import CliRunner
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_cli_datasource_list_on_project_with_no_datasources(
    caplog, monkeypatch, empty_data_context, filesystem_csv_2
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource list",
        catch_exceptions=False,
    )

    stdout = result.stdout.strip()
    assert "No Datasources found" in stdout
    assert context.list_datasources() == []


def test_cli_datasource_list_on_project_with_one_datasource(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    filesystem_csv_2,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    project_root_dir = context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource list",
        catch_exceptions=False,
    )

    expected_output = f"""Using v3 (Batch Request) API\x1b[0m
1 Datasource found:[0m
[0m
 - [36mname:[0m my_datasource[0m
   [36mclass_name:[0m Datasource[0m
""".strip()
    stdout = result.stdout.strip()
    assert stdout == expected_output
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new(
    mock_subprocess, caplog, monkeypatch, empty_data_context, filesystem_csv_2
):
    context = empty_data_context
    root_dir = context.root_directory
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(root_dir))
    result = runner.invoke(
        cli,
        "--v3-api datasource new",
        input=f"1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert context.list_datasources() == []

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = DataContext(root_dir)

    assert len(context.list_datasources()) == 1

    assert context.list_datasources() == [
        {
            "name": "my_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "my_datasource_example_data_connector": {
                    "default_regex": {
                        "group_names": "data_asset_name",
                        "pattern": "(.*)",
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": "../../filesystem_csv_2",
                    "class_name": "InferredAssetFilesystemDataConnector",
                }
            },
        }
    ]
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_no_jupyter_writes_notebook(
    mock_subprocess, caplog, monkeypatch, empty_data_context, filesystem_csv_2
):
    context = empty_data_context
    root_dir = context.root_directory
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(root_dir))
    result = runner.invoke(
        cli,
        "--v3-api datasource new --no-jupyter",
        input=f"1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert context.list_datasources() == []

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout
    assert "To continue editing this Datasource" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    assert mock_subprocess.call_count == 0
    assert len(context.list_datasources()) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_with_name_param(
    mock_subprocess, caplog, monkeypatch, empty_data_context, filesystem_csv_2
):
    context = empty_data_context
    root_dir = context.root_directory
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(root_dir))
    result = runner.invoke(
        cli,
        "--v3-api datasource new --name foo",
        input=f"1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert context.list_datasources() == []

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = DataContext(root_dir)

    assert len(context.list_datasources()) == 1

    assert context.list_datasources() == [
        {
            "name": "foo",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "foo_example_data_connector": {
                    "default_regex": {
                        "group_names": "data_asset_name",
                        "pattern": "(.*)",
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": "../../filesystem_csv_2",
                    "class_name": "InferredAssetFilesystemDataConnector",
                }
            },
        }
    ]
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_from_misc_directory(
    mock_subprocess,
    caplog,
    monkeypatch,
    tmp_path_factory,
    empty_data_context,
    filesystem_csv_2,
):
    context = empty_data_context
    root_dir = context.root_directory
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    misc_dir = tmp_path_factory.mktemp("misc", numbered=False)
    monkeypatch.chdir(misc_dir)
    result = runner.invoke(
        cli,
        f"--config {root_dir} --v3-api datasource new",
        input=f"1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = DataContext(root_dir)
    assert context.list_datasources() == [
        {
            "name": "my_datasource",
            "class_name": "Datasource",
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "class_name": "PandasExecutionEngine",
            },
            "data_connectors": {
                "my_datasource_example_data_connector": {
                    "default_regex": {
                        "group_names": "data_asset_name",
                        "pattern": "(.*)",
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                    "base_directory": "../../filesystem_csv_2",
                    "class_name": "InferredAssetFilesystemDataConnector",
                }
            },
        }
    ]
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_delete_on_project_with_one_datasource(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert "my_datasource" in [ds["name"] for ds in context.list_datasources()]
    assert len(context.list_datasources()) == 1

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource delete my_datasource",
        input="Y\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0
    assert "Using v3 (Batch Request) API" in stdout
    assert "Datasource deleted successfully." in stdout

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)
    assert len(context.list_datasources()) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_delete_on_project_with_one_datasource_assume_yes_flag(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert "my_datasource" in [ds["name"] for ds in context.list_datasources()]
    assert len(context.list_datasources()) == 1

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api --assume-yes datasource delete my_datasource",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0

    assert "Would you like to proceed? [Y/n]:" not in stdout
    # This assertion is extra assurance since this test is too permissive if we change the confirmation message
    assert "[Y/n]" not in stdout

    assert "Using v3 (Batch Request) API" in stdout
    assert "Datasource deleted successfully." in stdout

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)
    assert len(context.list_datasources()) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_delete_on_project_with_one_datasource_declining_prompt_does_not_delete(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert "my_datasource" in [ds["name"] for ds in context.list_datasources()]
    assert len(context.list_datasources()) == 1

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource delete my_datasource",
        input="n\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0
    assert "Using v3 (Batch Request) API" in stdout
    assert "Datasource `my_datasource` was not deleted." in stdout

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)
    assert len(context.list_datasources()) == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_delete_with_non_existent_datasource_raises_error(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert "foo" not in [ds["name"] for ds in context.list_datasources()]

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource delete foo",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 1
    assert "Using v3 (Batch Request) API" in stdout
    assert "Datasource foo could not be found." in stdout
