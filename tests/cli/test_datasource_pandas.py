import os
from unittest import mock

import nbformat
import pytest
from click.testing import CliRunner
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations.cli import cli
from great_expectations.cli.cli_messages import FLUENT_DATASOURCE_LIST_WARNING
from great_expectations.cli.cli_messages import FLUENT_DATASOURCE_DELETE_ERROR
from great_expectations.util import get_context
from tests.cli.utils import assert_no_logging_messages_or_tracebacks, escape_ansi


pytestmark = [pytest.mark.cli]


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_datasource_list_on_project_with_no_datasources(
    mock_emit, caplog, monkeypatch, empty_data_context_stats_enabled, filesystem_csv_2
):
    context = empty_data_context_stats_enabled

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "datasource list",
        catch_exceptions=False,
    )

    stdout = result.stdout.strip()
    assert "No Datasources found" in stdout
    assert context.list_datasources() == []

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.list.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_count == len(expected_call_args_list)
    assert mock_emit.call_args_list == expected_call_args_list


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_datasource_list_on_project_with_one_datasource(
    mock_emit,
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    filesystem_csv_2,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "datasource list",
        catch_exceptions=False,
    )

    expected_output = """1 block config Datasource found:

 - name: my_datasource
   class_name: Datasource
""".strip()
    stdout = escape_ansi(result.stdout).strip()
    assert stdout == expected_output
    assert_no_logging_messages_or_tracebacks(caplog, result)

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.list.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_count == len(expected_call_args_list)
    assert mock_emit.call_args_list == expected_call_args_list


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@pytest.mark.slow  # 6.84s
def test_cli_datasource_new(
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    empty_data_context_stats_enabled,
    filesystem_csv_2,
):
    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(root_dir))
    result = runner.invoke(
        cli,
        "datasource new",
        input=f"y\n1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert context.list_datasources() == []

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GX_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {"type": "pandas", "api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.new.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = get_context(context_root_dir=root_dir)

    assert len(context.list_datasources()) == 1
    assert context.list_datasources() == [
        {
            "class_name": "Datasource",
            "data_connectors": {
                "default_inferred_data_connector_name": {
                    "base_directory": "../../test_cli_datasource_new0/filesystem_csv_2",
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "default_regex": {
                        "group_names": ["data_asset_name"],
                        "pattern": "(.*)",
                    },
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                },
                "default_runtime_data_connector_name": {
                    "assets": {
                        "my_runtime_asset_name": {
                            "batch_identifiers": ["runtime_batch_identifier_name"],
                            "class_name": "Asset",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                        }
                    },
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                },
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": "my_datasource",
        }
    ]
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_no_jupyter_writes_notebook(
    mock_subprocess,
    mock_emit,
    caplog,
    monkeypatch,
    empty_data_context_stats_enabled,
    filesystem_csv_2,
):
    context = empty_data_context_stats_enabled
    root_dir = context.root_directory
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(root_dir))
    result = runner.invoke(
        cli,
        "datasource new --no-jupyter",
        input=f"y\n1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert context.list_datasources() == []

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout
    assert "To continue editing this Datasource" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GX_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    assert mock_subprocess.call_count == 0
    assert len(context.list_datasources()) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {"type": "pandas", "api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.new.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@pytest.mark.slow  # 5.39s
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
        "datasource new --name foo",
        input=f"y\n1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert context.list_datasources() == []

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GX_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = get_context(context_root_dir=root_dir)

    assert len(context.list_datasources()) == 1
    assert context.list_datasources() == [
        {
            "class_name": "Datasource",
            "data_connectors": {
                "default_inferred_data_connector_name": {
                    "base_directory": "../../filesystem_csv_2",
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "default_regex": {
                        "group_names": ["data_asset_name"],
                        "pattern": "(.*)",
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                },
                "default_runtime_data_connector_name": {
                    "assets": {
                        "my_runtime_asset_name": {
                            "batch_identifiers": ["runtime_batch_identifier_name"],
                            "class_name": "Asset",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                        }
                    },
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                },
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": "foo",
        }
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
@pytest.mark.slow  # 5.19s
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
        f"--config {root_dir} datasource new",
        input=f"y\n1\n1\n{filesystem_csv_2}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GX_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = get_context(context_root_dir=root_dir)

    assert context.list_datasources() == [
        {
            "class_name": "Datasource",
            "data_connectors": {
                "default_inferred_data_connector_name": {
                    "base_directory": "../../filesystem_csv_2",
                    "class_name": "InferredAssetFilesystemDataConnector",
                    "default_regex": {
                        "group_names": ["data_asset_name"],
                        "pattern": "(.*)",
                    },
                    "module_name": "great_expectations.datasource.data_connector",
                },
                "default_runtime_data_connector_name": {
                    "assets": {
                        "my_runtime_asset_name": {
                            "batch_identifiers": ["runtime_batch_identifier_name"],
                            "class_name": "Asset",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                        }
                    },
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                },
            },
            "execution_engine": {
                "class_name": "PandasExecutionEngine",
                "module_name": "great_expectations.execution_engine",
            },
            "module_name": "great_expectations.datasource",
            "name": "my_datasource",
        }
    ]
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_datasource_delete_on_project_with_one_datasource(
    mock_emit,
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
        "datasource delete my_datasource",
        input="Y\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0
    assert "Datasource deleted successfully." in stdout

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = get_context(context_root_dir=root_directory)
    assert len(context.list_datasources()) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_list_fluent_datasource_warning(
    caplog,
    monkeypatch,
    data_context_with_fluent_datasource_and_block_datasource,
):
    """
    What does this test and why?
    The CLI does not support fluent datasources. This test ensures that if a fluent datasource is detected, a warning is printed.
    It also ensures that the correct number of block style datasources is listed.
    """

    context = data_context_with_fluent_datasource_and_block_datasource  # 1 fluent datasource, 1 block datasource

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        f"datasource list",
        input="Y\n",
        catch_exceptions=False,
    )
    stdout = result.output

    assert result.exit_code == 0
    assert FLUENT_DATASOURCE_LIST_WARNING in stdout
    assert "1 block config Datasource found" in stdout


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_prevent_fluent_datasource_delete(
    mock_emit,
    caplog,
    monkeypatch,
    data_context_with_fluent_datasource,
):
    context = data_context_with_fluent_datasource
    test_datasource_name = "my_pandas_datasource"
    assert test_datasource_name in [ds["name"] for ds in context.list_datasources()]
    assert len(context.list_datasources()) == 1

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        f"datasource delete {test_datasource_name}",
        input="Y\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 1
    assert FLUENT_DATASOURCE_DELETE_ERROR in stdout


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_datasource_delete_on_project_with_one_datasource_assume_yes_flag(
    mock_emit,
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
        "--assume-yes datasource delete my_datasource",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0

    assert "Would you like to proceed? [Y/n]:" not in stdout
    # This assertion is extra assurance since this test is too permissive if we change the confirmation message
    assert "[Y/n]" not in stdout

    assert "Datasource deleted successfully." in stdout

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = get_context(context_root_dir=root_directory)
    assert len(context.list_datasources()) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_datasource_delete_on_project_with_one_datasource_declining_prompt_does_not_delete(
    mock_emit,
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
        "datasource delete my_datasource",
        input="n\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0
    assert "Datasource `my_datasource` was not deleted." in stdout

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.end",
                "event_payload": {"cancelled": True, "api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = get_context(context_root_dir=root_directory)
    assert len(context.list_datasources()) == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_datasource_delete_with_non_existent_datasource_raises_error(
    mock_emit,
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
        "datasource delete foo",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 1
    assert "Datasource foo could not be found." in stdout

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.delete.end",
                "event_payload": {"api_version": "v3"},
                "success": False,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)
