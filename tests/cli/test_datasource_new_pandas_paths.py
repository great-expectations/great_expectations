"""
This module specifically tests for combinations of paths for datasource new.

Tests are named according to the matrix of possible states is as follows:

run from            data path
-----------------------------
 ge dir             absolute
 ge dir             relative
 adjacent           absolute
 adjacent           relative
 misc + --config    absolute
 misc + --config    relative

Please note that all tests have the same excpected result, hence the consolidation.
"""

import os
import shutil

import mock
import nbformat
import pytest
from click.testing import CliRunner
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations import DataContext
from great_expectations.cli import cli


def _run_notebook(context: DataContext, datasource_name: str) -> None:
    uncommitted_dir = os.path.join(context.root_directory, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(
        uncommitted_dir, f"datasource_new_{datasource_name}.ipynb"
    )
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)
    ep = ExecutePreprocessor(timeout=60, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})


def _run_cli_datasource_new_path_test(
    context: DataContext, args: str, invocation_input: str
) -> None:
    root_dir = context.root_directory
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        args=args,
        input=invocation_input,
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    _run_notebook(context, "foo")

    # Renew a context since we executed a notebook in a different process
    del context
    context = DataContext(root_dir)
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
                    "base_directory": "../../test_files",
                    "class_name": "InferredAssetFilesystemDataConnector",
                }
            },
        }
    ]


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_run_from_ge_dir_absolute_data_path(
    mock_subprocess, monkeypatch, empty_data_context, filesystem_csv_2
):
    context = empty_data_context
    monkeypatch.chdir(context.root_directory)
    invocation = "--v3-api datasource new"
    invocation_input = f"1\n1\n{filesystem_csv_2}\nfoo\n"
    _run_cli_datasource_new_path_test(context, invocation, invocation_input)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_run_from_ge_dir_relative_data_path(
    mock_subprocess, monkeypatch, empty_data_context, filesystem_csv_2
):
    context = empty_data_context
    monkeypatch.chdir(context.root_directory)
    invocation = "--v3-api datasource new"
    invocation_input = "1\n1\n../../test_files\nfoo\n"
    _run_cli_datasource_new_path_test(context, invocation, invocation_input)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_run_from_adjacent_dir_absolute_data_path(
    mock_subprocess, monkeypatch, empty_data_context, filesystem_csv_2
):
    context = empty_data_context
    root_dir = context.root_directory
    monkeypatch.chdir(os.path.dirname(root_dir))
    invocation = "--v3-api datasource new"
    invocation_input = f"1\n1\n{filesystem_csv_2}\nfoo\n"
    _run_cli_datasource_new_path_test(context, invocation, invocation_input)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_run_from_adjacent_dir_relative_data_path(
    mock_subprocess, monkeypatch, empty_data_context, filesystem_csv_2
):
    context = empty_data_context
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    invocation = "--v3-api datasource new"
    invocation_input = "1\n1\n../../test_files\nfoo\n"
    _run_cli_datasource_new_path_test(context, invocation, invocation_input)


@pytest.fixture
def misc_directory(tmp_path_factory):
    misc_dir = tmp_path_factory.mktemp("random", numbered=False)
    assert os.path.isabs(misc_dir)
    yield misc_dir
    shutil.rmtree(misc_dir)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_run_from_misc_dir_using_config_flag_absolute_data_path(
    mock_subprocess, monkeypatch, empty_data_context, filesystem_csv_2, misc_directory
):
    context = empty_data_context
    monkeypatch.chdir(misc_directory)
    invocation = f"--config {context.root_directory} --v3-api datasource new"
    invocation_input = f"1\n1\n{filesystem_csv_2}\nfoo\n"
    _run_cli_datasource_new_path_test(context, invocation, invocation_input)


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_run_from_misc_dir_using_config_flag_relative_data_path(
    mock_subprocess, monkeypatch, empty_data_context, filesystem_csv_2, misc_directory
):
    context = empty_data_context
    monkeypatch.chdir(misc_directory)
    invocation = f"--config {context.root_directory} --v3-api datasource new"
    invocation_input = "1\n1\n../../test_files\nfoo\n"
    _run_cli_datasource_new_path_test(context, invocation, invocation_input)
