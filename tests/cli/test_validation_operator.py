# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import os

import mock
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_validation_operator_run_interactive_golden_path(caplog, data_context, filesystem_csv_2
):
    """
    Interactive mode golden path - pass an existing suite name and an existing validation
    operator name, select an existing file.
    """
    not_so_empty_data_context = data_context
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["validation-operator", "run", "-d", root_dir, "--name", "default", "--suite", "my_dag_node.default"],
        input=f"{csv_path}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Validation Failed" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)

def test_validation_operator_run_interactive_pass_non_existing_expectation_suite(caplog, data_context, filesystem_csv_2
):
    """
    Interactive mode: pass an non-existing suite name and an existing validation
    operator name, select an existing file.
    """
    not_so_empty_data_context = data_context
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["validation-operator", "run", "-d", root_dir, "--name", "default", "--suite", "this.suite.does.not.exist"],
        input=f"{csv_path}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Could not find a suite named" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)

def test_validation_operator_run_interactive_pass_non_existing_operator_name(caplog, data_context, filesystem_csv_2
):
    """
    Interactive mode: pass an non-existing suite name and an existing validation
    operator name, select an existing file.
    """
    not_so_empty_data_context = data_context
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    runner = CliRunner(mix_stderr=False)
    csv_path = os.path.join(filesystem_csv_2, "f1.csv")
    result = runner.invoke(
        cli,
        ["validation-operator", "run", "-d", root_dir, "--name", "this_val_op_does_not_exist", "--suite", "my_dag_node.default"],
        input=f"{csv_path}\n",
        catch_exceptions=False,
    )
    stdout = result.stdout
    assert "Could not find a validation operator" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)

def test_validation_operator_run_noninteractive_golden_path(caplog, data_context, filesystem_csv_2
):
    """
    Non-nteractive mode golden path - use the --validation_config_file argument to pass the path
    to a valid validation config file
    """
    not_so_empty_data_context = data_context
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    csv_path = os.path.join(filesystem_csv_2, "f1.csv")

    validation_config = {
      "validation_operator_name": "default",
      "batches": [
        {
          "batch_kwargs": {
            "path": csv_path,
            "datasource": "mydatasource",
            "reader_method": "read_csv"
          },
          "expectation_suite_names": ["my_dag_node.default"]
        }
      ]
    }
    validation_config_file_path = os.path.join(root_dir, "uncommitted", "validation_config_1.json")
    with open(validation_config_file_path, 'w') as f:
        json.dump(validation_config, f)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["validation-operator", "run", "-d", root_dir, "--validation_config_file", validation_config_file_path],
        catch_exceptions=False
    )
    stdout = result.stdout
    assert "Validation Failed" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)

def test_validation_operator_run_noninteractive_validation_config_file_does_not_exist(caplog, data_context, filesystem_csv_2
):
    """
    Non-nteractive mode. Use the --validation_config_file argument to pass the path
    to a validation config file that does not exist.
    """
    not_so_empty_data_context = data_context
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    validation_config_file_path = os.path.join(root_dir, "uncommitted", "validation_config_1.json")

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["validation-operator", "run", "-d", root_dir, "--validation_config_file", validation_config_file_path],
        catch_exceptions=False
    )
    stdout = result.stdout
    assert "Failed to process the --validation_config_file argument" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)

def test_validation_operator_run_noninteractive_validation_config_file_does_is_misconfigured(caplog, data_context, filesystem_csv_2
):
    """
    Non-nteractive mode. Use the --validation_config_file argument to pass the path
    to a validation config file that is misconfigured - one of the batches does not
    have expectation_suite_names attribute
    """
    not_so_empty_data_context = data_context
    root_dir = not_so_empty_data_context.root_directory
    os.mkdir(os.path.join(root_dir, "uncommitted"))

    csv_path = os.path.join(filesystem_csv_2, "f1.csv")

    validation_config = {
      "validation_operator_name": "default",
      "batches": [
        {
          "batch_kwargs": {
            "path": csv_path,
            "datasource": "mydatasource",
            "reader_method": "read_csv"
          },
          "wrong_attribute_expectation_suite_names": ["my_dag_node.default1"]
        }
      ]
    }
    validation_config_file_path = os.path.join(root_dir, "uncommitted", "validation_config_1.json")
    with open(validation_config_file_path, 'w') as f:
        json.dump(validation_config, f)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["validation-operator", "run", "-d", root_dir, "--validation_config_file", validation_config_file_path],
        catch_exceptions=False
    )
    stdout = result.stdout
    assert "is misconfigured: Each batch must have a list of expectation suite names" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_validation_operator_list_with_one_operator(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("a.warning")
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "validation-operator list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "1 validation operator found" in result.output
    assert "\taction_list_operator" in result.output
    assert_no_logging_messages_or_tracebacks(caplog, result)


@mock.patch.object(DataContext, 'list_validation_operator_names', return_value=[], side_effect=None)
def test_validation_operator_list_with_no_operators(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("a.warning")
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "validation-operator list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No validation operators are configured in the project" in result.output


@mock.patch.object(DataContext, 'list_validation_operator_names', return_value=["op_one", "op_two"], side_effect=None)
def test_validation_operator_list_with_two_operators(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    context = DataContext(project_dir)
    context.create_expectation_suite("a.warning")
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "validation-operator list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "2 validation operators" in result.output
