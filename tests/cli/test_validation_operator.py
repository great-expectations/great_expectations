# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import json
import os

import mock
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.core import ExpectationSuite
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
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)
