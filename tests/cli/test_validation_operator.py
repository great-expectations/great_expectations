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


def test_validation_operator_list_with_zero_validation_operators(caplog, empty_data_context):
    project_dir = empty_data_context.root_directory
    runner = CliRunner(mix_stderr=False)

    result = runner.invoke(
        cli, "validation-operator list -d {}".format(project_dir), catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Validation Operators found" in result.output

    assert_no_logging_messages_or_tracebacks(caplog, result)