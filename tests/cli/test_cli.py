# -*- coding: utf-8 -*-
from click.testing import CliRunner
from ruamel.yaml import YAML

from great_expectations import __version__ as ge_version
from great_expectations.cli import cli

yaml = YAML()
yaml.default_flow_style = False


def test_cli_command_entrance():
    runner = CliRunner()
    result = runner.invoke(cli)
    assert result.exit_code == 0
    assert (
        result.output
        == """Usage: cli [OPTIONS] COMMAND [ARGS]...

  Welcome to the great_expectations CLI!

  Most commands follow this format: great_expectations <NOUN> <VERB>

  The nouns are: datasource, docs, project, suite

  Most nouns accept the following verbs: new, list, edit

  In addition, the CLI supports the following special commands:

  - great_expectations init : same as `project new`

  - great_expectations datasource profile : profile a  datasource

  - great_expectations docs build : compile documentation from expectations

Options:
  --version      Show the version and exit.
  -v, --verbose  Set great_expectations to use verbose output.
  --help         Show this message and exit.

Commands:
  datasource  datasource operations
  docs        data docs operations
  init        Initialize a new Great Expectations project.
  project     project operations
  suite       expectation suite operations
"""
    )


def test_cli_command_invalid_command():
    runner = CliRunner()
    result = runner.invoke(cli, ["blarg"])
    assert result.exit_code == 2
    assert (
        result.output
        == """Usage: cli [OPTIONS] COMMAND [ARGS]...
Try "cli --help" for help.

Error: No such command "blarg".
"""
    )


def test_cli_version():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert ge_version in str(result.output)
