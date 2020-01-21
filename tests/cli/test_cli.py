# -*- coding: utf-8 -*-
import os

from click.testing import CliRunner
from ruamel.yaml import YAML

from great_expectations import __version__ as ge_version
from great_expectations.cli import cli
from great_expectations.exceptions import ConfigNotFoundError

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


def test_cli_config_not_found(tmp_path_factory):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    curdir = os.path.abspath(os.getcwd())
    try:
        os.chdir(tmp_dir)
        runner = CliRunner()
        error_message = ConfigNotFoundError().message

        # datasource list
        result = runner.invoke(cli, ["datasource", "list", "-d", "./"])
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "list"])
        assert error_message in result.output

        # datasource new
        result = runner.invoke(cli, ["datasource", "new", "-d", "./"])
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "new"])
        assert error_message in result.output

        # datasource profile
        result = runner.invoke(cli, ["datasource", "profile", "-d", "./", "--no-view"])
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "profile", "--no-view"])
        assert error_message in result.output

        # docs build
        result = runner.invoke(cli, ["docs", "build", "-d", "./", "--no-view"])
        assert error_message in result.output
        result = runner.invoke(cli, ["docs", "build", "--no-view"])
        assert error_message in result.output

        # project check-config
        result = runner.invoke(cli, ["project", "check-config", "-d", "./"])
        assert error_message in result.output
        result = runner.invoke(cli, ["project", "check-config"])
        assert error_message in result.output

        # suite new
        result = runner.invoke(cli, ["suite", "new", "-d", "./"])
        assert error_message in result.output
        result = runner.invoke(cli, ["suite", "new"])
        assert error_message in result.output

        # suite edit
        result = runner.invoke(cli, ["suite", "edit", "FAKE", "FAKE", "-d", "./"])
        assert error_message in result.output
        result = runner.invoke(cli, ["suite", "edit", "FAKE", "FAKE"])
        assert error_message in result.output
    except:
        raise
    finally:
        os.chdir(curdir)
