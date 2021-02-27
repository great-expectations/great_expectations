import os

from click.testing import CliRunner
from ruamel.yaml import YAML

from great_expectations import __version__ as ge_version
from great_expectations.cli.v012 import cli
from great_expectations.exceptions import ConfigNotFoundError
from tests.cli.v012.utils import assert_no_logging_messages_or_tracebacks

yaml = YAML()
yaml.default_flow_style = False


def test_cli_command_entrance(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, catch_exceptions=False)
    assert result.exit_code == 0
    print(result.output)
    assert (
        result.output
        == """Usage: cli [OPTIONS] COMMAND [ARGS]...

  Welcome to the great_expectations CLI!

  Most commands follow this format: great_expectations <NOUN> <VERB>

  The nouns are: datasource, docs, project, suite, validation-operator

  Most nouns accept the following verbs: new, list, edit

  In particular, the CLI supports the following special commands:

  - great_expectations init : create a new great_expectations project

  - great_expectations datasource profile : profile a datasource

  - great_expectations docs build : compile documentation from expectations

Options:
  --version      Show the version and exit.
  -v, --verbose  Set great_expectations to use verbose output.
  --help         Show this message and exit.

Commands:
  checkpoint           Checkpoint operations
  datasource           Datasource operations
  docs                 Data Docs operations
  init                 Initialize a new Great Expectations project.
  project              Project operations
  store                Store operations
  suite                Expectation Suite operations
  validation-operator  Validation Operator operations
"""
    )
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_command_invalid_command(
    caplog,
):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["blarg"])
    assert result.exit_code == 2
    assert "Error: No such command" in result.stderr
    assert ("'blarg'" in result.stderr) or ('"blarg"' in result.stderr)


def test_cli_ge_version_exists(
    caplog,
):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["--version"], catch_exceptions=False)
    assert ge_version in str(result.output)
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_config_not_found_raises_error_for_all_commands(tmp_path_factory):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    curdir = os.path.abspath(os.getcwd())
    try:
        os.chdir(tmp_dir)
        runner = CliRunner(mix_stderr=False)
        error_message = ConfigNotFoundError().message

        # datasource list
        result = runner.invoke(
            cli, ["datasource", "list", "-d", "./"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "list"], catch_exceptions=False)
        assert error_message in result.output

        # datasource new
        result = runner.invoke(
            cli, ["datasource", "new", "-d", "./"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "new"], catch_exceptions=False)
        assert error_message in result.output

        # datasource profile
        result = runner.invoke(
            cli,
            ["datasource", "profile", "-d", "./", "--no-view"],
            catch_exceptions=False,
        )
        assert error_message in result.output
        result = runner.invoke(
            cli, ["datasource", "profile", "--no-view"], catch_exceptions=False
        )
        assert error_message in result.output

        # docs build
        result = runner.invoke(
            cli, ["docs", "build", "-d", "./", "--no-view"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(
            cli, ["docs", "build", "--no-view"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(
            cli, ["docs", "build", "--no-view", "--assume-yes"], catch_exceptions=False
        )
        assert error_message in result.output

        # project check-config
        result = runner.invoke(
            cli, ["project", "check-config", "-d", "./"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["project", "check-config"], catch_exceptions=False)
        assert error_message in result.output

        # suite new
        result = runner.invoke(
            cli, ["suite", "new", "-d", "./"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["suite", "new"], catch_exceptions=False)
        assert error_message in result.output

        # suite edit
        result = runner.invoke(
            cli, ["suite", "edit", "FAKE", "-d", "./"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["suite", "edit", "FAKE"], catch_exceptions=False)
        assert error_message in result.output

        # expectation suite delete
        result = runner.invoke(
            cli, ["suite", "delete", "deleteme", "-d", "FAKE"], catch_exceptions=False
        )
        assert error_message in result.output
        # expectation create new
        # suite new
        result = runner.invoke(
            cli, ["suite", "new", "-d", "./"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(
            cli, ["suite", "delete", "deleteme"], catch_exceptions=False
        )
        assert error_message in result.output

        # datasource delete
        result = runner.invoke(
            cli, ["datasource", "delete", "new"], catch_exceptions=False
        )
        assert error_message in result.output
        # create new before delete again
        # datasource new
        result = runner.invoke(
            cli, ["datasource", "new", "-d", "./"], catch_exceptions=False
        )

        # data_docs clean
        result = runner.invoke(
            cli, ["docs", "clean", "-d", "FAKE"], catch_exceptions=False
        )
        assert error_message in result.output
        # build docs before deleting again
        result = runner.invoke(
            cli, ["docs", "build", "-d", "./", "--no-view"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["docs", "clean"], catch_exceptions=False)
        assert error_message in result.output

        # leave with docs built
        result = runner.invoke(
            cli, ["docs", "build", "-d", "./", "--no-view"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(
            cli, ["docs", "build", "--no-view"], catch_exceptions=False
        )
        assert error_message in result.output

    except:
        raise
    finally:
        os.chdir(curdir)
