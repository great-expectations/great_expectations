import os

from click.testing import CliRunner
from ruamel.yaml import YAML

from great_expectations import __version__ as ge_version
from great_expectations.cli import cli
from great_expectations.exceptions import ConfigNotFoundError
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

yaml = YAML()
yaml.default_flow_style = False

# TODO: <Alex>ALEX -- Update this --help page test to fit with the new design.</Alex>
TOP_LEVEL_HELP = """Usage: great_expectations [OPTIONS] COMMAND [ARGS]...

  Welcome to the great_expectations CLI!

  Most commands follow this format: great_expectations <NOUN> <VERB>

  The nouns are: checkpoint, datasource, docs, init, project, store, suite,
  validation-operator. Most nouns accept the following verbs: new, list, edit

Options:
  --version                 Show the version and exit.
  --legacy-api / --new-api  Default to legacy APIs (before 0.13). Use --new-api
                            for new APIs (after version 0.13)

  -v, --verbose             Set great_expectations to use verbose output.
  -c, --config TEXT         Path to great_expectations configuration file
                            location (great_expectations.yml). Inferred if not
                            provided.

  --help                    Show this message and exit.

Commands:
  checkpoint  Checkpoint operations
  datasource  Datasource operations
  docs        Data Docs operations
  init        Initialize a new Great Expectations project.
  project     Project operations
  store       Store operations
  suite       Expectation Suite operations
"""


def test_cli_command_entrance(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, catch_exceptions=False)
    print(result.output)
    assert result.exit_code == 0
    assert result.output == TOP_LEVEL_HELP
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_top_level_help(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, "--new-api --help", catch_exceptions=False)
    print(result.output)
    assert result.exit_code == 0
    assert result.output == TOP_LEVEL_HELP
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_command_invalid_command(
    caplog,
):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, ["blarg"])
    assert result.exit_code == 2
    assert "Error: No such command" in result.stderr
    assert ("'blarg'" in result.stderr) or ('"blarg"' in result.stderr)


def test_cli_version(
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
            cli, ["-c", "./", "datasource", "list"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "list"], catch_exceptions=False)
        assert error_message in result.output

        # datasource new
        result = runner.invoke(
            cli, ["-c", "./", "datasource", "new"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "new"], catch_exceptions=False)
        assert error_message in result.output

        # docs build
        result = runner.invoke(
            cli, ["-c", "./", "docs", "build", "--no-view"], catch_exceptions=False
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
            cli, ["-c", "./", "project", "check-config"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["project", "check-config"], catch_exceptions=False)
        assert error_message in result.output

        # suite new
        result = runner.invoke(
            cli, ["-c", "./", "suite", "new"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["suite", "new"], catch_exceptions=False)
        assert error_message in result.output

        # suite edit
        result = runner.invoke(
            cli, ["-c", "./", "suite", "edit", "FAKE"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["suite", "edit", "FAKE"], catch_exceptions=False)
        assert error_message in result.output

        # expectation suite delete
        result = runner.invoke(
            cli, ["-c", "FAKE", "suite", "delete", "deleteme"], catch_exceptions=False
        )
        assert error_message in result.output
        # expectation create new
        # suite new
        result = runner.invoke(
            cli, ["-c", "./", "suite", "new"], catch_exceptions=False
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
            cli, ["-c", "./", "datasource", "new"], catch_exceptions=False
        )

        # data_docs clean
        result = runner.invoke(
            cli,
            [
                "-c",
                "FAKE",
                "docs",
                "clean",
            ],
            catch_exceptions=False,
        )
        assert error_message in result.output
        # build docs before deleting again
        result = runner.invoke(
            cli, ["-c", "./", "docs", "build", "--no-view"], catch_exceptions=False
        )
        assert error_message in result.output
        result = runner.invoke(cli, ["docs", "clean"], catch_exceptions=False)
        assert error_message in result.output

        # leave with docs built
        result = runner.invoke(
            cli, ["-c", "./", "docs", "build", "--no-view"], catch_exceptions=False
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
