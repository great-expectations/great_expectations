import os

from click.testing import CliRunner
from ruamel.yaml import YAML

from great_expectations import __version__ as ge_version
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks

yaml = YAML()
yaml.default_flow_style = False

TOP_LEVEL_HELP = """Usage: great_expectations [OPTIONS] COMMAND [ARGS]...

  Welcome to the great_expectations CLI!

  Most commands follow this format: great_expectations <NOUN> <VERB>

  The nouns are: checkpoint, datasource, docs, init, project, store, suite,
  validation-operator. Most nouns accept the following verbs: new, list, edit

Options:
  --version            Show the version and exit.
  --v2-api / --v3-api  Default to v2 (Batch Kwargs) API. Use --v3-api for v3
                       (Batch Request) API

  -v, --verbose        Set great_expectations to use verbose output.
  -c, --config TEXT    Path to great_expectations configuration file location
                       (great_expectations.yml). Inferred if not provided.

  --help               Show this message and exit.

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
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(cli, catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == TOP_LEVEL_HELP
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_top_level_help(caplog):
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(cli, "--help", catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == TOP_LEVEL_HELP
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_top_level_help_with_v3_flag(caplog):
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(cli, "--v3-api --help", catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == TOP_LEVEL_HELP
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_command_invalid_command(caplog):
    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(cli, "--v3-api blarg")
    assert result.exit_code == 2
    assert "Error: No such command" in result.stderr
    assert ("'blarg'" in result.stderr) or ('"blarg"' in result.stderr)


def test_cli_ge_version_exists(caplog):
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(cli, "--v3-api --version", catch_exceptions=False)
    assert ge_version in str(result.output)
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_works_from_adjacent_directory_without_config_flag(
    monkeypatch, empty_data_context
):
    """We don't care about the NOUN here just combinations of the config flag"""
    runner = CliRunner(mix_stderr=True)
    monkeypatch.chdir(os.path.dirname(empty_data_context.root_directory))
    result = runner.invoke(cli, "--v3-api checkpoint list", catch_exceptions=False)
    assert result.exit_code == 0
    assert "No Checkpoints found" in result.output


def test_cli_works_from_great_expectations_directory_without_config_flag(
    monkeypatch, empty_data_context
):
    """We don't care about the NOUN here just combinations of the config flag"""
    runner = CliRunner(mix_stderr=True)
    monkeypatch.chdir(empty_data_context.root_directory)
    result = runner.invoke(cli, "--v3-api checkpoint list", catch_exceptions=False)
    assert result.exit_code == 0
    assert "No Checkpoints found" in result.output


def test_cli_works_from_random_directory_with_config_flag_fully_specified_yml(
    monkeypatch, empty_data_context, tmp_path_factory
):
    """We don't care about the NOUN here just combinations of the config flag"""
    context = empty_data_context
    runner = CliRunner(mix_stderr=True)
    temp_dir = tmp_path_factory.mktemp("config_flag_check")
    monkeypatch.chdir(temp_dir)
    result = runner.invoke(
        cli,
        f"--config {context.root_directory}/great_expectations.yml --v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Checkpoints found" in result.output


def test_cli_works_from_random_directory_with_config_flag_great_expectations_directory(
    monkeypatch, empty_data_context, tmp_path_factory
):
    """We don't care about the NOUN here just combinations of the config flag"""
    context = empty_data_context
    runner = CliRunner(mix_stderr=True)
    temp_dir = tmp_path_factory.mktemp("config_flag_check")
    monkeypatch.chdir(temp_dir)
    result = runner.invoke(
        cli,
        f"--config {context.root_directory} --v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Checkpoints found" in result.output


def test_cli_works_from_random_directory_with_c_flag_fully_specified_yml(
    monkeypatch, empty_data_context, tmp_path_factory
):
    """We don't care about the NOUN here just combinations of the config flag"""
    context = empty_data_context
    runner = CliRunner(mix_stderr=True)
    temp_dir = tmp_path_factory.mktemp("config_flag_check")
    monkeypatch.chdir(temp_dir)
    result = runner.invoke(
        cli,
        f"-c {context.root_directory}/great_expectations.yml --v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Checkpoints found" in result.output


def test_cli_works_from_random_directory_with_c_flag_great_expectations_directory(
    monkeypatch, empty_data_context, tmp_path_factory
):
    """We don't care about the NOUN here just combinations of the config flag"""
    context = empty_data_context
    runner = CliRunner(mix_stderr=True)
    temp_dir = tmp_path_factory.mktemp("config_flag_check")
    monkeypatch.chdir(temp_dir)
    result = runner.invoke(
        cli,
        f"-c {context.root_directory} --v3-api checkpoint list",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "No Checkpoints found" in result.output


CONFIG_NOT_FOUND_ERROR_MESSAGE = "No great_expectations directory was found here!"


def test_cli_config_not_found_raises_error_for_datasource_list(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "datasource", "list"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "datasource", "list"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_datasource_new(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "datasource", "new"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "datasource", "new"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_datasource_delete(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        ["-c", "./", "--v3-api", "datasource", "delete", "new"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "datasource", "delete", "new"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_docs_build(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        ["-c", "./", "--v3-api", "docs", "build", "--no-view"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "docs", "build", "--no-view"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli,
        ["--v3-api", "docs", "build", "--no-view", "--assume-yes"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_project_check_config(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        ["-c", "./", "--v3-api", "project", "check-config"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "project", "check-config"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_project_upgrade(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        ["-c", "./", "--v3-api", "project", "upgrade"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "project", "upgrade"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_store_list(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "store", "list"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(cli, ["--v3-api", "store", "list"], catch_exceptions=False)
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_suite_new(tmp_path_factory, monkeypatch):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "suite", "new"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(cli, ["--v3-api", "suite", "new"], catch_exceptions=False)
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_suite_list(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "suite", "list"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(cli, ["--v3-api", "suite", "list"], catch_exceptions=False)
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_suite_scaffold(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "suite", "scaffold"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "suite", "scaffold"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_suite_edit(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "suite", "edit", "FAKE"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "suite", "edit", "FAKE"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_suite_delete(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "suite", "delete", "deleteme"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "suite", "delete", "deleteme"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_checkpoint_new(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "checkpoint", "new"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "checkpoint", "new"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_checkpoint_list(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli, ["-c", "./", "--v3-api", "checkpoint", "list"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "checkpoint", "list"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_checkpoint_delete(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        ["-c", "./", "--v3-api", "checkpoint", "delete", "deleteme"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "checkpoint", "delete", "deleteme"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_docs_clean(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        [
            "-c",
            "./",
            "--v3-api",
            "docs",
            "clean",
        ],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(cli, ["--v3-api", "docs", "clean"], catch_exceptions=False)
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_docs_list(tmp_path_factory, monkeypatch):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        [
            "-c",
            "./",
            "--v3-api",
            "docs",
            "list",
        ],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(cli, ["--v3-api", "docs", "list"], catch_exceptions=False)
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output


def test_cli_config_not_found_raises_error_for_docs_build(
    tmp_path_factory, monkeypatch
):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    monkeypatch.chdir(tmp_dir)
    runner = CliRunner(mix_stderr=True)
    result = runner.invoke(
        cli,
        ["-c", "./", "--v3-api", "docs", "build", "--no-view"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output

    result = runner.invoke(
        cli,
        ["-c", "./", "--v3-api", "docs", "build", "--no-view"],
        catch_exceptions=False,
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
    result = runner.invoke(
        cli, ["--v3-api", "docs", "build", "--no-view"], catch_exceptions=False
    )
    assert CONFIG_NOT_FOUND_ERROR_MESSAGE in result.output
