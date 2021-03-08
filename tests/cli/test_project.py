import os

from click.testing import CliRunner

from great_expectations.cli import cli
from tests.cli.utils import (
    VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    assert_no_logging_messages_or_tracebacks,
)


def test_project_check_on_missing_ge_dir_guides_user_to_fix(
    caplog, monkeypatch, tmp_path_factory
):
    project_dir = str(tmp_path_factory.mktemp("empty_dir"))
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(project_dir)
    result = runner.invoke(
        cli,
        ["--v3-api", "project", "check-config"],
        catch_exceptions=False,
    )
    stdout = result.output
    assert "Checking your config files for validity" in stdout
    assert "Unfortunately, your config appears to be invalid" in stdout
    assert "Error: No great_expectations directory was found here!" in stdout
    assert result.exit_code == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_project_check_on_valid_project_says_so(
    caplog, monkeypatch, titanic_data_context
):
    context = titanic_data_context
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["--v3-api", "project", "check-config"],
        catch_exceptions=False,
    )
    assert "Checking your config files for validity" in result.output
    assert "Your config file appears valid" in result.output
    assert result.exit_code == 0
    assert_no_logging_messages_or_tracebacks(
        my_caplog=caplog,
        click_result=result,
        allowed_deprecation_message=VALIDATION_OPERATORS_DEPRECATION_MESSAGE,
    )


def test_project_check_on_project_with_missing_config_file_guides_user(
    caplog, monkeypatch, titanic_data_context
):
    context = titanic_data_context
    # Remove the config file.
    os.remove(os.path.join(context.root_directory, "great_expectations.yml"))

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["--v3-api", "project", "check-config"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Checking your config files for validity" in result.output
    assert "Unfortunately, your config appears to be invalid" in result.output
    assert_no_logging_messages_or_tracebacks(caplog, result)
