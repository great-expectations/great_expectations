import os

from click.testing import CliRunner

from great_expectations.cli import cli


def test_project_check_on_missing_ge_dir(tmp_path_factory):
    project_dir = str(tmp_path_factory.mktemp("empty_dir"))
    runner = CliRunner()
    result = runner.invoke(cli, ["project", "check-config", "-d", project_dir])
    stdout = result.output
    assert "Checking your config files for validity" in stdout
    assert "Unfortunately, your config appears to be invalid" in stdout
    assert "Error: No great_expectations directory was found here!" in stdout
    assert result.exit_code == 1


def test_project_check_on_valid_project(titanic_data_context):
    project_dir = titanic_data_context.root_directory
    runner = CliRunner()
    result = runner.invoke(cli, ["project", "check-config", "-d", project_dir])
    assert "Checking your config files for validity" in result.output
    assert "Your config file appears valid" in result.output
    assert result.exit_code == 0


def test_project_check_on_project_with_missing_config_file(titanic_data_context):
    project_dir = titanic_data_context.root_directory
    # Remove the config file.
    os.remove(os.path.join(project_dir, "great_expectations.yml"))

    runner = CliRunner()
    result = runner.invoke(cli, ["project", "check-config", "-d", project_dir])
    assert result.exit_code == 1
    assert "Checking your config files for validity" in result.output
    assert "Unfortunately, your config appears to be invalid" in result.output
