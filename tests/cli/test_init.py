import os
import shutil

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.util import file_relative_path


def test_cli_init_on_existing_project_with_no_uncommitted_dirs_answering_yes_to_fixing_them(
    tmp_path_factory,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory.
    """
    root_dir = tmp_path_factory.mktemp("hiya")
    root_dir = str(root_dir)
    os.makedirs(os.path.join(root_dir, "data"))
    data_path = os.path.join(root_dir, "data/Titanic.csv")
    fixture_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(fixture_path, data_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["init", "--no-view", "-d", root_dir],
        input="Y\n1\n1\n{}\n\n\n\n".format(data_path),
    )
    stdout = result.output
    assert result.exit_code == 0
    assert "Great Expectations is now set up." in stdout
    print(stdout)

    context = DataContext(os.path.join(root_dir, DataContext.GE_DIR))
    uncommitted_dir = os.path.join(context.root_directory, "uncommitted")
    shutil.rmtree(uncommitted_dir)
    assert not os.path.isdir(uncommitted_dir)

    # Test the second invocation of init
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "-d", root_dir], input="Y\nn\n")
    stdout = result.stdout
    print(stdout)

    assert result.exit_code == 0
    assert "To run locally, we need some files that are not in source control" in stdout
    assert "Done. You may see new files in" in stdout
    assert "OK. You must run" not in stdout
    assert "great_expectations init" not in stdout
    assert "to fix the missing files!" not in stdout
    assert "Would you like to build & view this project's Data Docs!?" in stdout

    assert os.path.isdir(uncommitted_dir)
    config_var_path = os.path.join(uncommitted_dir, "config_variables.yml")
    assert os.path.isfile(config_var_path)
    with open(config_var_path, "r") as f:
        assert f.read() == CONFIG_VARIABLES_TEMPLATE


def test_cli_init_on_existing_project_with_no_uncommitted_dirs_answering_no_to_fixing_them(
    tmp_path_factory,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory, runs init and answers No to fixing.

    Therefore the disk should not be changed.
    """
    root_dir = tmp_path_factory.mktemp("hiya")
    root_dir = str(root_dir)
    os.makedirs(os.path.join(root_dir, "data"))
    data_path = os.path.join(root_dir, "data/Titanic.csv")
    fixture_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(fixture_path, data_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["init", "--no-view", "-d", root_dir],
        input="Y\n1\n1\n{}\n\n\n\n".format(data_path),
    )
    stdout = result.output
    assert result.exit_code == 0
    assert "Great Expectations is now set up." in stdout
    print(stdout)

    context = DataContext(os.path.join(root_dir, DataContext.GE_DIR))
    uncommitted_dir = os.path.join(context.root_directory, "uncommitted")
    shutil.rmtree(uncommitted_dir)
    assert not os.path.isdir(uncommitted_dir)

    # Test the second invocation of init
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "-d", root_dir], input="n\nn\n")
    stdout = result.stdout
    print(stdout)

    assert result.exit_code == 0
    assert "To run locally, we need some files that are not in source control" in stdout
    assert "OK. You must run" in stdout
    assert "great_expectations init" in stdout
    assert "to fix the missing files!" in stdout

    # DataContext should not write to disk unless you explicitly tell it to
    assert not os.path.isdir(uncommitted_dir)
    assert not os.path.isfile(os.path.join(uncommitted_dir, "config_variables.yml"))


def test_cli_init_on_complete_existing_project_all_uncommitted_dirs_exist(
    tmp_path_factory,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory.
    """
    root_dir = tmp_path_factory.mktemp("hiya")
    root_dir = str(root_dir)
    os.makedirs(os.path.join(root_dir, "data"))
    data_path = os.path.join(root_dir, "data/Titanic.csv")
    fixture_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
    shutil.copy(fixture_path, data_path)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["init", "--no-view", "-d", root_dir],
        input="Y\n1\n1\n{}\n\n\n\n".format(data_path),
    )
    stdout = result.output
    assert result.exit_code == 0
    print(stdout)

    # Test the second invocation of init
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--no-view", "-d", root_dir], input="n\n")
    stdout = result.stdout
    print(stdout)

    assert result.exit_code == 0
    assert "This looks like an existing project that" in stdout
    assert "appears complete" in stdout
    assert "ready to roll" in stdout
    assert "Would you like to build & view this project's Data Docs" in stdout
