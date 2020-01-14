import os
import shutil

import pytest
from click.testing import CliRunner

from great_expectations.cli import cli
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.util import gen_directory_tree_str


@pytest.mark.skip(reason="TBD if this behavior is desired")
def test_cli_init_with_no_datasource_has_correct_cli_output_and_writes_config_yml(
    tmp_path_factory,
):
    """
    This is a low-key snapshot test used to sanity check some of the config yml
    inline comments, and some CLI output.
    """
    curdir = os.path.abspath(os.getcwd())

    try:
        basedir = str(tmp_path_factory.mktemp("test_cli_init_diff"))
        os.chdir(basedir)
        runner = CliRunner()
        result = runner.invoke(cli, ["init", "--no-view"], input="Y\n4\n")

        assert "Skipping datasource configuration." in result.output
        print(result.output)

        assert os.path.isdir(os.path.join(basedir, "great_expectations"))
        config_file_path = os.path.join(
            basedir, "great_expectations/great_expectations.yml"
        )
        assert os.path.isfile(config_file_path)
        with open(config_file_path, "r") as f:
            observed_config = f.read()

        assert (
            """# Welcome to Great Expectations! Always know what to expect from your data."""
            in observed_config
        )
        assert (
            """# Datasources tell Great Expectations where your data lives and how to get it.
# You can use the CLI command `great_expectations add-datasource` to help you"""
            in observed_config
        )
        assert (
            """# The plugins_directory will be added to your python path for custom modules
# used to override and extend Great Expectations."""
            in observed_config
        )
        assert (
            """# Stores are configurable places to store things like Expectations, Validations
# Data Docs, and more. These are for advanced users only - most users can simply
# leave this section alone.
# 
# Three stores are required: expectations, validations, and
# evaluation_parameters, and must exist with a valid store entry. Additional
# stores can be configured for uses such as data_docs, validation_operators, etc."""
            in observed_config
        )
        assert (
            """# Data Docs make it simple to visualize data quality in your project. These"""
            in observed_config
        )
    except:
        raise
    finally:
        os.chdir(curdir)


@pytest.mark.skip()
def test_cli_init_on_existing_ge_yml_with_some_missing_uncommitted_dirs(
    tmp_path_factory,
):
    """
    This test walks through the onboarding experience.

    The user just checked an existing project out of source control and does
    not yet have an uncommitted directory.
    """
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_init_on_existing_ge_yml"))
    curdir = os.path.abspath(os.getcwd())
    os.chdir(tmp_dir)
    runner = CliRunner()
    runner.invoke(cli, ["init", "--no-view"], input="Y\n4\n")
    shutil.rmtree(os.path.join(tmp_dir, "great_expectations/uncommitted"))

    try:
        result = runner.invoke(cli, ["init", "--no-view"], input="Y\n4\n")
        obs = result.output
        # Users should see
        assert (
            "To run locally, we need some files that are not in source control." in obs
        )
        assert "You may see new files in" in obs
        assert "Let's add Great Expectations to your project, by scaffolding" not in obs
        # Users should not see
        assert "open a tutorial notebook" not in obs
    except:
        raise
    finally:
        os.chdir(curdir)


@pytest.mark.skip()
def test_cli_init_on_existing_ge_yml_with_missing_uncommitted_dirs_and_missing_config_variables_yml(
    tmp_path_factory,
):
    """
    This test walks through an onboarding experience.

    The user just is missing some uncommitted dirs and is missing
    config_variables.yml
    """
    tmp_dir = str(tmp_path_factory.mktemp("more_stuff"))
    ge_dir = os.path.join(tmp_dir, "great_expectations")
    curdir = os.path.abspath(os.getcwd())
    os.chdir(tmp_dir)
    runner = CliRunner()
    runner.invoke(cli, ["init", "--no-view"], input="Y\n4\n")
    # mangle setup
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")
    shutil.rmtree(os.path.join(uncommitted_dir, "data_docs"))
    config_var_path = os.path.join(uncommitted_dir, "config_variables.yml")
    os.remove(config_var_path)
    # sanity check
    assert not os.path.isfile(config_var_path)

    try:
        result = runner.invoke(cli, ["init", "--no-view"], input="Y\n")

        # check dir structure
        dir_structure = gen_directory_tree_str(ge_dir)
        print(dir_structure)
        assert (
            dir_structure
            == """\
great_expectations/
    .gitignore
    great_expectations.yml
    datasources/
    expectations/
    notebooks/
        pandas/
            create_expectations.ipynb
            validation_playground.ipynb
        spark/
            create_expectations.ipynb
            validation_playground.ipynb
        sql/
            create_expectations.ipynb
            validation_playground.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
        samples/
        validations/
"""
        )
        # check config_variables.yml
        with open(config_var_path, "r") as f:
            obs_yml = f.read()
        assert obs_yml == CONFIG_VARIABLES_TEMPLATE

        # Check CLI output
        obs = result.output
        assert (
            "To run locally, we need some files that are not in source control." in obs
        )
        assert "You may see new files in" in obs
        assert "Let's add Great Expectations to your project, by scaffolding" not in obs

        assert "open a tutorial notebook" not in obs
    except:
        raise
    finally:
        os.chdir(curdir)


@pytest.mark.skip()
def test_cli_init_does_not_prompt_to_fix_if_all_uncommitted_dirs_exist(
    tmp_path_factory,
):
    """This test walks through an already onboarded project."""
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_init_on_existing_ge_yml"))
    curdir = os.path.abspath(os.getcwd())
    os.chdir(tmp_dir)
    runner = CliRunner()
    runner.invoke(cli, ["init", "--no-view"], input="Y\n4\n")

    try:
        result = runner.invoke(cli, ["init", "--no-view"])
        assert result.exit_code == 0
        obs = result.output

        # Users should see:
        assert "This looks like an existing project" in obs
        assert "appears complete" in obs
        assert "ready to roll." in obs

        # Users should NOT see:
        assert (
            "Great Expectations needs some directories that are not in source control."
            not in obs
        )
        assert "You may see new directories in" not in obs
        assert "Let's add Great Expectations to your project, by scaffolding" not in obs
        assert "open a tutorial notebook" not in obs
    except:
        raise
    finally:
        os.chdir(curdir)