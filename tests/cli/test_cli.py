# -*- coding: utf-8 -*-

# Since our cli produces unicode output, but we want tests in python2 as well
from __future__ import unicode_literals

from datetime import datetime
from click.testing import CliRunner

import pytest
import json
import os
import shutil
import logging
import sys
import re
from ruamel.yaml import YAML

from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.exceptions import ConfigNotFoundError


try:
    from unittest import mock
except ImportError:
    import mock

from six import PY2

from great_expectations.cli import cli
from great_expectations.util import gen_directory_tree_str
from great_expectations import __version__ as ge_version
from tests.test_utils import assertDeepAlmostEqual, expectationSuiteValidationResultSchema

yaml = YAML()
yaml.default_flow_style = False


def test_cli_command_entrance():
    runner = CliRunner()

    result = runner.invoke(cli)
    assert result.exit_code == 0
    assert result.output == """Usage: cli [OPTIONS] COMMAND [ARGS]...

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


def test_cli_command_bad_command():
    runner = CliRunner()

    result = runner.invoke(cli, [u"blarg"])
    assert result.exit_code == 2
    assert result.output == """Usage: cli [OPTIONS] COMMAND [ARGS]...
Try "cli --help" for help.

Error: No such command "blarg".
"""


def test_cli_version():
    runner = CliRunner()

    result = runner.invoke(cli, ["--version"])
    assert ge_version in str(result.output)


@pytest.mark.skip()
def test_cli_init_on_new_project(tmp_path_factory):
    try:
        basedir = tmp_path_factory.mktemp("test_cli_init_diff")
        basedir = str(basedir)
        os.makedirs(os.path.join(basedir, "data"))
        curdir = os.path.abspath(os.getcwd())
        shutil.copy(
            "./tests/test_sets/Titanic.csv",
            str(os.path.join(basedir, "data/Titanic.csv"))
        )

        os.chdir(basedir)

        runner = CliRunner()
        result = runner.invoke(cli, ["init", "--no-view"], input="Y\n1\n%s\n\nn\n\n" % str(
            os.path.join(basedir, "data")))

        print(result.output)
        print("result.output length:", len(result.output))

        assert len(result.output) < 10000, "CLI output is unreasonably long."
        assert len(re.findall(
            "{", result.output)) < 100, "CLI contains way more '{' than we would reasonably expect."

        assert """Always know what to expect from your data""" in result.output
        assert """Let's add Great Expectations to your project""" in result.output

        assert os.path.isdir(os.path.join(basedir, "great_expectations"))
        assert os.path.isfile(os.path.join(
            basedir, "great_expectations/great_expectations.yml"))
        config = yaml.load(
            open(os.path.join(basedir, "great_expectations/great_expectations.yml"), "r"))
        assert config["datasources"]["data__dir"]["class_name"] == "PandasDatasource"


        print(gen_directory_tree_str(os.path.join(basedir, "great_expectations")))
        assert gen_directory_tree_str(os.path.join(basedir, "great_expectations")) == """\
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

    except:
        raise
    finally:
        os.chdir(curdir)


@pytest.mark.skip()
def test_cli_init_with_no_datasource_has_correct_cli_output_and_writes_config_yml(tmp_path_factory):
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
        config_file_path = os.path.join(basedir, "great_expectations/great_expectations.yml")
        assert os.path.isfile(config_file_path)
        with open(config_file_path, "r") as f:
            observed_config = f.read()

        assert """# Welcome to Great Expectations! Always know what to expect from your data.""" in observed_config
        assert """# Datasources tell Great Expectations where your data lives and how to get it.
# You can use the CLI command `great_expectations add-datasource` to help you""" in observed_config
        assert """# The plugins_directory will be added to your python path for custom modules
# used to override and extend Great Expectations.""" in observed_config
        assert """# Stores are configurable places to store things like Expectations, Validations
# Data Docs, and more. These are for advanced users only - most users can simply
# leave this section alone.
# 
# Three stores are required: expectations, validations, and
# evaluation_parameters, and must exist with a valid store entry. Additional
# stores can be configured for uses such as data_docs, validation_operators, etc.""" in observed_config
        assert """# Data Docs make it simple to visualize data quality in your project. These""" in observed_config
    except:
        raise
    finally:
        os.chdir(curdir)


@pytest.mark.skip()
def test_cli_add_datasource(empty_data_context, filesystem_csv_2, capsys):
    runner = CliRunner()
    project_root_dir = empty_data_context.root_directory
    # For some reason, even with this logging change (which is required and done in main of the cli)
    # the click cli runner does not pick up output; capsys appears to intercept it first
    logger = logging.getLogger("great_expectations")
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        '%(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["datasource", "new", "-d", project_root_dir, "--no-view"],
        input="1\n%s\nmynewsource\nn\n" % str(filesystem_csv_2)
    )

    captured = capsys.readouterr()

    ccc = [datasource['name'] for datasource in empty_data_context.list_datasources()]

    assert "Would you like to profile 'mynewsource'?" in result.stdout
    logger.removeHandler(handler)


def test_cli_profile_with_datasource_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    logger = logging.getLogger("great_expectations")
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter("%(levelname)s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "my_datasource",
            "-d",
            project_root_dir,
            "--no-view",
        ],
    )

    assert "Profiling 'my_datasource'" in result.stdout
    logger.removeHandler(handler)


def test_cli_profile_with_no_args(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory
    # print(project_root_dir)

    # For some reason, even with this logging change (which is required and done in main of the cli)
    # the click cli runner does not pick up output; capsys appears to intercept it first
    logger = logging.getLogger("great_expectations")
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        '%(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["profile", "-d", project_root_dir, "--no-view"])

    captured = capsys.readouterr()

    assert "Profiling 'my_datasource' with 'BasicDatasetProfiler'" in captured.out
    assert "Please review results using data-docs." in captured.out
    logger.removeHandler(handler)


def test_cli_profile_with_additional_batch_kwargs(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner()
    result = runner.invoke(
        cli, ["profile", "-d", project_root_dir, "--batch_kwargs", '{"reader_options": {"sep": ",", "parse_dates": ['
                                                                   '0]}}',
              "--no-view"])
    evr = not_so_empty_data_context.get_validation_result("f1", expectation_suite_name="BasicDatasetProfiler")

    assert evr.meta["batch_kwargs"]["reader_options"]["parse_dates"] == [0]
    assert evr.meta["batch_kwargs"]["reader_options"]["sep"] == ","

def test_cli_profile_with_valid_data_asset_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory
    # print(project_root_dir)

    # For some reason, even with this logging change (which is required and done in main of the cli)
    # the click cli runner does not pick up output; capsys appears to intercept it first
    logger = logging.getLogger("great_expectations")
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        '%(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["profile", "my_datasource", "--data_assets", "f1", "-d", project_root_dir, "--no-view"])

    captured = capsys.readouterr()

    assert "Profiling 'my_datasource' with 'BasicDatasetProfiler'" in captured.out
    assert "Please review results using data-docs." in captured.out
    logger.removeHandler(handler)

def test_cli_profile_with_invalid_data_asset_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory
    # print(project_root_dir)

    # For some reason, even with this logging change (which is required and done in main of the cli)
    # the click cli runner does not pick up output; capsys appears to intercept it first
    logger = logging.getLogger("great_expectations")
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        '%(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["profile", "my_datasource", "--data_assets", "bad-bad-asset", "-d", project_root_dir, "--no-view"],
    input="2\n")

    assert "Some of the data assets you specified were not found: bad-bad-asset" in result.output
    
    logger.removeHandler(handler)


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
        result = runner.invoke(cli, ["datasource", "new", "-d", "./", "--no-view"])
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "new", "--no-view"])
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


@pytest.mark.skip()
def test_cli_init_on_existing_ge_yml_with_some_missing_uncommitted_dirs(tmp_path_factory):
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
        assert "To run locally, we need some files that are not in source control." in obs
        assert "You may see new files in" in obs
        assert "Let's add Great Expectations to your project, by scaffolding" not in obs
        # Users should not see
        assert "open a tutorial notebook" not in obs
    except:
        raise
    finally:
        os.chdir(curdir)


@pytest.mark.skip()
def test_cli_init_on_existing_ge_yml_with_missing_uncommitted_dirs_and_missing_config_variables_yml(tmp_path_factory):
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
        assert dir_structure == """\
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
        # check config_variables.yml
        with open(config_var_path, 'r') as f:
            obs_yml = f.read()
        assert obs_yml == CONFIG_VARIABLES_TEMPLATE

        # Check CLI output
        obs = result.output
        assert "To run locally, we need some files that are not in source control." in obs
        assert "You may see new files in" in obs
        assert "Let's add Great Expectations to your project, by scaffolding" not in obs

        assert "open a tutorial notebook" not in obs
    except:
        raise
    finally:
        os.chdir(curdir)


@pytest.mark.skip()
def test_cli_init_does_not_prompt_to_fix_if_all_uncommitted_dirs_exist(tmp_path_factory):
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
        assert "Great Expectations needs some directories that are not in source control." not in obs
        assert "You may see new directories in" not in obs
        assert "Let's add Great Expectations to your project, by scaffolding" not in obs
        assert "open a tutorial notebook" not in obs
    except:
        raise
    finally:
        os.chdir(curdir)
