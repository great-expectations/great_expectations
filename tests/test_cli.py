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
from .test_utils import assertDeepAlmostEqual
yaml = YAML()
yaml.default_flow_style = False


def test_cli_command_entrance():
    runner = CliRunner()

    result = runner.invoke(cli)
    assert result.exit_code == 0
    assert result.output == """Usage: cli [OPTIONS] COMMAND [ARGS]...

  great_expectations command-line interface

Options:
  --version      Show the version and exit.
  -v, --verbose  Set great_expectations to use verbose output.
  --help         Show this message and exit.

Commands:
  add-datasource  Add a new datasource to the data context.
  build-docs      Build Data Docs for a project.
  check-config    Check a config for validity and help with migrations.
  init            Create a new project and help with onboarding.
  profile         Profile datasources from the specified context.
  validate        Validate a CSV file against an expectation suite.
"""


def test_cli_command_bad_command():
    runner = CliRunner()

    result = runner.invoke(cli, [u"blarg"])
    assert result.exit_code == 2
    assert result.output == """Usage: cli [OPTIONS] COMMAND [ARGS]...
Try "cli --help" for help.

Error: No such command "blarg".
"""


def test_cli_validate_help():
    runner = CliRunner()

    result = runner.invoke(cli, ["validate", "--help"])

    assert result.exit_code == 0
    expected_help_message = """Usage: cli validate [OPTIONS] DATASET EXPECTATION_SUITE_FILE

  Validate a CSV file against an expectation suite.

  DATASET: Path to a file containing a CSV file to validate using the
  provided expectation_suite_file.

  EXPECTATION_SUITE_FILE: Path to a file containing a valid
  great_expectations expectations suite to use to validate the data.

Options:
  -p, --evaluation_parameters TEXT
                                  Path to a file containing JSON object used
                                  to evaluate parameters in expectations
                                  config.
  -o, --result_format TEXT        Result format to use when building
                                  evaluation responses.
  -e, --catch_exceptions BOOLEAN  Specify whether to catch exceptions raised
                                  during evaluation of expectations (defaults
                                  to True).
  -f, --only_return_failures BOOLEAN
                                  Specify whether to only return expectations
                                  that are not met during evaluation
                                  (defaults to False).
  -m, --custom_dataset_module TEXT
                                  Path to a python module containing a custom
                                  dataset class.
  -c, --custom_dataset_class TEXT
                                  Name of the custom dataset class to use
                                  during evaluation.
  --help                          Show this message and exit.
""".replace(" ", "").replace("\t", "").replace("\n", "")
    output = str(result.output).replace(
        " ", "").replace("\t", "").replace("\n", "")
    assert output == expected_help_message


def test_cli_validate_missing_positional_arguments():
    runner = CliRunner()

    result = runner.invoke(cli, ["validate"])

    assert "Error: Missing argument \"DATASET\"." in str(result.output)


def test_cli_version():
    runner = CliRunner()

    result = runner.invoke(cli, ["--version"])
    assert ge_version in str(result.output)


def test_validate_basic_operation():
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(1955, 11, 5)
        runner = CliRunner()
        with pytest.warns(UserWarning, match="No great_expectations version found in configuration object."):
            result = runner.invoke(cli, ["validate", "./tests/test_sets/Titanic.csv",
                                         "./tests/test_sets/titanic_expectations.json"])

            assert result.exit_code == 1
            json_result = json.loads(str(result.output))

    del json_result["meta"]["great_expectations.__version__"]
    with open('./tests/test_sets/expected_cli_results_default.json', 'r') as f:
        expected_cli_results = json.load(f)

    # In PY2 sorting is possible and order is wonky. Order doesn't matter. So sort in that case
    if PY2:
        json_result["results"] = sorted(json_result["results"])
        expected_cli_results["results"] = sorted(expected_cli_results["results"])

    assertDeepAlmostEqual(json_result, expected_cli_results)


def test_validate_custom_dataset():
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(1955, 11, 5)
        runner = CliRunner()
        with pytest.warns(UserWarning, match="No great_expectations version found in configuration object."):
            result = runner.invoke(cli, ["validate",
                                         "./tests/test_sets/Titanic.csv",
                                         "./tests/test_sets/titanic_custom_expectations.json",
                                         "-f", "True",
                                         "-m", "./tests/test_fixtures/custom_pandas_dataset.py",
                                         "-c", "CustomPandasDataset"])

            json_result = json.loads(result.output)

    del json_result["meta"]["great_expectations.__version__"]
    del json_result["results"][0]["result"]['partial_unexpected_counts']
    with open('./tests/test_sets/expected_cli_results_custom.json', 'r') as f:
        expected_cli_results = json.load(f)

    assert json_result == expected_cli_results


def test_cli_evaluation_parameters():
    with pytest.warns(UserWarning, match="No great_expectations version found in configuration object."):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate",
                                     "./tests/test_sets/Titanic.csv",
                                     "./tests/test_sets/titanic_parameterized_expectations.json",
                                     "--evaluation_parameters",
                                     "./tests/test_sets/titanic_evaluation_parameters.json",
                                     "-f", "True"])
        json_result = json.loads(result.output)

    with open('./tests/test_sets/titanic_evaluation_parameters.json', 'r') as f:
        expected_evaluation_parameters = json.load(f)

    assert json_result['evaluation_parameters'] == expected_evaluation_parameters


def test_cli_init_on_new_project(tmp_path_factory, filesystem_csv_2):
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
        result = runner.invoke(cli, ["init"], input="Y\n1\n%s\n\nn\n\n" % str(
            os.path.join(basedir, "data")))

        print(result.output)
        print("result.output length:", len(result.output))

        assert len(result.output) < 10000, "CLI output is unreasonably long."
        assert len(re.findall(
            "{", result.output)) < 100, "CLI contains way more '{' than we would reasonably expect."

        assert """Always know what to expect from your data""" in result.output
        assert """Let's add Great Expectations to your project""" in result.output
        assert """open a tutorial notebook""" in result.output

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
        data__dir/
            default/
                Titanic/
                    BasicDatasetProfiler.json
    notebooks/
        create_expectations.ipynb
        integrate_validation_into_pipeline.ipynb
    plugins/
        custom_data_docs/
            renderers/
            styles/
                data_docs_custom_styles.css
            views/
    uncommitted/
        config_variables.yml
        data_docs/
            local_site/
                index.html
                expectations/
                    data__dir/
                        default/
                            Titanic/
                                BasicDatasetProfiler.html
                validations/
                    profiling/
                        data__dir/
                            default/
                                Titanic/
                                    BasicDatasetProfiler.html
        samples/
        validations/
            profiling/
                data__dir/
                    default/
                        Titanic/
                            BasicDatasetProfiler.json
"""

        assert os.path.isfile(
            os.path.join(
                basedir,
                "great_expectations/expectations/data__dir/default/Titanic/BasicDatasetProfiler.json"
            )
        )

        fnames = []
        path = os.path.join(basedir, "great_expectations/uncommitted/validations/profiling/data__dir/default/Titanic")
        for (dirpath, dirnames, filenames) in os.walk(path):
            for filename in filenames:
                fnames.append(filename)
        assert fnames == ["BasicDatasetProfiler.json"]

        assert os.path.isfile(
            os.path.join(
                basedir,
                "great_expectations/uncommitted/data_docs/local_site/validations/profiling/data__dir/default/Titanic/BasicDatasetProfiler.html")
        )

        assert os.path.getsize(
            os.path.join(
                basedir,
                "great_expectations/uncommitted/data_docs/local_site/validations/profiling/data__dir/default/Titanic/BasicDatasetProfiler.html"
            )
        ) > 0
        print(result)
    except:
        raise
    finally:
        os.chdir(curdir)


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
        result = runner.invoke(cli, ["init"], input="Y\n4\n")

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
    result = runner.invoke(cli, ["add-datasource", "-d", project_root_dir], input="1\n%s\nmynewsource\nn\n" % str(filesystem_csv_2))

    captured = capsys.readouterr()

    ccc = [datasource['name'] for datasource in empty_data_context.list_datasources()]

    assert "Would you like to profile 'mynewsource'?" in result.stdout
    logger.removeHandler(handler)

# def test_cli_render(tmp_path_factory):
#     runner = CliRunner()
#     result = runner.invoke(cli, ["render"])

#     print(result)
#     print(result.output)
#     assert False


def test_cli_profile_with_datasource_arg(empty_data_context, filesystem_csv_2, capsys):

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
        cli, ["profile", "my_datasource", "-d", project_root_dir])

    captured = capsys.readouterr()

    assert "Profiling 'my_datasource' with 'BasicDatasetProfiler'" in captured.out
    assert "Please review results using data-docs." in captured.out
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
        cli, ["profile", "-d", project_root_dir])

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
        cli, ["profile", "-d", project_root_dir, "--batch_kwargs", '{"sep": ",", "parse_dates": [0]}'])
    evr = not_so_empty_data_context.get_validation_result("f1", expectation_suite_name="BasicDatasetProfiler")

    assert evr["meta"]["batch_kwargs"]["parse_dates"] == [0]
    assert evr["meta"]["batch_kwargs"]["sep"] == ","

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
        cli, ["profile", "my_datasource", "--data_assets", "f1", "-d", project_root_dir])

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
        cli, ["profile", "my_datasource", "--data_assets", "bad-bad-asset", "-d", project_root_dir],
    input="2\n")

    assert "Some of the data assets you specified were not found: bad-bad-asset" in result.output
    
    logger.removeHandler(handler)

def test_cli_documentation(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    print(json.dumps(not_so_empty_data_context.get_project_config(), indent=2))

    project_root_dir = not_so_empty_data_context.root_directory

    # For some reason, even with this logging change (which is required and done in main of the cli)
    # the click cli runner does not pick up output; capsys appears to intercept it first
    logger = logging.getLogger("great_expectations")
    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = logging.Formatter(
        '%(levelname)s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    runner = CliRunner()
    _ = runner.invoke(cli, ["profile", "my_datasource", "-d", project_root_dir])

    captured = capsys.readouterr()

    assert "Profiling 'my_datasource' with 'BasicDatasetProfiler'" in captured.out
    assert "Please review results using data-docs." in captured.out

    _ = runner.invoke(cli, ["build-docs", "-d", project_root_dir, "--no-view"])

    assert "index.html" in os.listdir(os.path.join(
        project_root_dir,
        "uncommitted/data_docs/local_site"
        )
    )

    logger.removeHandler(handler)


def test_cli_config_not_found(tmp_path_factory):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    curdir = os.path.abspath(os.getcwd())
    try:
        os.chdir(tmp_dir)
        runner = CliRunner()

        # profile
        result = runner.invoke(cli, ["profile", "-d", "./"])
        assert ConfigNotFoundError().message in result.output
        result = runner.invoke(cli, ["profile"])
        assert ConfigNotFoundError().message in result.output

        # build-docs
        result = runner.invoke(cli, ["build-docs", "-d", "./", "--no-view"])
        assert ConfigNotFoundError().message in result.output
        result = runner.invoke(cli, ["build-docs", "--no-view"])
        assert ConfigNotFoundError().message in result.output

        # check-config
        result = runner.invoke(cli, ["check-config", "-d", "./"])
        assert ConfigNotFoundError().message in result.output
        result = runner.invoke(cli, ["check-config"])
        assert ConfigNotFoundError().message in result.output
    except:
        raise
    finally:
        os.chdir(curdir)


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
    runner.invoke(cli, ["init"], input="Y\n4\n")
    shutil.rmtree(os.path.join(tmp_dir, "great_expectations/uncommitted"))

    try:
        result = runner.invoke(cli, ["init"], input="Y\n4\n")
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
    runner.invoke(cli, ["init"], input="Y\n4\n")
    # mangle setup
    uncommitted_dir = os.path.join(ge_dir, "uncommitted")
    shutil.rmtree(os.path.join(uncommitted_dir, "data_docs"))
    config_var_path = os.path.join(uncommitted_dir, "config_variables.yml")
    os.remove(config_var_path)
    # sanity check
    assert not os.path.isfile(config_var_path)

    try:
        result = runner.invoke(cli, ["init"], input="Y\n")

        # check dir structure
        dir_structure = gen_directory_tree_str(ge_dir)
        assert dir_structure == """\
great_expectations/
    .gitignore
    great_expectations.yml
    datasources/
    expectations/
    notebooks/
        create_expectations.ipynb
        integrate_validation_into_pipeline.ipynb
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


def test_cli_init_does_not_prompt_to_fix_if_all_uncommitted_dirs_exist(tmp_path_factory):
    """This test walks through an already onboarded project."""
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_init_on_existing_ge_yml"))
    curdir = os.path.abspath(os.getcwd())
    os.chdir(tmp_dir)
    runner = CliRunner()
    runner.invoke(cli, ["init"], input="Y\n4\n")

    try:
        result = runner.invoke(cli, ["init"])
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
