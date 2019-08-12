# Since our cli produces unicode output, but we want tests in python2 as well
from __future__ import unicode_literals

from datetime import datetime
from click.testing import CliRunner
import great_expectations.version
from great_expectations.cli import cli
import tempfile
import pytest
import json
import os
import shutil
import logging
import sys
import re
from ruamel.yaml import YAML
yaml = YAML()
yaml.default_flow_style = False

try:
    from unittest import mock
except ImportError:
    import mock


from great_expectations.cli.init import scaffold_directories_and_notebooks


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
  build-documentation  Build data documentation for a project.
  init                 Initialize a new Great Expectations project.
  profile              Profile datasources from the specified context.
  render               Render a great expectations object to documentation.
  validate             Validate a CSV file against an expectation suite.
"""


def test_cli_command_bad_command():
    runner = CliRunner()

    result = runner.invoke(cli, ["blarg"])
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
    assert great_expectations.version.__version__ in str(result.output)


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

    assert json_result == expected_cli_results


def test_validate_custom_dataset():
    with mock.patch("datetime.datetime") as mock_datetime:
        mock_datetime.utcnow.return_value = datetime(1955, 11, 5)
        runner = CliRunner()
        with pytest.warns(UserWarning, match="No great_expectations version found in configuration object."):
            result = runner.invoke(cli, ["validate",
                                         "./tests/test_sets/Titanic.csv",
                                         "./tests/test_sets/titanic_custom_expectations.json",
                                         "-f", "True",
                                         "-m", "./tests/test_fixtures/custom_dataset.py",
                                         "-c", "CustomPandasDataset"])

            json_result = json.loads(result.output)

    del json_result["meta"]["great_expectations.__version__"]
    del json_result["results"][0]["result"]['partial_unexpected_counts']
    with open('./tests/test_sets/expected_cli_results_custom.json', 'r') as f:
        expected_cli_results = json.load(f)

    assert json_result == expected_cli_results


def test_cli_evaluation_parameters(capsys):
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


def test_cli_init(tmp_path_factory, filesystem_csv_2):
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
        result = runner.invoke(cli, ["init"], input="Y\n1\n%s\n\n" % str(
            os.path.join(basedir, "data")))

        print(result.output)
        print("result.output length:", len(result.output))

        assert len(result.output) < 10000, "CLI output is unreasonably long."
        assert len(re.findall(
            "{", result.output)) < 100, "CLI contains way more '{' than we would reasonably expect."

        assert """Always know what to expect from your data.""" in result.output

        assert os.path.isdir(os.path.join(basedir, "great_expectations"))
        assert os.path.isfile(os.path.join(
            basedir, "great_expectations/great_expectations.yml"))
        config = yaml.load(
            open(os.path.join(basedir, "great_expectations/great_expectations.yml"), "r"))
        assert config["datasources"]["data__dir"]["type"] == "pandas"

        assert os.path.isfile(
            os.path.join(
                basedir,
                "great_expectations/expectations/data__dir/default/Titanic/BasicDatasetProfiler.json"
            )
        )

        assert os.path.isfile(
            os.path.join(
                basedir,
                "great_expectations/uncommitted/validations/profiling/data__dir/default/Titanic/BasicDatasetProfiler.json")
        )

        assert os.path.isfile(
            os.path.join(
                basedir,
                "great_expectations/uncommitted/documentation/local_site/profiling/data__dir/default/Titanic/BasicDatasetProfiler.html")
        )

        assert os.path.getsize(
            os.path.join(
                basedir,
                "great_expectations/uncommitted/documentation/local_site/profiling/data__dir/default/Titanic/BasicDatasetProfiler.html"
            )
        ) > 0
        print(result)
    except:
        raise
    finally:
        os.chdir(curdir)


# def test_cli_render(tmp_path_factory):
#     runner = CliRunner()
#     result = runner.invoke(cli, ["render"])

#     print(result)
#     print(result.output)
#     assert False


def test_cli_profile_with_datasource_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv_2))
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
    assert "Note: You will need to review and revise Expectations before using them in production." in captured.out
    logger.removeHandler(handler)

def test_cli_profile_with_no_args(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv_2))
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
    assert "Note: You will need to review and revise Expectations before using them in production." in captured.out
    logger.removeHandler(handler)

def test_cli_profile_with_valid_data_asset_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv_2))
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
    assert "Note: You will need to review and revise Expectations before using them in production." in captured.out
    logger.removeHandler(handler)

def test_cli_profile_with_invalid_data_asset_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv_2))
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
    empty_data_context.add_datasource(
        "my_datasource", "pandas", base_directory=str(filesystem_csv_2))
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
    assert "Note: You will need to review and revise Expectations before using them in production." in captured.out

    result = runner.invoke(
        cli, ["documentation", "-d", project_root_dir])

    assert "index.html" in os.listdir(os.path.join(
        project_root_dir,
        "uncommitted/documentation/local_site"
        )
    )

    assert "index.html" in os.listdir(os.path.join(
        project_root_dir,
        "uncommitted/documentation/team_site"
        )
    )

    logger.removeHandler(handler)


def test_cli_config_not_found(tmp_path_factory):
    tmp_dir = str(tmp_path_factory.mktemp("test_cli_config_not_found"))
    curdir = os.path.abspath(os.getcwd())
    try:
        os.chdir(tmp_dir)
        runner = CliRunner()
        result = runner.invoke(
            cli, ["profile", "-d", "./"])
        assert "no great_expectations context configuration" in result.output
        result = runner.invoke(
            cli, ["profile"])
        assert "no great_expectations context configuration" in result.output
        result = runner.invoke(
            cli, ["build-documentation", "-d", "./"])
        assert "no great_expectations context configuration" in result.output
        result = runner.invoke(
            cli, ["build-documentation"])
        assert "no great_expectations context configuration" in result.output
    except:
        raise
    finally:
        os.chdir(curdir)


def test_scaffold_directories_and_notebooks(tmp_path_factory):
    empty_directory = str(tmp_path_factory.mktemp("test_scaffold_directories_and_notebooks"))
    scaffold_directories_and_notebooks(empty_directory)
    print(empty_directory)

    assert set(os.listdir(empty_directory)) == \
           {'datasources', 'plugins', 'expectations', '.gitignore', 'fixtures', 'uncommitted', 'notebooks'}
    assert set(os.listdir(os.path.join(empty_directory, "uncommitted"))) == \
           {'samples', 'documentation', 'validations', 'credentials'}
