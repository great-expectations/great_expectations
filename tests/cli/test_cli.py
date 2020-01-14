# -*- coding: utf-8 -*-

# Since our cli produces unicode output, but we want tests in python2 as well
from __future__ import unicode_literals

import os
import re
import shutil

import pytest
from click.testing import CliRunner
from ruamel.yaml import YAML

from great_expectations import DataContext
from great_expectations import __version__ as ge_version
from great_expectations.cli import cli
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import ConfigNotFoundError
from great_expectations.util import gen_directory_tree_str
from tests.test_utils import is_library_installed

try:
    from unittest import mock
except ImportError:
    import mock


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


def test_project_check_on_missing_ge_dir():
    runner = CliRunner()
    result = runner.invoke(cli, ["project", "check-config"])
    assert "Checking your config files for validity" in result.output
    assert "Unfortunately, your config appears to be invalid" in result.output
    assert "Error: No great_expectations directory was found here!" in result.output
    assert result.exit_code == 1


def test_project_check_on_valid_project(titanic_data_context):
    project_dir = titanic_data_context.root_directory
    runner = CliRunner()
    result = runner.invoke(cli, ["project", "check-config", "-d", project_dir])
    assert "Checking your config files for validity" in result.output
    assert "Your config file appears valid" in result.output
    assert result.exit_code == 0


def test_project_check_on_invalid_project(titanic_data_context):
    project_dir = titanic_data_context.root_directory
    # Remove the config file.
    os.remove(os.path.join(project_dir, "great_expectations.yml"))

    runner = CliRunner()
    result = runner.invoke(cli, ["project", "check-config", "-d", project_dir])
    assert result.exit_code == 1
    assert "Checking your config files for validity" in result.output
    assert "Unfortunately, your config appears to be invalid" in result.output


def test_cli_version():
    runner = CliRunner()

    result = runner.invoke(cli, ["--version"])
    assert ge_version in str(result.output)


def test_cli_init_on_new_project(tmp_path_factory):
    curdir = os.path.abspath(os.getcwd())

    try:
        basedir = tmp_path_factory.mktemp("test_cli_init_diff")
        basedir = str(basedir)
        os.makedirs(os.path.join(basedir, "data"))
        data_path = os.path.join(basedir, "data/Titanic.csv")
        fixture_path = file_relative_path(__file__, "../test_sets/Titanic.csv")
        shutil.copy(fixture_path, data_path)

        os.chdir(basedir)

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["init", "--no-view"],
            input="Y\n1\n1\n{}\n\n\n\n".format(data_path),
        )
        stdout = result.output

        assert len(stdout) < 2000, "CLI output is unreasonably long."

        assert "Always know what to expect from your data" in stdout
        assert "What data would you like Great Expectations to connect to" in stdout
        assert "What are you processing your files with" in stdout
        assert "Enter the path (relative or absolute) of a data file" in stdout
        assert "Give your new data asset a short name" in stdout
        assert "Name the new expectation suite [warning]" in stdout
        assert (
            "Great Expectations will choose a couple of columns and generate expectations about them"
            in stdout
        )
        assert "Profiling Titanic" in stdout
        assert "Building" in stdout
        assert "Data Docs" in stdout
        assert "Great Expectations is now set up" in stdout

        assert os.path.isdir(os.path.join(basedir, "great_expectations"))
        config_path = os.path.join(basedir, "great_expectations/great_expectations.yml")
        assert os.path.isfile(config_path)

        config = yaml.load(open(config_path, "r"))
        data_source_class = config["datasources"]["files_datasource"]["data_asset_type"]["class_name"]
        assert data_source_class == "PandasDataset"

        obs_tree = gen_directory_tree_str(os.path.join(basedir, "great_expectations"))

        # Instead of monkey patching datetime, just regex out the time directories
        date_safe_obs_tree = re.sub(r"\d*T\d*\.\d*Z", "9999.9999", obs_tree)

        assert (
            date_safe_obs_tree
            == """\
great_expectations/
    .gitignore
    great_expectations.yml
    datasources/
    expectations/
        files_datasource/
            default/
                Titanic/
                    warning.json
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
            local_site/
                index.html
                expectations/
                    files_datasource/
                        default/
                            Titanic/
                                warning.html
                static/
                    fonts/
                        HKGrotesk/
                            HKGrotesk-Bold.otf
                            HKGrotesk-BoldItalic.otf
                            HKGrotesk-Italic.otf
                            HKGrotesk-Light.otf
                            HKGrotesk-LightItalic.otf
                            HKGrotesk-Medium.otf
                            HKGrotesk-MediumItalic.otf
                            HKGrotesk-Regular.otf
                            HKGrotesk-SemiBold.otf
                            HKGrotesk-SemiBoldItalic.otf
                    images/
                        0_values_not_null_html_en.jpg
                        10_suite_toc.jpeg
                        11_home_validation_results_failed.jpeg
                        12_validation_overview.png
                        13_validation_passed.jpeg
                        14_validation_failed.jpeg
                        15_validation_failed_unexpected_values.jpeg
                        16_validation_failed_unexpected_values (1).gif
                        1_values_not_null_html_de.jpg
                        2_values_not_null_json.jpg
                        3_values_not_null_validation_result_json.jpg
                        4_values_not_null_validation_result_html_en.jpg
                        5_home.png
                        6_home_tables.jpeg
                        7_home_suites.jpeg
                        8_home_validation_results_succeeded.jpeg
                        9_suite_overview.png
                        favicon.ico
                        glossary_scroller.gif
                        iterative-dev-loop.png
                        logo-long-vector.svg
                        logo-long.png
                        short-logo-vector.svg
                        short-logo.png
                        validation_failed_unexpected_values.gif
                        values_not_null_html_en.jpg
                        values_not_null_json.jpg
                        values_not_null_validation_result_html_en.jpg
                        values_not_null_validation_result_json.jpg
                    styles/
                        data_docs_custom_styles_template.css
                        data_docs_default_styles.css
                validations/
                    9999.9999/
                        files_datasource/
                            default/
                                Titanic/
                                    warning.html
        samples/
        validations/
            9999.9999/
                files_datasource/
                    default/
                        Titanic/
                            warning.json
"""
        )
    except Exception as e:
        raise e
    finally:
        os.chdir(curdir)


def _library_not_loaded_test(tmp_path_factory, cli_input, library_name, library_import_name):
    """
    This test requires that a library is NOT installed. It tests that:
    - a helpful error message is returned to install the missing library
    - the expected tree structure is in place
    - the config yml contains an empty dict in its datasource entry
    """
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")
    basedir = str(basedir)
    os.chdir(basedir)

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--no-view"], input=cli_input)
    stdout = result.output

    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "Which database backend are you using" in stdout
    assert "Give your new data source a short name" in stdout
    assert (
        """Next, we will configure database credentials and store them in the `my_db` section
of this config file: great_expectations/uncommitted/config_variables.yml"""
        in stdout
    )
    assert "Great Expectations relies on the library `{}`".format(library_import_name) in stdout
    assert "Please `pip install {}` before trying again".format(library_name) in stdout

    assert "Profiling" not in stdout
    assert "Building" not in stdout
    assert "Data Docs" not in stdout
    assert "Great Expectations is now set up" not in stdout

    assert result.exit_code == 1

    assert os.path.isdir(os.path.join(basedir, "great_expectations"))
    config_path = os.path.join(basedir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path, "r"))
    assert config["datasources"] == dict()

    obs_tree = gen_directory_tree_str(os.path.join(basedir, "great_expectations"))
    assert (
        obs_tree
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
@pytest.mark.skipif(
    is_library_installed("pymysql"), reason="requires pymysql to NOT be installed"
)
def test_cli_init_db_mysql_without_library_installed_instructs_user(tmp_path_factory):
    _library_not_loaded_test(tmp_path_factory, "Y\n2\n1\nmy_db\n", "pymysql", "pymysql")


@pytest.mark.skipif(
    is_library_installed("psycopg2"), reason="requires psycopg2 to NOT be installed"
)
def test_cli_init_db_postgres_without_library_installed_instructs_user(
    tmp_path_factory
):
    _library_not_loaded_test(
        tmp_path_factory, "Y\n2\n2\nmy_db\n", "psycopg2", "psycopg2"
    )


@pytest.mark.skipif(
    is_library_installed("psycopg2"), reason="requires psycopg2 to NOT be installed"
)
def test_cli_init_db_redshift_without_library_installed_instructs_user(
    tmp_path_factory
):
    _library_not_loaded_test(
        tmp_path_factory, "Y\n2\n3\nmy_db\n", "psycopg2", "psycopg2"
    )


@pytest.mark.skipif(
    is_library_installed("snowflake"),
    reason="requires snowflake-sqlalchemy to NOT be installed",
)
def test_cli_init_db_snowflake_without_library_installed_instructs_user(
    tmp_path_factory
):
    _library_not_loaded_test(
        tmp_path_factory, "Y\n2\n4\nmy_db\n", "snowflake-sqlalchemy", "snowflake"
    )


@pytest.mark.skipif(
    is_library_installed("pyspark"), reason="requires pyspark to NOT be installed"
)
def test_cli_init_spark_without_library_installed_instructs_user(tmp_path_factory):
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")
    basedir = str(basedir)
    os.chdir(basedir)

    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--no-view"], input="Y\n1\n2\n")
    stdout = result.output

    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "What are you processing your files with" in stdout
    assert "Great Expectations relies on the library `pyspark`" in stdout
    assert "Please `pip install pyspark` before trying again" in stdout

    assert "Profiling" not in stdout
    assert "Building" not in stdout
    assert "Data Docs" not in stdout
    assert "Great Expectations is now set up" not in stdout

    assert result.exit_code == 1

    assert os.path.isdir(os.path.join(basedir, "great_expectations"))
    config_path = os.path.join(basedir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path, "r"))
    assert config["datasources"] == dict()

    obs_tree = gen_directory_tree_str(os.path.join(basedir, "great_expectations"))
    assert (
        obs_tree
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


@pytest.mark.skipif(
    is_library_installed("pymssql"), reason="requires pymssql to NOT be installed"
)
def test_cli_init_connection_string_invalid_mssql_connection_instructs_user(tmp_path_factory):
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")
    basedir = str(basedir)
    os.chdir(basedir)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["init", "--no-view"],
        input="Y\n2\n5\nmy_db\nmssql+pymssql://scott:tiger@not_a_real_host:1234/dbname\n"
    )
    stdout = result.output

    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "Which database backend are you using" in stdout
    assert "What is the url/connection string for the sqlalchemy connection" in stdout
    assert "Give your new data source a short name" in stdout
    assert "Attempting to connect to your database. This may take a moment" in stdout
    assert "Cannot connect to the database" in stdout
    assert "Database Error: No module named 'pymssql'" in stdout

    assert "Profiling" not in stdout
    assert "Building" not in stdout
    assert "Data Docs" not in stdout
    assert "Great Expectations is now set up" not in stdout

    assert result.exit_code == 1

    assert os.path.isdir(os.path.join(basedir, "great_expectations"))
    config_path = os.path.join(basedir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path, "r"))
    assert config["datasources"] == dict()

    obs_tree = gen_directory_tree_str(os.path.join(basedir, "great_expectations"))
    assert (
        obs_tree
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


def test_cli_datasorce_list(empty_data_context, filesystem_csv_2, capsys):
    """Test an empty project and a project with a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])

        obs = result.output.strip()
        assert "[]" in obs
    assert context.list_datasources() == []

    context.add_datasource(
        "wow_a_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    assert context.list_datasources() == [
        {"name": "wow_a_datasource", "class_name": "PandasDatasource"}
    ]

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])

        obs = result.output.strip()
        assert "[{'name': 'wow_a_datasource', 'class_name': 'PandasDatasource'}]" in obs


def test_cli_datasorce_new(empty_data_context, filesystem_csv_2, capsys):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["datasource", "new", "-d", project_root_dir],
            input="1\n1\n%s\nmynewsource\n" % str(filesystem_csv_2),
        )
        stdout = result.stdout

        assert "What data would you like Great Expectations to connect to?" in stdout
        assert "What are you processing your files with?" in stdout
        assert "Give your new data source a short name." in stdout
        assert "A new datasource 'mynewsource' was added to your project." in stdout

        assert result.exit_code == 0


def test_cli_datasource_profile_answering_no(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
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
            input="n\n"
        )

        stdout = result.stdout
        assert "Profiling 'my_datasource'" in stdout
        assert "Skipping profiling for now." in stdout


def test_cli_datasource_profile_with_datasource_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context
    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
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
            input="Y\n",
        )
        assert result.exit_code == 0
        stdout = result.stdout
        assert "Profiling 'my_datasource'" in stdout
        assert result.exit_code == 0

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 13


def test_cli_datasource_profile_with_no_datasource_args(
    empty_data_context, filesystem_csv_2, capsys
):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["datasource", "profile", "-d", project_root_dir, "--no-view"],
            input="Y\n",
        )
        assert result.exit_code == 0
        stdout = result.stdout
        assert "Profiling 'my_datasource'" in stdout
        assert "The following Data Docs sites were built:\n- local_site:" in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 13


def test_cli_datasource_profile_with_additional_batch_kwargs(empty_data_context, filesystem_csv_2):
    empty_data_context.add_datasource(
        "my_datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "-d",
            project_root_dir,
            "--batch_kwargs",
            '{"reader_options": {"sep": ",", "parse_dates": [0]}}',
            "--no-view",
        ],
        input="Y\n",
    )
    assert result.exit_code == 0

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 8

    evr = context.get_validation_result(
        "f1", expectation_suite_name="BasicDatasetProfiler"
    )
    reader_options = evr.meta["batch_kwargs"]["reader_options"]
    assert reader_options["parse_dates"] == [0]
    assert reader_options["sep"] == ","


def test_cli_datasource_profile_with_valid_data_asset_arg(empty_data_context, filesystem_csv_2, capsys):
    empty_data_context.add_datasource("my_datasource",
                                    module_name="great_expectations.datasource",
                                    class_name="PandasDatasource",
                                    base_directory=str(filesystem_csv_2))
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    with capsys.disabled():
        runner = CliRunner()
        result = runner.invoke(
            cli, ["datasource", "profile", "my_datasource", "--data_assets", "f1", "-d", project_root_dir, "--no-view"])

        assert result.exit_code == 0
        stdout = result.stdout
        assert "Profiling 'my_datasource'" in stdout
        assert "The following Data Docs sites were built:\n- local_site:" in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert suites[0].expectation_suite_name == "BasicDatasetProfiler"

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert validation.meta["expectation_suite_name"] == "BasicDatasetProfiler"
    assert validation.success is False
    assert len(validation.results) == 13


def test_cli_datasource_profile_with_invalid_data_asset_arg_answering_no(empty_data_context, filesystem_csv_2):
    empty_data_context.add_datasource(
        "my_datasource",
        module_name="great_expectations.datasource",
        class_name="PandasDatasource",
        base_directory=str(filesystem_csv_2),
    )
    not_so_empty_data_context = empty_data_context

    project_root_dir = not_so_empty_data_context.root_directory

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "my_datasource",
            "--data_assets",
            "bad-bad-asset",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="2\n",
    )

    stdout = result.stdout
    assert "Some of the data assets you specified were not found: bad-bad-asset" in stdout
    assert "Skipping profiling for now." in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 0


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
        result = runner.invoke(cli, ["datasource", "new", "-d", "./"])
        assert error_message in result.output
        result = runner.invoke(cli, ["datasource", "new"])
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
