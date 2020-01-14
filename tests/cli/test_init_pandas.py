import os
import re
import shutil

import pytest
from click.testing import CliRunner

from great_expectations.cli import cli
from great_expectations.data_context.templates import CONFIG_VARIABLES_TEMPLATE
from great_expectations.data_context.util import file_relative_path
from great_expectations.exceptions import ConfigNotFoundError
from great_expectations.util import gen_directory_tree_str
from tests.cli.test_cli import yaml
from tests.test_utils import is_library_installed


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
            cli, ["init", "--no-view"], input="Y\n1\n1\n{}\n\n\n\n".format(data_path),
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
        data_source_class = config["datasources"]["files_datasource"][
            "data_asset_type"
        ]["class_name"]
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


@pytest.mark.skipif(
    is_library_installed("pymssql"), reason="requires pymssql to NOT be installed"
)
def test_cli_init_connection_string_invalid_mssql_connection_instructs_user(
    tmp_path_factory,
):
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")
    basedir = str(basedir)
    os.chdir(basedir)

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["init", "--no-view"],
        input="Y\n2\n5\nmy_db\nmssql+pymssql://scott:tiger@not_a_real_host:1234/dbname\n",
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
