import os
import re
import shutil

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.data_context.util import file_relative_path
from great_expectations.util import gen_directory_tree_str
from tests.cli.test_cli import yaml


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


def test_cli_init_connection_string_non_working_mssql_connection_instructs_user_and_leaves_entries_in_config_files_for_debugging(
    tmp_path_factory,
):
    basedir = tmp_path_factory.mktemp("mssql_test")
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

    ge_dir = os.path.join(basedir, DataContext.GE_DIR)
    assert os.path.isdir(ge_dir)
    config_path = os.path.join(ge_dir, DataContext.GE_YML)
    assert os.path.isfile(config_path)

    # TODO this entry might not be totally right, but one needs to be here.
    config = yaml.load(open(config_path, "r"))
    assert config["datasources"] == {
        "my_db": {
            "data_asset_type": {
                "module_name": None,
                "class_name": "SqlAlchemyDataset",
            },
            "credentials": "${my_db}",
            "class_name": "SqlAlchemyDatasource",
            "generators": {
                "default": {"class_name": "TableGenerator"},
                "passthrough": {"class_name": "PassthroughGenerator"},
            },
        }
    }

    # TODO add entry in config_vars this entry might not be totally right, but one needs to be here.
    config_path = os.path.join(
        ge_dir, DataContext.GE_UNCOMMITTED_DIR, "config_variables.yml"
    )
    config = yaml.load(open(config_path, "r"))
    assert config["my_db"] == {
        "connection_string": "mssql+pymssql://scott:tiger@not_a_real_host:1234/dbname"
    }

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
