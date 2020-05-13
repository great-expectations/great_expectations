import os

import pytest
from click.testing import CliRunner
from great_expectations.cli import cli
from great_expectations.util import gen_directory_tree_str
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks
from tests.test_utils import is_library_installed


def _library_not_loaded_test(
    tmp_path_factory, cli_input, library_name, library_import_name, my_caplog
):
    """
    This test requires that a library is NOT installed. It tests that:
    - a helpful error message is returned to install the missing library
    - the expected tree structure is in place
    - the config yml contains an empty dict in its datasource entry
    """
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")
    basedir = str(basedir)
    os.chdir(basedir)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["init", "--no-view"], input=cli_input, catch_exceptions=False
    )
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
    assert (
        "Great Expectations relies on the library `{}`".format(library_import_name)
        in stdout
    )
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
    checkpoints/
    expectations/
    notebooks/
        pandas/
            validation_playground.ipynb
        spark/
            validation_playground.ipynb
        sql/
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
        validations/
"""
    )

    assert_no_logging_messages_or_tracebacks(my_caplog, result)


@pytest.mark.skipif(
    is_library_installed("pymysql"), reason="requires pymysql to NOT be installed"
)
def test_cli_init_db_mysql_without_library_installed_instructs_user(
    caplog, tmp_path_factory
):
    _library_not_loaded_test(
        tmp_path_factory, "Y\n2\n1\nmy_db\n", "pymysql", "pymysql", caplog
    )


@pytest.mark.skipif(
    is_library_installed("psycopg2"), reason="requires psycopg2 to NOT be installed"
)
def test_cli_init_db_postgres_without_library_installed_instructs_user(
    caplog, tmp_path_factory,
):
    _library_not_loaded_test(
        tmp_path_factory, "Y\n2\n2\nmy_db\n", "psycopg2", "psycopg2", caplog
    )


@pytest.mark.skipif(
    is_library_installed("psycopg2"), reason="requires psycopg2 to NOT be installed"
)
def test_cli_init_db_redshift_without_library_installed_instructs_user(
    caplog, tmp_path_factory,
):
    _library_not_loaded_test(
        tmp_path_factory, "Y\n2\n3\nmy_db\n", "psycopg2", "psycopg2", caplog
    )


@pytest.mark.skipif(
    is_library_installed("snowflake"),
    reason="requires snowflake-sqlalchemy to NOT be installed",
)
def test_cli_init_db_snowflake_without_library_installed_instructs_user(
    caplog, tmp_path_factory,
):
    _library_not_loaded_test(
        tmp_path_factory,
        "Y\n2\n4\nmy_db\n",
        "snowflake-sqlalchemy",
        "snowflake",
        caplog,
    )


@pytest.mark.skipif(
    is_library_installed("pyspark"), reason="requires pyspark to NOT be installed"
)
def test_cli_init_spark_without_library_installed_instructs_user(
    caplog, tmp_path_factory
):
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")
    basedir = str(basedir)
    os.chdir(basedir)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["init", "--no-view"], input="Y\n1\n2\n", catch_exceptions=False
    )
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
    checkpoints/
    expectations/
    notebooks/
        pandas/
            validation_playground.ipynb
        spark/
            validation_playground.ipynb
        sql/
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
        validations/
"""
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)
