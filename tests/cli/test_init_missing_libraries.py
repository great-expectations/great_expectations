import os

import pytest
from click.testing import CliRunner

from great_expectations.cli import cli
from great_expectations.cli.python_subprocess import (
    execute_shell_command_with_progress_polling,
)
from great_expectations.util import gen_directory_tree_str, is_library_loadable
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def _library_not_loaded_test(
    tmp_path_factory,
    cli_input,
    library_name,
    library_import_name,
    my_caplog,
    monkeypatch,
):
    """
    This test requires that a library is NOT installed. It tests that:
    - a helpful error message is returned to install the missing library
    - the expected tree structure is in place
    - the config yml contains an empty dict in its datasource entry
    """
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")
    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(basedir)
    result = runner.invoke(
        cli, ["--v3-api", "init", "--no-view"], input=cli_input, catch_exceptions=False
    )
    stdout = result.output
    print(stdout)
    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "Which database backend are you using" in stdout
    assert "Give your new Datasource a short name" in stdout
    assert (
        """Next, we will configure database credentials and store them in the `my_db` section
of this config file: great_expectations/uncommitted/config_variables.yml"""
        in stdout
    )
    assert (
        f"""Great Expectations relies on the library `{library_import_name}` to connect to your data, \
but the package `{library_name}` containing this library is not installed.
    Would you like Great Expectations to try to execute `pip install {library_name}` for you?"""
        in stdout
    )
    assert (
        f"""\nOK, exiting now.
    - Please execute `pip install {library_name}` before trying again."""
        in stdout
    )

    assert "Profiling" not in stdout
    assert "Building" not in stdout
    assert "Data Docs" not in stdout
    assert "Great Expectations is now set up" not in stdout

    assert result.exit_code == 1

    assert os.path.isdir(os.path.join(basedir, "great_expectations"))
    config_path = os.path.join(basedir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path))
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
        .ge_store_backend_id
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
            .ge_store_backend_id
"""
    )

    assert_no_logging_messages_or_tracebacks(my_caplog, result)


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@pytest.mark.skipif(
    is_library_loadable(library_name="sqlalchemy"),
    reason="requires sqlalchemy to NOT be installed",
)
def test_init_install_sqlalchemy(caplog, tmp_path_factory, monkeypatch):
    """WARNING: THIS TEST IS AWFUL AND WE HATE IT."""
    # This test is as much about changing the entire test environment with side effects as it is about actually testing
    # the observed behavior.
    library_import_name = "sqlalchemy"
    library_name = "sqlalchemy"

    cli_input = "\n\n2\nn\n"

    basedir = tmp_path_factory.mktemp("test_cli_init_diff")

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(basedir)
    result = runner.invoke(
        cli, ["--v3-api", "init", "--no-view"], input=cli_input, catch_exceptions=False
    )
    stdout = result.output

    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert (
        f"""Great Expectations relies on the library `{library_import_name}` to connect to your data, \
but the package `{library_name}` containing this library is not installed.
    Would you like Great Expectations to try to execute `pip install {library_name}` for you?"""
        in stdout
    )

    # NOW, IN AN EVIL KNOWN ONLY TO SLEEPLESS PROGRAMMERS, WE USE OUR UTILITY TO INSTALL SQLALCHEMY
    _ = execute_shell_command_with_progress_polling("pip install 'sqlalchemy<1.4.0'")


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@pytest.mark.skipif(
    is_library_loadable(library_name="pymysql"),
    reason="requires pymysql to NOT be installed",
)
def test_cli_init_db_mysql_without_library_installed_instructs_user(
    caplog, tmp_path_factory, monkeypatch
):
    _library_not_loaded_test(
        tmp_path_factory,
        "\n\n2\n1\nmy_db\nn\n",
        "pymysql",
        "pymysql",
        caplog,
        monkeypatch,
    )


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@pytest.mark.skipif(
    is_library_loadable(library_name="pyodbc"),
    reason="requires pyodbc to NOT be installed",
)
def test_cli_init_db_mssql_without_library_installed_instructs_user(
    caplog, tmp_path_factory, monkeypatch
):
    # TODO: Update to reflect the CLI flow sequence (once it has been implemented) and re-enable.
    _library_not_loaded_test(
        tmp_path_factory,
        "\n\n2\n6\nmy_db\nwrong_ms_sql_server_library\nn\n",
        "pyodbc",
        "pyodbc",
        caplog,
        monkeypatch,
    )


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@pytest.mark.skipif(
    is_library_loadable(library_name="psycopg2"),
    reason="requires psycopg2 to NOT be installed",
)
def test_cli_init_db_postgres_without_library_installed_instructs_user(
    caplog, tmp_path_factory, monkeypatch
):
    _library_not_loaded_test(
        tmp_path_factory,
        "\n\n2\n2\nmy_db\nn\n",
        "psycopg2-binary",
        "psycopg2",
        caplog,
        monkeypatch,
    )


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@pytest.mark.skipif(
    is_library_loadable(library_name="psycopg2"),
    reason="requires psycopg2 to NOT be installed",
)
def test_cli_init_db_redshift_without_library_installed_instructs_user(
    caplog, tmp_path_factory, monkeypatch
):
    _library_not_loaded_test(
        tmp_path_factory,
        "\n\n2\n3\nmy_db\nn\n",
        "psycopg2-binary",
        "psycopg2",
        caplog,
        monkeypatch,
    )


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@pytest.mark.skipif(
    is_library_loadable(library_name="snowflake.sqlalchemy"),
    reason="requires snowflake-sqlalchemy to NOT be installed",
)
def test_cli_init_db_snowflake_without_library_installed_instructs_user(
    caplog, tmp_path_factory, monkeypatch
):
    _library_not_loaded_test(
        tmp_path_factory,
        "\n\n2\n4\nmy_db\nn\n",
        "snowflake-sqlalchemy",
        "snowflake.sqlalchemy.snowdialect",
        caplog,
        monkeypatch,
    )


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
@pytest.mark.skipif(
    is_library_loadable(library_name="pyspark"),
    reason="requires pyspark to NOT be installed",
)
def test_cli_init_spark_without_library_installed_instructs_user(
    caplog, tmp_path_factory, monkeypatch
):
    basedir = tmp_path_factory.mktemp("test_cli_init_diff")

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(basedir)
    result = runner.invoke(
        cli,
        ["--v3-api", "init", "--no-view"],
        input="\n\n1\n2\nn\n",
        catch_exceptions=False,
    )
    stdout = result.output
    assert "Always know what to expect from your data" in stdout
    assert "What data would you like Great Expectations to connect to" in stdout
    assert "What are you processing your files with" in stdout
    assert (
        f"""Great Expectations relies on the library `pyspark` to connect to your data, \
but the package `pyspark` containing this library is not installed.
    Would you like Great Expectations to try to execute `pip install pyspark` for you?"""
        in stdout
    )
    assert (
        f"""\nOK, exiting now.
    - Please execute `pip install pyspark` before trying again."""
        in stdout
    )
    # assert "Great Expectations relies on the library `pyspark`" in stdout
    # assert "Please `pip install pyspark` before trying again" in stdout

    assert "Profiling" not in stdout
    assert "Building" not in stdout
    assert "Data Docs" not in stdout
    assert "Great Expectations is now set up" not in stdout

    assert result.exit_code == 1

    assert os.path.isdir(os.path.join(basedir, "great_expectations"))
    config_path = os.path.join(basedir, "great_expectations/great_expectations.yml")
    assert os.path.isfile(config_path)

    config = yaml.load(open(config_path))
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
        .ge_store_backend_id
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
            .ge_store_backend_id
"""
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)
