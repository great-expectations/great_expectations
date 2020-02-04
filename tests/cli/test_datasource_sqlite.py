import os

import pytest
from click.testing import CliRunner
from six import PY2

from great_expectations import DataContext
from great_expectations.cli import cli
from great_expectations.exceptions import DataContextError
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_cli_datasorce_list(empty_data_context, empty_sqlite_db, caplog):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner()
    result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])

    stdout = result.output.strip()
    assert "[]" in stdout
    assert context.list_datasources() == []

    datasource_name = "wow_a_datasource"
    _add_datasource_and_credentials_to_context(
        context, datasource_name, empty_sqlite_db
    )

    runner = CliRunner()
    result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])
    stdout = result.output.strip()
    assert (
        "[{'name': 'wow_a_datasource', 'class_name': 'SqlAlchemyDatasource'}]" in stdout
    )

    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(condition=PY2)
def _add_datasource_and_credentials_to_context(context, datasource_name, sqlite_engine):
    original_datasources = context.list_datasources()

    credentials = {"url": str(sqlite_engine.url)}
    context.save_config_variable(datasource_name, credentials)
    context.add_datasource(
        datasource_name,
        initialize=False,
        module_name="great_expectations.datasource",
        class_name="SqlAlchemyDatasource",
        data_asset_type={"class_name": "SqlAlchemyDataset"},
        credentials="${" + datasource_name + "}",
        generators={"default": {"class_name": "TableGenerator"}},
    )

    expected_datasources = original_datasources
    expected_datasources.append(
        {"name": datasource_name, "class_name": "SqlAlchemyDatasource"}
    )

    assert context.list_datasources() == expected_datasources
    return context


def test_cli_datasorce_new_connection_string(
    empty_data_context, empty_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["datasource", "new", "-d", project_root_dir],
        input="2\n5\nmynewsource\n{}\n".format(str(empty_sqlite_db.url)),
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "Give your new data source a short name." in stdout
    assert (
        "Next, we will configure database credentials and store them in the `mynewsource` section"
        in stdout
    )
    assert "What is the url/connection string for the sqlalchemy connection?" in stdout
    assert "Attempting to connect to your database. This may take a moment" in stdout
    assert "Great Expectations connected to your database" in stdout
    assert "A new datasource 'mynewsource' was added to your project." in stdout

    assert result.exit_code == 0

    config_path = os.path.join(project_root_dir, DataContext.GE_YML)
    config = yaml.load(open(config_path, "r"))
    datasources = config["datasources"]
    assert "mynewsource" in datasources.keys()
    data_source_class = datasources["mynewsource"]["data_asset_type"]["class_name"]
    assert data_source_class == "SqlAlchemyDataset"

    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(reason="# TODO profiling is broken due to generators")
def test_cli_datasource_profile_answering_no(
    empty_data_context, empty_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, empty_sqlite_db
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["datasource", "profile", datasource_name, "-d", project_root_dir, "--no-view"],
        input="n\n",
    )

    stdout = result.output
    print(stdout)
    assert result.exit_code == 0

    assert "Profiling 'wow_a_datasource'" in stdout
    assert "Skipping profiling for now." in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(condition=PY2)
def test_cli_datasource_profile_with_datasource_arg(
    empty_data_context, titanic_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            datasource_name,
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="Y\n",
    )
    stdout = result.stdout

    assert result.exit_code == 0
    assert "Profiling '{}'".format(datasource_name) in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert (
        suites[0].expectation_suite_name
        == "wow_a_datasource.default.main.titanic.BasicDatasetProfiler"
    )

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert (
        validation.meta["expectation_suite_name"]
        == "wow_a_datasource.default.main.titanic.BasicDatasetProfiler"
    )
    assert validation.success is False
    assert len(validation.results) == 51

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_profile_with_no_datasource_args(
    empty_data_context, titanic_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["datasource", "profile", "-d", project_root_dir, "--no-view"],
        input="Y\n",
    )
    assert result.exit_code == 0
    stdout = result.stdout
    assert "Profiling 'wow_a_datasource'" in stdout
    assert "The following Data Docs sites were built:\n- local_site:" in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 1
    assert (
        suites[0].expectation_suite_name
        == "wow_a_datasource.default.main.titanic.BasicDatasetProfiler"
    )

    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 1

    validation = validations_store.get(validation_keys[0])
    assert (
        validation.meta["expectation_suite_name"]
        == "wow_a_datasource.default.main.titanic.BasicDatasetProfiler"
    )
    assert validation.success is False
    assert len(validation.results) == 51

    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(reason="TODO broken")
def test_cli_datasource_profile_with_data_asset_and_additional_batch_kwargs_should_raise_helpful_error(
    empty_data_context, titanic_sqlite_db, caplog
):
    assert False
    """
    Passing additional batch kwargs along with a data asset name to a sql
    backend is an invalid operation and should display a helpful error message.
    """
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "-d",
            project_root_dir,
            "--data_assets",
            "main.titanic",
            "--batch_kwargs",
            '{"query": "select * from main.titanic"}',
            "--no-view",
        ],
        input="Y\n",
    )
    stdout = result.output
    print(stdout)
    print(result.exception)
    assert result.exit_code == 1

    # There should not be a suite created
    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 0

    # There should not be a validation created
    validations_store = context.stores["validations_store"]
    validation_keys = validations_store.list_keys()
    assert len(validation_keys) == 0

    # TODO is DataContextError the best error to raise? Should it be a CLI error?
    assert isinstance(result.exception, DataContextError)

    # TODO this may not be appropriate for this test...
    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail
def test_cli_datasource_profile_with_valid_data_asset_arg(
    empty_data_context, titanic_sqlite_db, caplog
):
    assert False
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            datasource_name,
            "--data_assets",
            "main.titanic",
            "-d",
            project_root_dir,
            "--no-view",
        ],
    )

    stdout = result.stdout
    assert result.exit_code == 0
    assert "Profiling '{}'".format(datasource_name) in stdout
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
    assert len(validation.results) == 51

    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail
def test_cli_datasource_profile_with_invalid_data_asset_arg_answering_no(
    empty_data_context, titanic_sqlite_db, caplog
):
    assert False
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            datasource_name,
            "--data_assets",
            "bad-bad-asset",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="2\n",
    )

    stdout = result.stdout
    assert (
        "Some of the data assets you specified were not found: bad-bad-asset" in stdout
    )
    assert "Choose how to proceed" in stdout
    assert "Skipping profiling for now." in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 0

    assert_no_logging_messages_or_tracebacks(caplog, result)
