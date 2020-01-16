import os

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.test_cli import yaml


@pytest.fixture()
def sql_engine(sqlitedb_engine):
    """Silly shim to allow pluggable sql engines"""
    return sqlitedb_engine


def test_cli_datasorce_list(empty_data_context, sql_engine):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner()
    result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])

    stdout = result.output.strip()
    assert "[]" in stdout
    assert context.list_datasources() == []

    datasource_name = "wow_a_datasource"
    _add_datasource_and_credentials_to_context(context, datasource_name, sql_engine)

    runner = CliRunner()
    result = runner.invoke(cli, ["datasource", "list", "-d", project_root_dir])
    stdout = result.output.strip()
    assert (
        "[{'name': 'wow_a_datasource', 'class_name': 'SqlAlchemyDatasource'}]" in stdout
    )


def _add_datasource_and_credentials_to_context(context, datasource_name, sql_engine):
    credentials = {"url": str(sql_engine.url)}
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
    assert context.list_datasources() == [
        {"name": datasource_name, "class_name": "SqlAlchemyDatasource"}
    ]
    return context


def test_cli_datasorce_new_connection_string(empty_data_context, sql_engine):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["datasource", "new", "-d", project_root_dir],
        input="2\n5\nmynewsource\n{}\n".format(str(sql_engine.url)),
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


def test_cli_datasource_profile_answering_no(empty_data_context, sql_engine):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, sql_engine
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["datasource", "profile", datasource_name, "-d", project_root_dir, "--no-view"],
        input="n\n",
    )

    stdout = result.stdout
    assert result.exit_code == 0

    assert "Profiling 'wow_a_datasource'" in stdout
    assert "Skipping profiling for now." in stdout


def test_cli_datasource_profile_with_datasource_arg(
    empty_data_context, titanic_sqlite_db
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
    print(stdout)

    assert result.exit_code == 0
    assert "Traceback" not in stdout
    assert "Profiling '{}'".format(datasource_name) in stdout

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


@pytest.mark.skip(reason="not yet converted to sqlalchemy")
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


@pytest.mark.skip(reason="not yet converted to sqlalchemy")
def test_cli_datasource_profile_with_additional_batch_kwargs(
    empty_data_context, filesystem_csv_2
):
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


@pytest.mark.skip(reason="not yet converted to sqlalchemy")
def test_cli_datasource_profile_with_valid_data_asset_arg(
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
            [
                "datasource",
                "profile",
                "my_datasource",
                "--data_assets",
                "f1",
                "-d",
                project_root_dir,
                "--no-view",
            ],
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


@pytest.mark.skip(reason="not yet converted to sqlalchemy")
def test_cli_datasource_profile_with_invalid_data_asset_arg_answering_no(
    empty_data_context, filesystem_csv_2
):
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
    assert (
        "Some of the data assets you specified were not found: bad-bad-asset" in stdout
    )
    assert "Skipping profiling for now." in stdout

    context = DataContext(project_root_dir)
    assert len(context.list_datasources()) == 1

    expectations_store = context.stores["expectations_store"]
    suites = expectations_store.list_keys()
    assert len(suites) == 0
