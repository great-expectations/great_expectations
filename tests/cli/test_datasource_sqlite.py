import os
from collections import OrderedDict

from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.test_cli import yaml
from tests.cli.utils import (
    assert_dict_key_and_val_in_stdout,
    assert_no_logging_messages_or_tracebacks,
    assert_no_tracebacks,
)


def test_cli_datasource_list(empty_data_context, empty_sqlite_db, caplog):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["datasource", "list", "-d", project_root_dir], catch_exceptions=False
    )

    stdout = result.output.strip()
    assert "No Datasources found" in stdout
    assert context.list_datasources() == []

    datasource_name = "wow_a_datasource"
    _add_datasource_and_credentials_to_context(
        context, datasource_name, empty_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli, ["datasource", "list", "-d", project_root_dir], catch_exceptions=False
    )
    url = str(empty_sqlite_db.engine.url)
    expected_output = """\
1 Datasource found:[0m
[0m
 - [36mname:[0m wow_a_datasource[0m
   [36mmodule_name:[0m great_expectations.datasource[0m
   [36mclass_name:[0m SqlAlchemyDatasource[0m
   [36mbatch_kwargs_generators:[0m[0m
     [36mdefault:[0m[0m
       [36mclass_name:[0m TableBatchKwargsGenerator[0m
   [36mcredentials:[0m[0m
     [36murl:[0m {}[0m
   [36mdata_asset_type:[0m[0m
     [36mclass_name:[0m SqlAlchemyDataset[0m
     [36mmodule_name:[0m None[0m
""".format(
        url
    ).strip()
    stdout = result.output.strip()

    assert stdout == expected_output

    assert_no_logging_messages_or_tracebacks(caplog, result)


def _add_datasource_and_credentials_to_context(context, datasource_name, sqlite_engine):
    original_datasources = context.list_datasources()

    url = str(sqlite_engine.url)
    credentials = {"url": url}
    context.save_config_variable(datasource_name, credentials)
    context.add_datasource(
        datasource_name,
        initialize=False,
        module_name="great_expectations.datasource",
        class_name="SqlAlchemyDatasource",
        data_asset_type={"class_name": "SqlAlchemyDataset"},
        credentials="${" + datasource_name + "}",
        batch_kwargs_generators={
            "default": {"class_name": "TableBatchKwargsGenerator"}
        },
    )

    expected_datasources = original_datasources
    expected_datasources.append(
        {
            "name": datasource_name,
            "class_name": "SqlAlchemyDatasource",
            "module_name": "great_expectations.datasource",
            "credentials": OrderedDict([("url", url)]),
            "data_asset_type": {"class_name": "SqlAlchemyDataset", "module_name": None},
            "batch_kwargs_generators": {
                "default": {"class_name": "TableBatchKwargsGenerator"}
            },
        }
    )

    assert context.list_datasources() == expected_datasources
    return context


def _add_datasource__with_two_generators_and_credentials_to_context(
    context, datasource_name, sqlite_engine
):
    original_datasources = context.list_datasources()

    url = str(sqlite_engine.url)
    credentials = {"url": url}
    context.save_config_variable(datasource_name, credentials)
    context.add_datasource(
        datasource_name,
        initialize=False,
        module_name="great_expectations.datasource",
        class_name="SqlAlchemyDatasource",
        data_asset_type={"class_name": "SqlAlchemyDataset"},
        credentials="${" + datasource_name + "}",
        batch_kwargs_generators={
            "default": {"class_name": "TableBatchKwargsGenerator"},
            "second_generator": {
                "class_name": "ManualBatchKwargsGenerator",
                "assets": {
                    "asset_one": [
                        {"partition_id": 1, "query": "select * from main.titanic"}
                    ]
                },
            },
        },
    )

    expected_datasources = original_datasources
    expected_datasources.append(
        {
            "name": datasource_name,
            "class_name": "SqlAlchemyDatasource",
            "module_name": "great_expectations.datasource",
            "credentials": {"url": url},
            "data_asset_type": {"class_name": "SqlAlchemyDataset", "module_name": None},
            "batch_kwargs_generators": {
                "default": {"class_name": "TableBatchKwargsGenerator"},
                "second_generator": {
                    "assets": {
                        "asset_one": [
                            {
                                "partition_id": 1,
                                "query": "select " "* " "from " "main.titanic",
                            }
                        ]
                    },
                    "class_name": "ManualBatchKwargsGenerator",
                },
            },
        }
    )

    assert context.list_datasources() == expected_datasources
    return context


def test_cli_datasorce_new_connection_string(
    empty_data_context, empty_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "new", "-d", project_root_dir],
        input="2\n6\nmynewsource\n{}\n\n".format(str(empty_sqlite_db.url)),
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "Give your new Datasource a short name." in stdout
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
    config = yaml.load(open(config_path))
    datasources = config["datasources"]
    assert "mynewsource" in datasources.keys()
    data_source_class = datasources["mynewsource"]["data_asset_type"]["class_name"]
    assert data_source_class == "SqlAlchemyDataset"

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_profile_answering_no(
    empty_data_context, titanic_sqlite_db, caplog
):
    """
    When datasource profile command is called without additional arguments,
    the command must prompt the user with a confirm (y/n) before profiling.
    We are verifying  that it does that and respects user's "no".
    """
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "profile", datasource_name, "-d", project_root_dir, "--no-view"],
        input="n\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0
    assert "Profiling 'wow_a_datasource'" in stdout
    assert "Skipping profiling for now." in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_profile_on_empty_database(
    empty_data_context, empty_sqlite_db, caplog
):
    """
    We run the datasource profile command against an empty database (no tables).
    This means that no generator can "see" a list of available data assets.
    The command must exit with an error message saying that no generator can see
    any assets.
    """
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, empty_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "profile", datasource_name, "-d", project_root_dir, "--no-view"],
        input="n\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 1

    assert "Profiling 'wow_a_datasource'" in stdout
    assert "No batch kwargs generators can list available data assets" in stdout

    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_profile_with_datasource_arg(
    empty_data_context, titanic_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
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
        catch_exceptions=False,
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

    assert "Preparing column 1 of 7" in caplog.messages[0]
    assert len(caplog.messages) == 10
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_datasource_arg_and_generator_name_arg(
    empty_data_context, titanic_sqlite_db, caplog
):
    """
    Here we are verifying that when generator_name argument is passed to
    the methods down the stack.

    We use a datasource with two generators. This way we can check that the
    name of the expectation suite created by the profiler corresponds to
    the name of the data asset listed by the generator that we told the profiler
    to use.

    The logic of processing this argument is testing in tests/profile.
    """
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource__with_two_generators_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    second_generator_name = "second_generator"

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            datasource_name,
            "--batch-kwargs-generator-name",
            second_generator_name,
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
        == "wow_a_datasource.second_generator.asset_one.BasicDatasetProfiler"
    )

    assert "Preparing column 1 of 7" in caplog.messages[0]
    assert len(caplog.messages) == 10
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_no_datasource_args(
    empty_data_context, titanic_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        ["datasource", "profile", "-d", project_root_dir, "--no-view"],
        input="Y\n",
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    stdout = result.stdout
    assert "Profiling 'wow_a_datasource'" in stdout
    assert "The following Data Docs sites will be built:\n" in stdout
    assert "local_site:" in stdout

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

    assert "Preparing column 1 of 7" in caplog.messages[0]
    assert len(caplog.messages) == 10
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_data_asset_and_additional_batch_kwargs_with_limit(
    empty_data_context, titanic_sqlite_db, caplog
):
    """
    User can pass additional batch kwargs (e.g., limit) to a sql backend.
    Here we are verifying that passing "limit" affects the query correctly -
    the row count in the batch that the profiler uses to profile the data asset
    must match the limit passed by the user.
    """
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            "-d",
            project_root_dir,
            "--data-assets",
            "main.titanic",
            "--additional-batch-kwargs",
            '{"limit": 97}',
            "--no-view",
        ],
        input="Y\n",
        catch_exceptions=False,
    )

    stdout = result.stdout
    assert result.exit_code == 0
    assert "Profiling '{}'".format(datasource_name) in stdout
    assert "The following Data Docs sites will be built:\n" in stdout
    assert "local_site:" in stdout

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

    row_count_validation_results = [
        validation_result
        for validation_result in validation.results
        if validation_result.expectation_config.expectation_type
        == "expect_table_row_count_to_be_between"
    ]
    assert len(row_count_validation_results) == 1
    assert row_count_validation_results[0].result["observed_value"] == 97

    assert "Preparing column 1 of 7" in caplog.messages[0]
    assert len(caplog.messages) == 10
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_valid_data_asset_arg(
    empty_data_context, titanic_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            datasource_name,
            "--data-assets",
            "main.titanic",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        catch_exceptions=False,
    )

    stdout = result.stdout
    assert result.exit_code == 0
    assert "Profiling '{}'".format(datasource_name) in stdout
    assert "The following Data Docs sites will be built:\n" in stdout
    assert "local_site:" in stdout

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

    assert "Preparing column 1 of 7" in caplog.messages[0]
    assert len(caplog.messages) == 10
    assert_no_tracebacks(result)


def test_cli_datasource_profile_with_invalid_data_asset_arg_answering_no(
    empty_data_context, titanic_sqlite_db, caplog
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    datasource_name = "wow_a_datasource"
    context = _add_datasource_and_credentials_to_context(
        context, datasource_name, titanic_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    result = runner.invoke(
        cli,
        [
            "datasource",
            "profile",
            datasource_name,
            "--data-assets",
            "bad-bad-asset",
            "-d",
            project_root_dir,
            "--no-view",
        ],
        input="2\n",
        catch_exceptions=False,
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
