import os
from collections import OrderedDict

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
def test_cli_datasource_list(empty_data_context, empty_sqlite_db, caplog, monkeypatch):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "datasource",
            "list",
        ],
        catch_exceptions=False,
    )

    stdout = result.output.strip()
    assert "No Datasources found" in stdout
    assert context.list_datasources() == []

    datasource_name = "wow_a_datasource"
    _add_datasource_and_credentials_to_context(
        context, datasource_name, empty_sqlite_db
    )

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        ["--v3-api", "datasource", "list"],
        catch_exceptions=False,
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


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
def test_cli_datasorce_new_connection_string(
    empty_data_context, empty_sqlite_db, caplog, monkeypatch
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        [
            "--v3-api",
            "datasource",
            "new",
        ],
        input=f"2\n6\nmynewsource\n{empty_sqlite_db.url}\n\n",
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
