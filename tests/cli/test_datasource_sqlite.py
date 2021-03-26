import os
from collections import OrderedDict
from unittest import mock

import nbformat
from click.testing import CliRunner
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_cli_datasource_list(empty_data_context, empty_sqlite_db, caplog, monkeypatch):
    """Test an empty project and after adding a single datasource."""
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource list",
        catch_exceptions=False,
    )

    stdout = result.stdout.strip()
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
    expected_output = """\
Using v3 (Batch Request) API\x1b[0m
1 Datasource found:[0m
[0m
 - [36mname:[0m wow_a_datasource[0m
   [36mclass_name:[0m SqlAlchemyDatasource[0m
""".strip()
    stdout = result.stdout.strip()

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


@mock.patch("subprocess.call", return_value=True, side_effect=None)
def test_cli_datasource_new_connection_string(
    mock_subprocess, empty_data_context, empty_sqlite_db, caplog, monkeypatch
):
    root_dir = empty_data_context.root_directory
    context = DataContext(root_dir)
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource new",
        input=f"2\n6\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")
    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)

    # mock the user adding a connection string into the notebook by overwriting the right cell
    assert "connection_string" in nb["cells"][5]["source"]
    nb["cells"][5]["source"] = 'connection_string = "sqlite://"'
    ep = ExecutePreprocessor(timeout=60, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = DataContext(root_dir)
    assert context.list_datasources() == [
        {
            "class_name": "SimpleSqlalchemyDatasource",
            "connection_string": "sqlite://",
            "introspection": {
                "whole_table": {"data_asset_name_suffix": "__whole_table"}
            },
            "module_name": "great_expectations.datasource",
            "name": "my_datasource",
        }
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)
