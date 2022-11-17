import os
from collections import OrderedDict
from unittest import mock

import nbformat
import pytest
from click.testing import CliRunner
from nbconvert.preprocessors import ExecutePreprocessor

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.utils import assert_no_logging_messages_or_tracebacks, escape_ansi


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
def test_cli_datasource_list(
    mock_emit, empty_data_context_stats_enabled, empty_sqlite_db, caplog, monkeypatch
):
    """Test an empty project and after adding a single datasource."""
    context: DataContext = empty_data_context_stats_enabled

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
Using v3 (Batch Request) API
1 Datasource found:

 - name: wow_a_datasource
   class_name: SqlAlchemyDatasource
""".strip()
    stdout = escape_ansi(result.stdout).strip()

    assert stdout == expected_output

    assert_no_logging_messages_or_tracebacks(caplog, result)
    anonymized_name: str = mock_emit.call_args_list[3][0][0]["event_payload"][
        "anonymized_name"
    ]

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.list.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "parent_class": "SqlAlchemyDatasource",
                },
                "event": "data_context.add_datasource",
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "datasource.sqlalchemy.connect",
                "event_payload": {
                    "anonymized_name": anonymized_name,
                    "sqlalchemy_dialect": "sqlite",
                },
                "success": True,
            }
        ),
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.list.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.list.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_count == len(expected_call_args_list)
    assert mock_emit.call_args_list == expected_call_args_list


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


@mock.patch(
    "great_expectations.core.usage_statistics.usage_statistics.UsageStatisticsHandler.emit"
)
@mock.patch("subprocess.call", return_value=True, side_effect=None)
@pytest.mark.slow  # 6.81s
def test_cli_datasource_new_connection_string(
    mock_subprocess,
    mock_emit,
    empty_data_context_stats_enabled,
    empty_sqlite_db,
    caplog,
    monkeypatch,
):
    root_dir = empty_data_context_stats_enabled.root_directory
    context: DataContext = empty_data_context_stats_enabled
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource new",
        input="2\n7\n",
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout

    assert result.exit_code == 0

    uncommitted_dir = os.path.join(root_dir, context.GE_UNCOMMITTED_DIR)
    expected_notebook = os.path.join(uncommitted_dir, "datasource_new.ipynb")

    assert os.path.isfile(expected_notebook)
    mock_subprocess.assert_called_once_with(["jupyter", "notebook", expected_notebook])

    expected_call_args_list = [
        mock.call(
            {"event_payload": {}, "event": "data_context.__init__", "success": True}
        ),
        mock.call(
            {
                "event": "cli.datasource.new.begin",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.new_ds_choice",
                "event_payload": {
                    "type": "sqlalchemy",
                    "db": "other",
                    "api_version": "v3",
                },
                "success": True,
            }
        ),
        mock.call(
            {
                "event": "cli.datasource.new.end",
                "event_payload": {"api_version": "v3"},
                "success": True,
            }
        ),
    ]

    assert mock_emit.call_args_list == expected_call_args_list
    assert mock_emit.call_count == len(expected_call_args_list)

    # Run notebook
    with open(expected_notebook) as f:
        nb = nbformat.read(f, as_version=4)

    # Mock the user adding a connection string into the notebook by overwriting the right cell
    credentials_cell = nb["cells"][5]["source"]

    credentials = ("connection_string", "schema_name", "table_name")
    for credential in credentials:
        assert credential in credentials_cell

    # Replace placeholder with actual value to allow remainder of notebook to execute successfully
    nb["cells"][5]["source"] = credentials_cell.replace(
        "YOUR_CONNECTION_STRING", "sqlite://"
    )

    ep = ExecutePreprocessor(timeout=60, kernel_name="python3")
    ep.preprocess(nb, {"metadata": {"path": uncommitted_dir}})

    del context
    context = DataContext(root_dir)

    assert context.list_datasources() == [
        {
            "module_name": "great_expectations.datasource",
            "execution_engine": {
                "module_name": "great_expectations.execution_engine",
                "connection_string": "sqlite://",
                "class_name": "SqlAlchemyExecutionEngine",
            },
            "class_name": "Datasource",
            "data_connectors": {
                "default_runtime_data_connector_name": {
                    "batch_identifiers": ["default_identifier_name"],
                    "class_name": "RuntimeDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                },
                "default_inferred_data_connector_name": {
                    "class_name": "InferredAssetSqlDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                    "include_schema_name": True,
                    "introspection_directives": {
                        "schema_name": "YOUR_SCHEMA",
                    },
                },
                "default_configured_data_connector_name": {
                    "assets": {
                        "YOUR_TABLE_NAME": {
                            "class_name": "Asset",
                            "module_name": "great_expectations.datasource.data_connector.asset",
                            "schema_name": "YOUR_SCHEMA",
                        },
                    },
                    "class_name": "ConfiguredAssetSqlDataConnector",
                    "module_name": "great_expectations.datasource.data_connector",
                },
            },
            "name": "my_datasource",
        }
    ]

    assert_no_logging_messages_or_tracebacks(caplog, result)
