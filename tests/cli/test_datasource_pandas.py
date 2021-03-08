import os

import pytest
from click.testing import CliRunner

from great_expectations import DataContext
from great_expectations.cli import cli
from tests.cli.test_cli import yaml
from tests.cli.utils import assert_no_logging_messages_or_tracebacks


def test_cli_datasource_list_on_project_with_no_datasources(
    caplog, monkeypatch, empty_data_context, filesystem_csv_2
):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource list",
        catch_exceptions=False,
    )

    stdout = result.output.strip()
    assert "No Datasources found" in stdout
    assert context.list_datasources() == []


def test_cli_datasource_list_on_project_with_one_datasource(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
    filesystem_csv_2,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    project_root_dir = context.root_directory
    context = DataContext(project_root_dir)

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource list",
        catch_exceptions=False,
    )

    expected_output = f"""\x1b[32mUsing v3 (Batch Request) API[0m[0m
1 Datasource found:[0m
[0m
 - [36mname:[0m my_datasource[0m
   [36mclass_name:[0m Datasource[0m
""".strip()
    stdout = result.output.strip()
    assert stdout == expected_output
    assert_no_logging_messages_or_tracebacks(caplog, result)


@pytest.mark.xfail(
    reason="This command is not yet implemented for the modern API",
    run=True,
    strict=True,
)
def test_cli_datasorce_new(caplog, monkeypatch, empty_data_context, filesystem_csv_2):
    project_root_dir = empty_data_context.root_directory
    context = DataContext(project_root_dir)
    assert context.list_datasources() == []

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource new",
        input="1\n1\n%s\nmynewsource\n" % str(filesystem_csv_2),
        catch_exceptions=False,
    )
    stdout = result.stdout

    assert "What data would you like Great Expectations to connect to?" in stdout
    assert "What are you processing your files with?" in stdout
    assert "Give your new Datasource a short name." in stdout
    assert "A new datasource 'mynewsource' was added to your project." in stdout

    assert result.exit_code == 0

    config_path = os.path.join(project_root_dir, DataContext.GE_YML)
    config = yaml.load(open(config_path))
    datasources = config["datasources"]
    assert "mynewsource" in datasources.keys()
    data_source_class = datasources["mynewsource"]["data_asset_type"]["class_name"]
    assert data_source_class == "PandasDataset"
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_delete_on_project_with_one_datasource(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert "my_datasource" in [ds["name"] for ds in context.list_datasources()]
    assert len(context.list_datasources()) == 1

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource delete my_datasource",
        input="Y\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0
    assert "Using v3 (Batch Request) API" in stdout
    assert "Datasource deleted successfully." in stdout

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)
    assert len(context.list_datasources()) == 0
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_delete_on_project_with_one_datasource_declining_prompt_does_not_delete(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert "my_datasource" in [ds["name"] for ds in context.list_datasources()]
    assert len(context.list_datasources()) == 1

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource delete my_datasource",
        input="n\n",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 0
    assert "Using v3 (Batch Request) API" in stdout
    assert "Datasource `my_datasource` was not deleted." in stdout

    # reload context from disk to see if the datasource was in fact deleted
    root_directory = context.root_directory
    del context
    context = DataContext(root_directory)
    assert len(context.list_datasources()) == 1
    assert_no_logging_messages_or_tracebacks(caplog, result)


def test_cli_datasource_delete_with_non_existent_datasource_raises_error(
    caplog,
    monkeypatch,
    titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled,
):
    context = titanic_pandas_data_context_with_v013_datasource_with_checkpoints_v1_with_empty_store_stats_enabled
    assert "foo" not in [ds["name"] for ds in context.list_datasources()]

    runner = CliRunner(mix_stderr=False)
    monkeypatch.chdir(os.path.dirname(context.root_directory))
    result = runner.invoke(
        cli,
        "--v3-api datasource delete foo",
        catch_exceptions=False,
    )

    stdout = result.output
    assert result.exit_code == 1
    assert "Using v3 (Batch Request) API" in stdout
    assert "Datasource foo could not be found." in stdout
