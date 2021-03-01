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

    base_directory = context.root_directory
    # TODO do we want to print out all this information?
    expected_output = f"""\x1b[32mUsing v3 (Batch Request) API[0m[0m
1 Datasource found:[0m
[0m
 - [36mname:[0m my_datasource[0m
   [36mmodule_name:[0m great_expectations.datasource[0m
   [36mclass_name:[0m Datasource[0m
   [36mdata_connectors:[0m[0m
     [36mmy_basic_data_connector:[0m[0m
       [36mmodule_name:[0m great_expectations.datasource.data_connector[0m
       [36mclass_name:[0m InferredAssetFilesystemDataConnector[0m
       [36mbase_directory:[0m {base_directory}/../data/titanic[0m
       [36mdefault_regex:[0m[0m
         [36mgroup_names:[0m ['data_asset_name'][0m
         [36mpattern:[0m (.*)\.csv[0m
     [36mmy_other_data_connector:[0m[0m
       [36mmodule_name:[0m great_expectations.datasource.data_connector[0m
       [36mclass_name:[0m ConfiguredAssetFilesystemDataConnector[0m
       [36massets:[0m[0m
         [36musers:[0m[0m
           [36mmodule_name:[0m great_expectations.datasource.data_connector.asset[0m
           [36mclass_name:[0m Asset[0m
       [36mbase_directory:[0m {base_directory}/../data/titanic[0m
       [36mdefault_regex:[0m[0m
         [36mgroup_names:[0m ['name'][0m
         [36mpattern:[0m (.+)\.csv[0m
       [36mglob_directive:[0m *.csv[0m
     [36mmy_runtime_data_connector:[0m[0m
       [36mmodule_name:[0m great_expectations.datasource.data_connector[0m
       [36mclass_name:[0m RuntimeDataConnector[0m
       [36mruntime_keys:[0m ['pipeline_stage_name', 'airflow_run_id'][0m
     [36mmy_special_data_connector:[0m[0m
       [36mmodule_name:[0m great_expectations.datasource.data_connector[0m
       [36mclass_name:[0m ConfiguredAssetFilesystemDataConnector[0m
       [36massets:[0m[0m
         [36musers:[0m[0m
           [36mmodule_name:[0m great_expectations.datasource.data_connector.asset[0m
           [36mclass_name:[0m Asset[0m
           [36mbase_directory:[0m {base_directory}/../data/titanic[0m
           [36mgroup_names:[0m ['name', 'timestamp', 'size'][0m
           [36mpattern:[0m (.+)_(\d+)_(\d+)\.csv[0m
       [36mbase_directory:[0m {base_directory}/../data/titanic[0m
       [36mdefault_regex:[0m[0m
         [36mgroup_names:[0m ['name'][0m
         [36mpattern:[0m (.+)\.csv[0m
       [36mglob_directive:[0m *.csv[0m
   [36mexecution_engine:[0m[0m
     [36mmodule_name:[0m great_expectations.execution_engine[0m
     [36mclass_name:[0m PandasExecutionEngine[0m
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
